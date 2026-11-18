/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.job;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Metalake.PROPERTY_IN_USE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
import org.apache.gravitino.listener.api.event.EnableMetalakeEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.info.MetalakeInfo;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestBuiltInJobTemplateEventListener {

  private BuiltInJobTemplateEventListener listener;
  private JobManager jobManager;
  private EntityStore entityStore;
  private IdGenerator idGenerator;
  private Config config;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    config = Mockito.mock(Config.class);

    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    idGenerator = new RandomIdGenerator();

    jobManager = mock(JobManager.class);
    // Register on the test thread, since the ServiceLoader static mocks are thread local.
    listener =
        new BuiltInJobTemplateEventListener(
            jobManager, entityStore, idGenerator, MoreExecutors.newDirectExecutorService());
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
    }
  }

  @Test
  public void testOnPostCreateMetalakeEvent() throws IOException {
    String metalakeName = "test_metalake";
    createMetalake(metalakeName, true);
    NameIdentifier identifier = NameIdentifier.of(metalakeName);
    MetalakeInfo metalakeInfo = new MetalakeInfo(metalakeName, "comment", null, null);

    CreateMetalakeEvent event = new CreateMetalakeEvent("user", identifier, metalakeInfo);

    // Mock loadBuiltInJobTemplates to return one template
    JobTemplateProvider provider = mock(JobTemplateProvider.class);
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-test")
            .withComment("test template")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();

    when(provider.jobTemplates()).thenAnswer(invocation -> Collections.singletonList(template));

    try (MockedStatic<ServiceLoader> mockedLoader = mockStatic(ServiceLoader.class)) {
      ServiceLoader<JobTemplateProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(eq(JobTemplateProvider.class), any()))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Lists.newArrayList(provider).iterator());
      when(serviceLoader.spliterator()).thenReturn(Lists.newArrayList(provider).spliterator());
      Mockito.doAnswer(
              invocation -> {
                Consumer<JobTemplateProvider> consumer = invocation.getArgument(0);
                consumer.accept(provider);
                return null;
              })
          .when(serviceLoader)
          .forEach(any());

      // Mock jobManager methods
      when(jobManager.listJobTemplates(metalakeName)).thenReturn(Collections.emptyList());
      doNothing().when(jobManager).registerJobTemplate(eq(metalakeName), any());

      listener.onPostEvent(event);

      // Verify registerJobTemplate was called, with the metalake creator as the template creator
      ArgumentCaptor<JobTemplateEntity> captor = ArgumentCaptor.forClass(JobTemplateEntity.class);
      verify(jobManager, times(1)).registerJobTemplate(eq(metalakeName), captor.capture());
      Assertions.assertEquals("user", captor.getValue().auditInfo().creator());
    }
  }

  @Test
  public void testCreatedMetalakesAreNotDroppedWhileRegistrationIsBusy() throws Exception {
    for (String metalake : Arrays.asList("metalake1", "metalake2", "metalake3")) {
      createMetalake(metalake, true);
    }
    CountDownLatch firstLoadStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstLoad = new CountDownLatch(1);
    BuiltInJobTemplateEventListener busyListener =
        newBlockingListener(firstLoadStarted, releaseFirstLoad);
    when(jobManager.listJobTemplates(any())).thenReturn(Collections.emptyList());

    // Block the first registration, then create more metalakes while it is busy.
    busyListener.onPostEvent(createMetalakeEvent("metalake1"));
    Assertions.assertTrue(firstLoadStarted.await(30, TimeUnit.SECONDS));
    busyListener.onPostEvent(createMetalakeEvent("metalake2"));
    busyListener.onPostEvent(createMetalakeEvent("metalake3"));
    releaseFirstLoad.countDown();

    // Stop finishes the pending registrations.
    busyListener.stop();

    for (String metalake : Arrays.asList("metalake1", "metalake2", "metalake3")) {
      verify(jobManager, times(1)).registerJobTemplate(eq(metalake), any());
    }
  }

  @Test
  public void testRegistrationFollowsMetalakeRenamedWhileQueued() throws Exception {
    long metalakeId = createMetalake("metalake_a", true).id();
    CountDownLatch loadStarted = new CountDownLatch(1);
    CountDownLatch releaseLoad = new CountDownLatch(1);
    BuiltInJobTemplateEventListener busyListener = newBlockingListener(loadStarted, releaseLoad);
    when(jobManager.listJobTemplates(any())).thenReturn(Collections.emptyList());

    busyListener.onPostEvent(createMetalakeEvent("metalake_a"));
    Assertions.assertTrue(loadStarted.await(30, TimeUnit.SECONDS));
    // Rename metalake_a to metalake_b before the registration resumes.
    entityStore.delete(NameIdentifier.of("metalake_a"), Entity.EntityType.METALAKE);
    putMetalake("metalake_b", metalakeId, true);
    releaseLoad.countDown();
    busyListener.stop();

    verify(jobManager, times(1)).registerJobTemplate(eq("metalake_b"), any());
    verify(jobManager, times(0)).registerJobTemplate(eq("metalake_a"), any());
  }

  @Test
  public void testRegistrationSkipsMetalakeDroppedAndRecreatedWhileQueued() throws Exception {
    createMetalake("metalake_a", true);
    CountDownLatch loadStarted = new CountDownLatch(1);
    CountDownLatch releaseLoad = new CountDownLatch(1);
    BuiltInJobTemplateEventListener busyListener = newBlockingListener(loadStarted, releaseLoad);

    busyListener.onPostEvent(createMetalakeEvent("metalake_a"));
    Assertions.assertTrue(loadStarted.await(30, TimeUnit.SECONDS));
    // Drop metalake_a and create another metalake with the same name before the registration
    // resumes. The old event must not populate the new metalake.
    entityStore.delete(NameIdentifier.of("metalake_a"), Entity.EntityType.METALAKE);
    createMetalake("metalake_a", true);
    releaseLoad.countDown();
    busyListener.stop();

    verify(jobManager, times(0)).listJobTemplates(any());
    verify(jobManager, times(0)).registerJobTemplate(any(), any());
  }

  @Test
  public void testRegistrationSkipsDisabledMetalakeAndRetriesOnEnable() throws Exception {
    long metalakeId = createMetalake("metalake_a", true).id();
    CountDownLatch loadStarted = new CountDownLatch(1);
    CountDownLatch releaseLoad = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    BuiltInJobTemplateEventListener busyListener =
        newBlockingListener(loadStarted, releaseLoad, executor);
    when(jobManager.listJobTemplates(any())).thenReturn(Collections.emptyList());

    busyListener.onPostEvent(createMetalakeEvent("metalake_a"));
    Assertions.assertTrue(loadStarted.await(30, TimeUnit.SECONDS));
    // Disable metalake_a before the registration resumes, then wait for the registration to end.
    replaceMetalake("metalake_a", metalakeId, false);
    releaseLoad.countDown();
    executor.submit(() -> {}).get(30, TimeUnit.SECONDS);
    verify(jobManager, times(0)).registerJobTemplate(any(), any());

    // Enabling metalake_a registers its templates.
    replaceMetalake("metalake_a", metalakeId, true);
    busyListener.onPostEvent(
        new EnableMetalakeEvent("user", NameIdentifierUtil.ofMetalake("metalake_a")));
    busyListener.stop();

    verify(jobManager, times(1)).registerJobTemplate(eq("metalake_a"), any());
  }

  @Test
  public void testOnPostEventAfterStop() {
    listener.stop();

    // A metalake created after stop is skipped; start() registers its templates on next startup.
    Assertions.assertDoesNotThrow(() -> listener.onPostEvent(createMetalakeEvent("metalake1")));
    verify(jobManager, times(0)).listJobTemplates(any());
  }

  @Test
  public void testOnPostEventWithNonMetalakeEvent() {
    Event otherEvent = mock(Event.class);

    // Should not throw exception and should not interact with jobManager
    listener.onPostEvent(otherEvent);

    verify(jobManager, times(0)).listJobTemplates(any());
    verify(jobManager, times(0)).registerJobTemplate(any(), any());
  }

  @Test
  public void testStartWithExistingMetalakes() throws IOException {
    // Create real metalakes in entity store
    createMetalake("metalake1", true);
    createMetalake("metalake2", true);
    createMetalake("metalake3", false); // not in use

    // Mock loadBuiltInJobTemplates
    JobTemplateProvider provider = mock(JobTemplateProvider.class);
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-test")
            .withComment("test")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    when(provider.jobTemplates()).thenAnswer(invocation -> Collections.singletonList(template));

    try (MockedStatic<ServiceLoader> mockedLoader = mockStatic(ServiceLoader.class)) {
      ServiceLoader<JobTemplateProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(eq(JobTemplateProvider.class), any()))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Lists.newArrayList(provider).iterator());
      when(serviceLoader.spliterator()).thenReturn(Lists.newArrayList(provider).spliterator());
      Mockito.doAnswer(
              invocation -> {
                Consumer<JobTemplateProvider> consumer = invocation.getArgument(0);
                consumer.accept(provider);
                return null;
              })
          .when(serviceLoader)
          .forEach(any());

      when(jobManager.listJobTemplates(any())).thenReturn(Collections.emptyList());
      doNothing().when(jobManager).registerJobTemplate(any(), any());

      listener.start();

      // Should only register for metalake1 and metalake2 (in-use metalakes)
      verify(jobManager, times(1)).listJobTemplates("metalake1");
      verify(jobManager, times(1)).listJobTemplates("metalake2");
      verify(jobManager, times(0)).listJobTemplates("metalake3");
      verify(jobManager, times(2)).registerJobTemplate(any(), any());
    }
  }

  @Test
  public void testStartWithNoExistingMetalakes() throws IOException {
    // No metalakes created, entityStore is empty
    listener.start();

    // Should not interact with jobManager
    verify(jobManager, times(0)).listJobTemplates(any());
    verify(jobManager, times(0)).registerJobTemplate(any(), any());
  }

  @Test
  public void testLoadBuiltInJobTemplatesWithValidTemplates() {
    JobTemplateProvider provider = mock(JobTemplateProvider.class);

    ShellJobTemplate template1 =
        ShellJobTemplate.builder()
            .withName("builtin-sparkpi")
            .withComment("v1")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();

    ShellJobTemplate template2 =
        ShellJobTemplate.builder()
            .withName("builtin-sparkpi")
            .withComment("v2")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v2"))
            .build();

    when(provider.jobTemplates()).thenAnswer(invocation -> Arrays.asList(template1, template2));

    try (MockedStatic<ServiceLoader> mockedLoader = mockStatic(ServiceLoader.class)) {
      ServiceLoader<JobTemplateProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(eq(JobTemplateProvider.class), any()))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Lists.newArrayList(provider).iterator());
      when(serviceLoader.spliterator()).thenReturn(Lists.newArrayList(provider).spliterator());
      Mockito.doAnswer(
              invocation -> {
                Consumer<JobTemplateProvider> consumer = invocation.getArgument(0);
                consumer.accept(provider);
                return null;
              })
          .when(serviceLoader)
          .forEach(any());

      Map<String, JobTemplate> templates = listener.loadBuiltInJobTemplates();

      // Should keep the higher version (v2)
      Assertions.assertEquals(1, templates.size());
      Assertions.assertTrue(templates.containsKey("builtin-sparkpi"));
      Assertions.assertEquals("v2", templates.get("builtin-sparkpi").customFields().get("version"));
    }
  }

  @Test
  public void testLoadBuiltInJobTemplatesWithInvalidNames() {
    JobTemplateProvider provider = mock(JobTemplateProvider.class);

    ShellJobTemplate invalidName =
        ShellJobTemplate.builder()
            .withName("sparkpi") // Missing builtin- prefix
            .withComment("invalid")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();

    ShellJobTemplate validName =
        ShellJobTemplate.builder()
            .withName("builtin-valid")
            .withComment("valid")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();

    when(provider.jobTemplates()).thenAnswer(invocation -> Arrays.asList(invalidName, validName));

    try (MockedStatic<ServiceLoader> mockedLoader = mockStatic(ServiceLoader.class)) {
      ServiceLoader<JobTemplateProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(eq(JobTemplateProvider.class), any()))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Lists.newArrayList(provider).iterator());
      when(serviceLoader.spliterator()).thenReturn(Lists.newArrayList(provider).spliterator());
      Mockito.doAnswer(
              invocation -> {
                Consumer<JobTemplateProvider> consumer = invocation.getArgument(0);
                consumer.accept(provider);
                return null;
              })
          .when(serviceLoader)
          .forEach(any());

      Map<String, JobTemplate> templates = listener.loadBuiltInJobTemplates();

      // Should only include valid template
      Assertions.assertEquals(1, templates.size());
      Assertions.assertTrue(templates.containsKey("builtin-valid"));
      Assertions.assertFalse(templates.containsKey("sparkpi"));
    }
  }

  @Test
  public void testLoadBuiltInJobTemplatesWithInvalidVersions() {
    JobTemplateProvider provider = mock(JobTemplateProvider.class);

    ShellJobTemplate invalidVersion =
        ShellJobTemplate.builder()
            .withName("builtin-invalid")
            .withComment("invalid")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "version1")) // Invalid format
            .build();

    ShellJobTemplate validVersion =
        ShellJobTemplate.builder()
            .withName("builtin-valid")
            .withComment("valid")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();

    when(provider.jobTemplates())
        .thenAnswer(invocation -> Arrays.asList(invalidVersion, validVersion));

    try (MockedStatic<ServiceLoader> mockedLoader = mockStatic(ServiceLoader.class)) {
      ServiceLoader<JobTemplateProvider> serviceLoader = mock(ServiceLoader.class);
      mockedLoader
          .when(() -> ServiceLoader.load(eq(JobTemplateProvider.class), any()))
          .thenReturn(serviceLoader);
      when(serviceLoader.iterator()).thenReturn(Lists.newArrayList(provider).iterator());
      when(serviceLoader.spliterator()).thenReturn(Lists.newArrayList(provider).spliterator());
      Mockito.doAnswer(
              invocation -> {
                Consumer<JobTemplateProvider> consumer = invocation.getArgument(0);
                consumer.accept(provider);
                return null;
              })
          .when(serviceLoader)
          .forEach(any());

      Map<String, JobTemplate> templates = listener.loadBuiltInJobTemplates();

      // Should only include template with valid version
      Assertions.assertEquals(1, templates.size());
      Assertions.assertTrue(templates.containsKey("builtin-valid"));
    }
  }

  @Test
  public void testReconcileBuiltInJobTemplatesNewRegistration() {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>();

    ShellJobTemplate newTemplate =
        ShellJobTemplate.builder()
            .withName("builtin-new")
            .withComment("new template")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    builtInTemplates.put("builtin-new", newTemplate);

    when(jobManager.listJobTemplates(metalakeName)).thenReturn(Collections.emptyList());
    doNothing().when(jobManager).registerJobTemplate(eq(metalakeName), any());

    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);

    verify(jobManager, times(1)).registerJobTemplate(eq(metalakeName), any());
  }

  @Test
  public void testReconcileBuiltInJobTemplatesUpdate() throws IOException {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>();

    ShellJobTemplate updatedTemplate =
        ShellJobTemplate.builder()
            .withName("builtin-existing")
            .withComment("updated")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v2"))
            .build();
    builtInTemplates.put("builtin-existing", updatedTemplate);

    // Create and store the existing entity in the real entityStore
    JobTemplateEntity existingEntity = createJobTemplateEntity("builtin-existing", "v1");
    entityStore.put(existingEntity, false);

    when(jobManager.listJobTemplates(metalakeName))
        .thenReturn(Collections.singletonList(existingEntity));

    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);

    // Verify the entity was updated by loading it
    JobTemplateEntity updated =
        entityStore.get(
            existingEntity.nameIdentifier(),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class);
    Assertions.assertEquals("v2", updated.templateContent().customFields().get("version"));
  }

  @Test
  public void testReconcileBuiltInJobTemplatesNoUpdateWhenVersionSame() throws IOException {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>();

    // Same version and identical content as the entity created by createJobTemplateEntity(...)
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-existing")
            .withComment("test")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    builtInTemplates.put("builtin-existing", template);

    // Create and store the existing entity
    JobTemplateEntity existingEntity = createJobTemplateEntity("builtin-existing", "v1");
    entityStore.put(existingEntity, false);

    when(jobManager.listJobTemplates(metalakeName))
        .thenReturn(Collections.singletonList(existingEntity));

    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);

    // Should not update or register
    verify(jobManager, times(0)).registerJobTemplate(any(), any());

    // Verify entity version is still v1
    JobTemplateEntity result =
        entityStore.get(
            existingEntity.nameIdentifier(),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class);
    Assertions.assertEquals("v1", result.templateContent().customFields().get("version"));

    // Verify no update actually happened: an update would stamp lastModifier/lastModifiedTime,
    // which createJobTemplateEntity(...) leaves null, so these staying null proves the entity
    // was left untouched rather than merely ending up with the same version by coincidence.
    Assertions.assertNull(result.auditInfo().lastModifier());
    Assertions.assertNull(result.auditInfo().lastModifiedTime());
  }

  @Test
  public void testReconcileBuiltInJobTemplatesUpdateWhenExecutableChangedButVersionSame()
      throws IOException {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>();

    // Same version as the persisted entity, but a different executable path, e.g. because the
    // Gravitino release version (and thus the jar filename) changed while the job logic did not.
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-existing")
            .withComment("test")
            .withExecutable("/opt/gravitino/auxlib/gravitino-jobs-2.0.0-SNAPSHOT.jar")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    builtInTemplates.put("builtin-existing", template);

    // Existing entity was persisted with the old jar path.
    JobTemplateEntity existingEntity = createJobTemplateEntity("builtin-existing", "v1");
    entityStore.put(existingEntity, false);

    when(jobManager.listJobTemplates(metalakeName))
        .thenReturn(Collections.singletonList(existingEntity));

    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);

    // Verify the entity's executable path was refreshed despite the version staying at v1
    JobTemplateEntity result =
        entityStore.get(
            existingEntity.nameIdentifier(),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class);
    Assertions.assertEquals("v1", result.templateContent().customFields().get("version"));
    Assertions.assertEquals(
        "/opt/gravitino/auxlib/gravitino-jobs-2.0.0-SNAPSHOT.jar",
        result.templateContent().executable());
  }

  @Test
  public void testReconcileBuiltInJobTemplatesDeleteObsolete() {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>(); // Empty - no templates

    JobTemplateEntity obsoleteEntity = createJobTemplateEntity("builtin-obsolete", "v1");

    when(jobManager.listJobTemplates(metalakeName))
        .thenReturn(Collections.singletonList(obsoleteEntity));
    doReturn(true).when(jobManager).deleteJobTemplate(metalakeName, "builtin-obsolete");

    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);

    // Should delete obsolete template
    verify(jobManager, times(1)).deleteJobTemplate(metalakeName, "builtin-obsolete");
  }

  @Test
  public void testReconcileHandlesRegistrationException() {
    String metalakeName = "test_metalake";
    Map<String, JobTemplate> builtInTemplates = new HashMap<>();

    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-new")
            .withComment("new")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    builtInTemplates.put("builtin-new", template);

    when(jobManager.listJobTemplates(metalakeName)).thenReturn(Collections.emptyList());
    Mockito.doThrow(new JobTemplateAlreadyExistsException("Already exists"))
        .when(jobManager)
        .registerJobTemplate(eq(metalakeName), any());

    // Should not throw exception
    listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates);
  }

  @Test
  public void testReconcileBuiltInJobTemplatesUpdateUsesExistingIdentifier() throws IOException {
    String metalakeName = "test_metalake";

    JobTemplateEntity existingEntity = createJobTemplateEntity("builtin-existing", "v1");
    entityStore.put(existingEntity, false);

    ShellJobTemplate newTemplate =
        ShellJobTemplate.builder()
            .withName("builtin-existing")
            .withComment("updated")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v2"))
            .build();

    Map<String, JobTemplate> builtInTemplates = new HashMap<>();
    builtInTemplates.put(newTemplate.name(), newTemplate);

    when(jobManager.listJobTemplates(metalakeName))
        .thenReturn(Collections.singletonList(existingEntity));

    Assertions.assertDoesNotThrow(
        () -> listener.reconcileBuiltInJobTemplates(metalakeName, builtInTemplates));

    JobTemplateEntity updated =
        entityStore.get(
            existingEntity.nameIdentifier(),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class);

    Assertions.assertNotNull(updated);
    Assertions.assertEquals("v2", updated.templateContent().customFields().get("version"));
    Assertions.assertEquals("builtin-existing", updated.name());
  }

  private BaseMetalake createMetalake(String name, boolean inUse) throws IOException {
    return putMetalake(name, idGenerator.nextId(), inUse);
  }

  private void replaceMetalake(String name, long id, boolean inUse) throws IOException {
    entityStore.delete(NameIdentifier.of(name), Entity.EntityType.METALAKE);
    putMetalake(name, id, inUse);
  }

  private BaseMetalake putMetalake(String name, long id, boolean inUse) throws IOException {
    Map<String, String> props = new HashMap<>();
    props.put(PROPERTY_IN_USE, String.valueOf(inUse));

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(id)
            .withName(name)
            .withComment("test metalake")
            .withProperties(props)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    entityStore.put(metalake, false);
    return metalake;
  }

  private JobTemplateEntity createJobTemplateEntity(String name, String version) {
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName(name)
            .withComment("test")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", version))
            .build();

    return JobTemplateEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate("test_metalake"))
        // TemplateContent does not carry comment (see JobTemplateEntity#toJobTemplate(), which
        // reads comment from the entity itself), so it must be set explicitly here to match what
        // registerNewBuiltInJobTemplate(...) does in production and keep entity.toJobTemplate()
        // consistent with `template` above.
        .withComment(template.comment())
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(template))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  /**
   * Creates a listener with a real registration thread, whose first template load blocks until
   * {@code releaseFirstLoad} is counted down.
   */
  private BuiltInJobTemplateEventListener newBlockingListener(
      CountDownLatch firstLoadStarted, CountDownLatch releaseFirstLoad) {
    return newBlockingListener(
        firstLoadStarted, releaseFirstLoad, Executors.newSingleThreadExecutor());
  }

  private BuiltInJobTemplateEventListener newBlockingListener(
      CountDownLatch firstLoadStarted,
      CountDownLatch releaseFirstLoad,
      ExecutorService registrationExecutor) {
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-test")
            .withExecutable("/bin/echo")
            .withCustomFields(Collections.singletonMap("version", "v1"))
            .build();
    AtomicInteger loads = new AtomicInteger();
    return new BuiltInJobTemplateEventListener(
        jobManager, entityStore, idGenerator, registrationExecutor) {
      @Override
      Map<String, JobTemplate> loadBuiltInJobTemplates() {
        if (loads.incrementAndGet() == 1) {
          firstLoadStarted.countDown();
          try {
            releaseFirstLoad.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return Collections.singletonMap(template.name(), template);
      }
    };
  }

  private CreateMetalakeEvent createMetalakeEvent(String metalakeName) {
    return new CreateMetalakeEvent(
        "user",
        NameIdentifier.of(metalakeName),
        new MetalakeInfo(metalakeName, "comment", null, null));
  }
}
