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
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
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
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testcontainers.shaded.org.apache.commons.lang3.reflect.FieldUtils;

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
    listener = new BuiltInJobTemplateEventListener(jobManager, entityStore, idGenerator);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
    }
  }

  @Test
  public void testOnPostCreateMetalakeEvent() {
    String metalakeName = "test_metalake";
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

      // Verify registerJobTemplate was called
      verify(jobManager, times(1)).registerJobTemplate(eq(metalakeName), any());
    }
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

    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("builtin-existing")
            .withComment("same version")
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

  private void createMetalake(String name, boolean inUse) throws IOException {
    Map<String, String> props = new HashMap<>();
    props.put(PROPERTY_IN_USE, String.valueOf(inUse));

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(name)
            .withComment("test metalake")
            .withProperties(props)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    entityStore.put(metalake, false);
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
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(template))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
