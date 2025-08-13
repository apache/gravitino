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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestJobManager {

  private static JobManager jobManager;

  private static EntityStore entityStore;

  private static Config config;

  private static String testStagingDir;

  private static String metalake = "test_metalake";

  private static NameIdentifier metalakeIdent = NameIdentifier.of(metalake);

  private static MockedStatic<MetalakeManager> mockedMetalake;

  private static JobExecutor jobExecutor;

  private static IdGenerator idGenerator;

  @BeforeAll
  public static void setUp() throws IllegalAccessException {
    config = new Config(false) {};
    Random rand = new Random();
    testStagingDir = "test_staging_dir_" + rand.nextInt(100);
    config.set(Configs.JOB_STAGING_DIR, testStagingDir);
    config.set(Configs.JOB_STAGING_DIR_KEEP_TIME_IN_MS, 1000L);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    entityStore = Mockito.mock(EntityStore.class);
    jobExecutor = Mockito.mock(JobExecutor.class);
    idGenerator = new RandomIdGenerator();
    JobManager jm = new JobManager(config, entityStore, idGenerator, jobExecutor);
    jobManager = Mockito.spy(jm);

    mockedMetalake = mockStatic(MetalakeManager.class);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    // Clean up resources if necessary
    jobManager.close();
    FileUtils.deleteDirectory(new File(testStagingDir));
    mockedMetalake.close();
  }

  @AfterEach
  public void reset() {
    // Reset the mocked static methods after each test
    mockedMetalake.reset();
    Mockito.reset(entityStore);
    Mockito.reset(jobManager);
  }

  @Test
  public void testListJobTemplates() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);
    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    JobTemplateEntity sparkJobTemplate =
        newSparkJobTemplateEntity("spark_job", "A spark job template");

    when(entityStore.list(
            NamespaceUtil.ofJobTemplate(metalake),
            JobTemplateEntity.class,
            Entity.EntityType.JOB_TEMPLATE))
        .thenReturn(Lists.newArrayList(shellJobTemplate, sparkJobTemplate));

    List<JobTemplateEntity> templates = jobManager.listJobTemplates(metalake);
    Assertions.assertEquals(2, templates.size());
    Assertions.assertTrue(templates.contains(shellJobTemplate));
    Assertions.assertTrue(templates.contains(sparkJobTemplate));

    // Throw exception if metalake does not exist
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(NameIdentifier.of("non_existent"), entityStore))
        .thenThrow(new NoSuchMetalakeException("Metalake does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> jobManager.listJobTemplates("non_existent"));
    Assertions.assertEquals("Metalake does not exist", e.getMessage());

    // Throw exception if metalake is in use
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenThrow(new MetalakeInUseException("Metalake is in use"));

    e =
        Assertions.assertThrows(
            MetalakeInUseException.class, () -> jobManager.listJobTemplates(metalake));
    Assertions.assertEquals("Metalake is in use", e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .list(
            NamespaceUtil.ofJobTemplate(metalake),
            JobTemplateEntity.class,
            Entity.EntityType.JOB_TEMPLATE);

    Assertions.assertThrows(RuntimeException.class, () -> jobManager.listJobTemplates(metalake));
  }

  @Test
  public void testRegisterJobTemplate() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    doNothing().when(entityStore).put(shellJobTemplate, false);

    // Register a new job template
    Assertions.assertDoesNotThrow(() -> jobManager.registerJobTemplate(metalake, shellJobTemplate));

    // Throw exception if job template already exists
    doThrow(new EntityAlreadyExistsException("Job template already exists"))
        .when(entityStore)
        .put(shellJobTemplate, false /* overwrite */);

    Exception e =
        Assertions.assertThrows(
            JobTemplateAlreadyExistsException.class,
            () -> jobManager.registerJobTemplate(metalake, shellJobTemplate));
    Assertions.assertEquals(
        "Job template with name shell_job under metalake test_metalake already exists",
        e.getMessage());

    // Throw exception if metalake does not exist
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(NameIdentifier.of("non_existent"), entityStore))
        .thenThrow(new NoSuchMetalakeException("Metalake does not exist"));

    e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> jobManager.registerJobTemplate("non_existent", shellJobTemplate));
    Assertions.assertEquals("Metalake does not exist", e.getMessage());

    // Throw exception if metalake is in use
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenThrow(new MetalakeInUseException("Metalake is in use"));

    e =
        Assertions.assertThrows(
            MetalakeInUseException.class,
            () -> jobManager.registerJobTemplate(metalake, shellJobTemplate));
    Assertions.assertEquals("Metalake is in use", e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .put(shellJobTemplate, false /* overwrite */);

    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.registerJobTemplate(metalake, shellJobTemplate));
  }

  @Test
  public void testGetJobTemplate() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(entityStore.get(
            NameIdentifierUtil.ofJobTemplate(metalake, shellJobTemplate.name()),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class))
        .thenReturn(shellJobTemplate);

    // Get an existing job template
    JobTemplateEntity retrievedTemplate = jobManager.getJobTemplate(metalake, "shell_job");
    Assertions.assertEquals(shellJobTemplate, retrievedTemplate);

    // Throw exception if job template does not exist
    when(entityStore.get(
            NameIdentifierUtil.ofJobTemplate(metalake, "non_existent"),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class))
        .thenThrow(new NoSuchEntityException("Job template does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobTemplateException.class,
            () -> jobManager.getJobTemplate(metalake, "non_existent"));
    Assertions.assertEquals(
        "Job template with name non_existent under metalake test_metalake does not exist",
        e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .get(
            NameIdentifierUtil.ofJobTemplate(metalake, "job"),
            Entity.EntityType.JOB_TEMPLATE,
            JobTemplateEntity.class);
    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.getJobTemplate(metalake, "job"));
  }

  @Test
  public void testDeleteJobTemplate() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    doReturn(true)
        .when(entityStore)
        .delete(
            NameIdentifierUtil.ofJobTemplate(metalake, shellJobTemplate.name()),
            Entity.EntityType.JOB_TEMPLATE);

    doReturn(Collections.emptyList())
        .when(jobManager)
        .listJobs(metalake, Optional.of(shellJobTemplate.name()));

    // Delete an existing job template
    Assertions.assertTrue(() -> jobManager.deleteJobTemplate(metalake, "shell_job"));

    doReturn(false)
        .when(entityStore)
        .delete(
            NameIdentifierUtil.ofJobTemplate(metalake, "shell_job"),
            Entity.EntityType.JOB_TEMPLATE);

    // Delete a non-existing job template
    Assertions.assertFalse(() -> jobManager.deleteJobTemplate(metalake, "shell_job"));

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .delete(NameIdentifierUtil.ofJobTemplate(metalake, "job"), Entity.EntityType.JOB_TEMPLATE);
    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.deleteJobTemplate(metalake, "job"));

    // Delete job template that is in use
    Lists.newArrayList(
            JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING)
        .forEach(
            status -> {
              doReturn(Lists.newArrayList(newJobEntity("shell_job", status)))
                  .when(jobManager)
                  .listJobs(metalake, Optional.of(shellJobTemplate.name()));
              Assertions.assertThrows(
                  InUseException.class, () -> jobManager.deleteJobTemplate(metalake, "shell_job"));
            });

    // Delete job template that is not in use
    Lists.newArrayList(
            JobHandle.Status.CANCELLED, JobHandle.Status.FAILED, JobHandle.Status.SUCCEEDED)
        .forEach(
            status -> {
              doReturn(Lists.newArrayList(newJobEntity("shell_job", status)))
                  .when(jobManager)
                  .listJobs(metalake, Optional.of(shellJobTemplate.name()));
              Assertions.assertDoesNotThrow(
                  () -> jobManager.deleteJobTemplate(metalake, "shell_job"));
            });
  }

  @Test
  public void testListJobs() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobManager.getJobTemplate(metalake, shellJobTemplate.name())).thenReturn(shellJobTemplate);

    JobEntity job1 = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    JobEntity job2 = newJobEntity("spark_job", JobHandle.Status.QUEUED);

    String[] levels =
        ArrayUtils.add(shellJobTemplate.namespace().levels(), shellJobTemplate.name());
    Namespace jobTemplateIdentNs = Namespace.of(levels);
    when(entityStore.list(jobTemplateIdentNs, JobEntity.class, Entity.EntityType.JOB))
        .thenReturn(Lists.newArrayList(job1));

    List<JobEntity> jobs = jobManager.listJobs(metalake, Optional.of(shellJobTemplate.name()));
    Assertions.assertEquals(1, jobs.size());
    Assertions.assertTrue(jobs.contains(job1));
    Assertions.assertFalse(jobs.contains(job2));

    // List all jobs without filtering by job template
    // Mock the listJobs method to return a list of jobs associated with the job template
    when(entityStore.list(NamespaceUtil.ofJob(metalake), JobEntity.class, Entity.EntityType.JOB))
        .thenReturn(Lists.newArrayList(job1, job2));

    jobs = jobManager.listJobs(metalake, Optional.empty());
    Assertions.assertEquals(2, jobs.size());
    Assertions.assertTrue(jobs.contains(job1));
    Assertions.assertTrue(jobs.contains(job2));

    // Throw exception if job template does not exist
    when(jobManager.getJobTemplate(metalake, "non_existent"))
        .thenThrow(new NoSuchJobTemplateException("Job template does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobTemplateException.class,
            () -> jobManager.listJobs(metalake, Optional.of("non_existent")));
    Assertions.assertEquals("Job template does not exist", e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .list(NamespaceUtil.ofJob(metalake), JobEntity.class, Entity.EntityType.JOB);
    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.listJobs(metalake, Optional.empty()));
  }

  @Test
  public void testGetJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB, JobEntity.class))
        .thenReturn(job);

    // Get an existing job
    JobEntity retrievedJob = jobManager.getJob(metalake, job.name());
    Assertions.assertEquals(job, retrievedJob);

    // Throw exception if job does not exist
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, "non_existent"),
            Entity.EntityType.JOB,
            JobEntity.class))
        .thenThrow(new NoSuchEntityException("Job does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> jobManager.getJob(metalake, "non_existent"));
    Assertions.assertEquals(
        "Job with ID non_existent under metalake test_metalake does not exist", e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .get(NameIdentifierUtil.ofJob(metalake, "job"), Entity.EntityType.JOB, JobEntity.class);
    Assertions.assertThrows(RuntimeException.class, () -> jobManager.getJob(metalake, "job"));
  }

  @Test
  public void testRunJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobManager.getJobTemplate(metalake, shellJobTemplate.name())).thenReturn(shellJobTemplate);

    String jobExecutionId = "job_execution_id_for_test";
    when(jobExecutor.submitJob(any())).thenReturn(jobExecutionId);

    doNothing().when(entityStore).put(any(JobEntity.class), anyBoolean());

    JobEntity jobEntity = jobManager.runJob(metalake, "shell_job", Collections.emptyMap());

    Assertions.assertEquals(jobExecutionId, jobEntity.jobExecutionId());
    Assertions.assertEquals("shell_job", jobEntity.jobTemplateName());
    Assertions.assertEquals(JobHandle.Status.QUEUED, jobEntity.status());

    // Test when job template does not exist
    when(jobManager.getJobTemplate(metalake, "non_existent"))
        .thenThrow(new NoSuchJobTemplateException("Job template does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobTemplateException.class,
            () -> jobManager.runJob(metalake, "non_existent", Collections.emptyMap()));
    Assertions.assertEquals("Job template does not exist", e.getMessage());

    // Test when job executor fails
    doThrow(new RuntimeException("Job executor error")).when(jobExecutor).submitJob(any());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> jobManager.runJob(metalake, "shell_job", Collections.emptyMap()));

    // Test when entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .put(any(JobEntity.class), anyBoolean());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> jobManager.runJob(metalake, "shell_job", Collections.emptyMap()));
  }

  @Test
  public void testCancelJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, job.name())).thenReturn(job);
    doNothing().when(jobExecutor).cancelJob(job.jobExecutionId());
    doNothing().when(entityStore).put(any(JobEntity.class), anyBoolean());

    // Cancel an existing job
    JobEntity cancelledJob = jobManager.cancelJob(metalake, job.name());
    Assertions.assertEquals(job.jobExecutionId(), cancelledJob.jobExecutionId());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancelledJob.status());

    // Test cancel a nonexistent job
    when(jobManager.getJob(metalake, "non_existent"))
        .thenThrow(new NoSuchJobException("Job does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> jobManager.cancelJob(metalake, "non_existent"));
    Assertions.assertEquals("Job does not exist", e.getMessage());

    // Test cancelling a finished job
    Lists.newArrayList(
            JobHandle.Status.CANCELLED, JobHandle.Status.FAILED, JobHandle.Status.SUCCEEDED)
        .forEach(
            status -> {
              JobEntity finishedJob = newJobEntity("shell_job", status);
              when(jobManager.getJob(metalake, finishedJob.name())).thenReturn(finishedJob);

              JobEntity cancelledFinishedJob = jobManager.cancelJob(metalake, finishedJob.name());
              Assertions.assertEquals(
                  finishedJob.jobExecutionId(), cancelledFinishedJob.jobExecutionId());
              Assertions.assertEquals(status, cancelledFinishedJob.status());
            });

    // Test job executor failed to cancel the job
    doThrow(new RuntimeException("Job executor error"))
        .when(jobExecutor)
        .cancelJob(job.jobExecutionId());
    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.cancelJob(metalake, job.name()));

    // Test when entity store failed to update the job status
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .put(any(JobEntity.class), anyBoolean());

    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.cancelJob(metalake, job.name()));
  }

  @Test
  public void testPullJobStatus() throws IOException {
    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(job));

    when(jobExecutor.getJobStatus(job.jobExecutionId())).thenReturn(JobHandle.Status.QUEUED);
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());
    verify(entityStore, never()).put(any(), anyBoolean());

    when(jobExecutor.getJobStatus(job.jobExecutionId())).thenReturn(JobHandle.Status.SUCCEEDED);
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());
    verify(entityStore, times(1)).put(any(JobEntity.class), anyBoolean());
  }

  @Test
  public void testCleanUpStagingDirs() throws IOException, InterruptedException {
    JobEntity job = newJobEntity("shell_job", JobHandle.Status.STARTED);
    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));

    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(job));
    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());
    verify(entityStore, never()).delete(any(), any());

    JobEntity finishedJob = newJobEntity("shell_job", JobHandle.Status.SUCCEEDED);
    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(finishedJob));

    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .until(
            () -> {
              Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());
              try {
                verify(entityStore, times(1)).delete(any(), any());
                return true;
              } catch (Throwable e) {
                return false;
              }
            });
  }

  private static JobTemplateEntity newShellJobTemplateEntity(String name, String comment) {
    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withExecutable("/bin/echo")
            .build();

    Random rand = new Random();
    return JobTemplateEntity.builder()
        .withId(rand.nextLong())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static JobTemplateEntity newSparkJobTemplateEntity(String name, String comment) {
    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withClassName("org.apache.spark.examples.SparkPi")
            .withExecutable("file:/path/to/spark-examples.jar")
            .build();

    Random rand = new Random();
    return JobTemplateEntity.builder()
        .withId(rand.nextLong())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static JobEntity newJobEntity(String templateName, JobHandle.Status status) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withJobExecutionId(rand.nextLong() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withFinishedAt(System.currentTimeMillis())
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
