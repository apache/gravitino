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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.job.local.LocalJobExecutor;
import org.apache.gravitino.job.local.LocalJobExecutorConfigs;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.FileFetcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestJobManager {

  private JobManager jobManager;

  private EntityStore entityStore;

  private Config config;

  private String testStagingDir;

  private String metalake = "test_metalake";

  private NameIdentifier metalakeIdent = NameIdentifier.of(metalake);

  private MockedStatic<MetalakeManager> mockedMetalake;

  private JobExecutor jobExecutor;

  private IdGenerator idGenerator;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    config = new Config(false) {};
    testStagingDir = "test_staging_dir_" + UUID.randomUUID().toString();
    config.set(Configs.JOB_STAGING_DIR, testStagingDir);
    config.set(Configs.JOB_STAGING_DIR_KEEP_TIME_IN_MS, 1000L);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    entityStore = Mockito.mock(EntityStore.class);
    jobExecutor = Mockito.mock(JobExecutor.class);
    // Mocks don't call the default interface methods, so stub the defaults explicitly.
    when(jobExecutor.ownsJob(any())).thenReturn(true);
    idGenerator = new RandomIdGenerator();
    JobManager jm = new JobManager(config, entityStore, idGenerator, jobExecutor);
    jobManager = Mockito.spy(jm);

    // Stop the background schedulers to prevent interference with tests
    ScheduledExecutorService cleanUpExecutor = jobManager.cleanUpExecutor;
    if (cleanUpExecutor != null) {
      cleanUpExecutor.shutdownNow();
      try {
        if (!cleanUpExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          cleanUpExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        cleanUpExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    ScheduledExecutorService statusPullExecutor = jobManager.statusPullExecutor;
    if (statusPullExecutor != null) {
      statusPullExecutor.shutdown();
      try {
        if (!statusPullExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          statusPullExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        statusPullExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    mockedMetalake = mockStatic(MetalakeManager.class);
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Reset mocks to ensure test isolation
    if (mockedMetalake != null) {
      mockedMetalake.reset();
    }
    Mockito.reset(entityStore, jobManager);
    // Clean up resources if necessary
    jobManager.close();
    FileUtils.deleteDirectory(new File(testStagingDir));
    if (mockedMetalake != null) {
      mockedMetalake.close();
    }
  }

  @Test
  public void testRegisterJobTemplateReportsConcurrentMetalakeDeletion() throws IOException {
    JobTemplateEntity template = newShellJobTemplateEntity("shell_job", "A shell job template");
    NoSuchEntityException missing = new NoSuchEntityException("Metalake was deleted");
    doThrow(missing).when(entityStore).put(template, false);

    NoSuchMetalakeException failure =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> jobManager.registerJobTemplate(metalake, template));
    Assertions.assertSame(missing, failure.getCause());
  }

  @Test
  public void testRunJobReportsParentDisappearingDuringRegistration() throws Exception {
    JobTemplateEntity template = newShellJobTemplateEntity("shell_job", "A shell job template");
    doReturn(template).when(jobManager).getJobTemplate(metalake, template.name());
    for (Entity.EntityType parent :
        List.of(Entity.EntityType.METALAKE, Entity.EntityType.JOB_TEMPLATE)) {
      Mockito.reset(entityStore, jobExecutor);
      String executionId = "submitted_" + parent.name();
      when(jobExecutor.submitJob(any())).thenReturn(executionId);
      NoSuchEntityException missing = new NoSuchEntityException("Parent was deleted: %s", parent);
      doThrow(missing).when(entityStore).put(any(JobEntity.class), eq(false));

      NoSuchJobTemplateException failure =
          Assertions.assertThrows(
              NoSuchJobTemplateException.class,
              () -> jobManager.runJob(metalake, template.name(), Collections.emptyMap()));
      Assertions.assertSame(missing, failure.getCause());
      verify(jobExecutor, times(1)).submitJob(any());
      verify(jobExecutor, never()).cancelJob(any());
      verify(entityStore, times(1)).put(any(JobEntity.class), eq(false));
    }
  }

  @Test
  public void testAlterJobTemplateDistinguishesMissingFromConflict() throws IOException {
    NoSuchEntityException missing = new NoSuchEntityException("Template was deleted");
    doThrow(missing).when(entityStore).update(any(), any(), any(), any());
    NoSuchJobTemplateException failure =
        Assertions.assertThrows(
            NoSuchJobTemplateException.class,
            () -> jobManager.alterJobTemplate(metalake, "shell_job"));
    Assertions.assertEquals(
        "Job template with name shell_job under metalake " + metalake + " does not exist",
        failure.getMessage());

    OptimisticLockException conflict = new OptimisticLockException("Template was modified");
    doThrow(conflict).when(entityStore).update(any(), any(), any(), any());
    Assertions.assertSame(
        conflict,
        Assertions.assertThrows(
            OptimisticLockException.class,
            () -> jobManager.alterJobTemplate(metalake, "shell_job")));
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

  /** A failed root CAS must not remove files belonging to the still-active template. */
  @Test
  public void testDeleteJobTemplateConflictPreservesStaging() throws IOException {
    JobEntity finishedJob = expiredJob();
    doReturn(Collections.singletonList(finishedJob))
        .when(jobManager)
        .listJobs(metalake, Optional.of("shell_job"));
    doThrow(new OptimisticLockException("template changed"))
        .when(entityStore)
        .delete(
            NameIdentifierUtil.ofJobTemplate(metalake, "shell_job"),
            Entity.EntityType.JOB_TEMPLATE);
    File directory = jobManager.jobStagingDir(finishedJob.id());
    Assertions.assertTrue(directory.mkdirs() || directory.isDirectory());
    File artifact = new File(directory, "artifact");
    Assertions.assertTrue(artifact.createNewFile());
    Assertions.assertThrows(
        OptimisticLockException.class, () -> jobManager.deleteJobTemplate(metalake, "shell_job"));
    Assertions.assertTrue(artifact.isFile());
    doReturn(true)
        .when(entityStore)
        .delete(
            NameIdentifierUtil.ofJobTemplate(metalake, "shell_job"),
            Entity.EntityType.JOB_TEMPLATE);
    Assertions.assertTrue(jobManager.deleteJobTemplate(metalake, "shell_job"));
    Assertions.assertFalse(directory.exists());
  }

  /** A successful delete must preserve files belonging to a same-name replacement. */
  @Test
  public void testDeletePreservesReplacementStaging() throws IOException {
    doReturn(Collections.emptyList()).when(jobManager).listJobs(metalake, Optional.of("shell_job"));
    File replacementDir = jobManager.jobStagingDir(999L);
    File replacementArtifact = new File(replacementDir, "new-job-artifact");
    when(entityStore.delete(
            NameIdentifierUtil.ofJobTemplate(metalake, "shell_job"),
            Entity.EntityType.JOB_TEMPLATE))
        .thenAnswer(
            invocation -> {
              // The database delete has committed. Another server recreates the template and
              // stages a new job before this server resumes its filesystem cleanup.
              Assertions.assertTrue(replacementDir.mkdirs());
              Assertions.assertTrue(replacementArtifact.createNewFile());
              return true;
            });

    Assertions.assertTrue(jobManager.deleteJobTemplate(metalake, "shell_job"));
    Assertions.assertTrue(replacementArtifact.isFile(), "Replacement job files must survive");
  }

  /** A job inserted after the initial check must prevent deletion without losing staging files. */
  @Test
  public void testDeleteJobTemplateReportsConcurrentActiveJob() throws IOException {
    JobEntity finishedJob = expiredJob();
    doReturn(Collections.singletonList(finishedJob))
        .when(jobManager)
        .listJobs(metalake, Optional.of("shell_job"));
    File directory = jobManager.jobStagingDir(finishedJob.id());
    Assertions.assertTrue(directory.mkdirs());
    File artifact = new File(directory, "artifact");
    Assertions.assertTrue(artifact.createNewFile());
    doThrow(new NonEmptyEntityException("A job was inserted concurrently"))
        .when(entityStore)
        .delete(
            NameIdentifierUtil.ofJobTemplate(metalake, "shell_job"),
            Entity.EntityType.JOB_TEMPLATE);

    Assertions.assertThrows(
        InUseException.class, () -> jobManager.deleteJobTemplate(metalake, "shell_job"));
    Assertions.assertTrue(artifact.isFile());
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
    JobEntity retrievedJob = jobManager.getJob(metalake, job.name(), false);
    Assertions.assertEquals(job, retrievedJob);

    // Throw exception if job does not exist
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, "non_existent"),
            Entity.EntityType.JOB,
            JobEntity.class))
        .thenThrow(new NoSuchEntityException("Job does not exist"));

    Exception e =
        Assertions.assertThrows(
            NoSuchJobException.class, () -> jobManager.getJob(metalake, "non_existent", false));
    Assertions.assertEquals(
        "Job with ID non_existent under metalake test_metalake does not exist", e.getMessage());

    // Throw exception if entity store fails
    doThrow(new IOException("Entity store error"))
        .when(entityStore)
        .get(NameIdentifierUtil.ofJob(metalake, "job"), Entity.EntityType.JOB, JobEntity.class);
    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.getJob(metalake, "job", false));
  }

  @Test
  public void testGetJobWithOutput() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.SUCCEEDED);
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB, JobEntity.class))
        .thenReturn(job);

    List<String> stdout = ImmutableList.of("line1", "line2");
    List<String> stderr = ImmutableList.of("err1");
    // The default gravitino.job.outputMaxLines (1000) / outputMaxBytes (256KB) values are what
    // JobManager should resolve and pass through, since the test config doesn't override them.
    when(jobExecutor.getJobStdout(job.jobExecutionId(), 1000, 256 * 1024)).thenReturn(stdout);
    when(jobExecutor.getJobStderr(job.jobExecutionId(), 1000, 256 * 1024)).thenReturn(stderr);

    // includeOutput = true fetches and attaches the output.
    JobEntity jobWithOutput = jobManager.getJob(metalake, job.name(), true);
    Assertions.assertEquals(stdout, jobWithOutput.stdout());
    Assertions.assertEquals(stderr, jobWithOutput.stderr());
    // The rest of the entity is unaffected.
    Assertions.assertEquals(job.jobExecutionId(), jobWithOutput.jobExecutionId());
    Assertions.assertEquals(job.status(), jobWithOutput.status());

    // includeOutput = false never touches the executor for output.
    JobEntity jobWithoutOutput = jobManager.getJob(metalake, job.name(), false);
    Assertions.assertNull(jobWithoutOutput.stdout());
    Assertions.assertNull(jobWithoutOutput.stderr());

    verify(jobExecutor, times(1)).getJobStdout(job.jobExecutionId(), 1000, 256 * 1024);
    verify(jobExecutor, times(1)).getJobStderr(job.jobExecutionId(), 1000, 256 * 1024);
  }

  @Test
  public void testGetJobWithOutputPerRequestCapsAreClampedToGlobal() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.SUCCEEDED);
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB, JobEntity.class))
        .thenReturn(job);

    List<String> stdout = ImmutableList.of("line1");
    List<String> stderr = ImmutableList.of();

    // A per-request value smaller than the global default (1000 lines / 256KB) is honored as-is.
    when(jobExecutor.getJobStdout(job.jobExecutionId(), 10, 1024)).thenReturn(stdout);
    when(jobExecutor.getJobStderr(job.jobExecutionId(), 10, 1024)).thenReturn(stderr);
    JobEntity narrower = jobManager.getJob(metalake, job.name(), true, 10, 1024);
    Assertions.assertEquals(stdout, narrower.stdout());
    verify(jobExecutor, times(1)).getJobStdout(job.jobExecutionId(), 10, 1024);
    verify(jobExecutor, times(1)).getJobStderr(job.jobExecutionId(), 10, 1024);

    // A per-request value larger than the global default is clamped down to it - the global
    // configuration remains a hard upper bound, not just a fallback default.
    when(jobExecutor.getJobStdout(job.jobExecutionId(), 1000, 256 * 1024)).thenReturn(stdout);
    when(jobExecutor.getJobStderr(job.jobExecutionId(), 1000, 256 * 1024)).thenReturn(stderr);
    JobEntity clamped =
        jobManager.getJob(metalake, job.name(), true, 1_000_000, 1024 * 1024 * 1024);
    Assertions.assertEquals(stdout, clamped.stdout());
    verify(jobExecutor, times(1)).getJobStdout(job.jobExecutionId(), 1000, 256 * 1024);
    verify(jobExecutor, times(1)).getJobStderr(job.jobExecutionId(), 1000, 256 * 1024);

    // Non-positive per-request values are rejected outright.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> jobManager.getJob(metalake, job.name(), true, 0, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> jobManager.getJob(metalake, job.name(), true, null, -1));
  }

  @Test
  public void testGetJobIgnoresInvalidOutputCapsWhenOutputNotRequested() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.SUCCEEDED);
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB, JobEntity.class))
        .thenReturn(job);

    // maxLines/maxBytes are documented as ignored when includeOutput is false, so a non-positive
    // value here must not fail the call - it's never even inspected.
    JobEntity result = jobManager.getJob(metalake, job.name(), false, 0, -1);
    Assertions.assertEquals(job.jobExecutionId(), result.jobExecutionId());
    Assertions.assertNull(result.stdout());
    Assertions.assertNull(result.stderr());
    verify(jobExecutor, never()).getJobStdout(any(), anyInt(), anyInt());
    verify(jobExecutor, never()).getJobStderr(any(), anyInt(), anyInt());
  }

  @Test
  public void testGetJobWithOutputDegradesToEmptyWhenExecutorForgetsJob() throws IOException {
    // The job entity is confirmed to exist (via the entity store) before the executor is ever
    // consulted for output. The executor's own bookkeeping for a job's output is separate and can
    // legitimately expire or be lost (e.g. the local executor's in-memory state ages out
    // independently of the job entity/staging directory) - JobExecutor#getJobStdout/getJobStderr
    // report that as empty output, not an error, so getJob(..., true) must still return the full
    // entity with empty output rather than treat the job as missing.
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.SUCCEEDED);
    when(entityStore.get(
            NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB, JobEntity.class))
        .thenReturn(job);

    when(jobExecutor.getJobStdout(job.jobExecutionId(), 1000, 256 * 1024))
        .thenReturn(Collections.emptyList());
    when(jobExecutor.getJobStderr(job.jobExecutionId(), 1000, 256 * 1024))
        .thenReturn(Collections.emptyList());

    JobEntity jobWithOutput = jobManager.getJob(metalake, job.name(), true);
    Assertions.assertEquals(Collections.emptyList(), jobWithOutput.stdout());
    Assertions.assertEquals(Collections.emptyList(), jobWithOutput.stderr());
    // The entity itself is still fully returned, not treated as missing.
    Assertions.assertEquals(job.jobExecutionId(), jobWithOutput.jobExecutionId());
    Assertions.assertEquals(job.status(), jobWithOutput.status());
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
  public void testRunJobQueuesJobBeforeSubmission() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);
    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobManager.getJobTemplate(metalake, shellJobTemplate.name())).thenReturn(shellJobTemplate);
    doNothing().when(entityStore).put(any(JobEntity.class), anyBoolean());

    // The job executor may start the job right after it is submitted, so the queued time must be
    // taken before the submission to never be later than the reported started time.
    AtomicReference<Instant> submittedAt = new AtomicReference<>();
    when(jobExecutor.submitJob(any()))
        .thenAnswer(
            invocation -> {
              Thread.sleep(5);
              submittedAt.set(Instant.now());
              return "job_execution_id_for_test";
            });

    JobEntity jobEntity = jobManager.runJob(metalake, "shell_job", Collections.emptyMap());

    Assertions.assertTrue(
        jobEntity.auditInfo().createTime().isBefore(submittedAt.get()),
        "The queued time should be taken before the job is submitted");
  }

  @Test
  public void testRunJobPropagatesJobExecutorRejection() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobManager.getJobTemplate(metalake, shellJobTemplate.name())).thenReturn(shellJobTemplate);

    IllegalArgumentException rejection =
        new IllegalArgumentException(
            "gravitino.jobExecutor.local.sparkHome or SPARK_HOME environment variable must"
                + " be set for Spark jobs");
    doThrow(rejection).when(jobExecutor).submitJob(any());

    // The rejection must reach the caller as is, so the REST layer reports the original reason
    // with a 400 instead of wrapping it into a generic 500 error.
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> jobManager.runJob(metalake, "shell_job", Collections.emptyMap()));
    Assertions.assertSame(rejection, e);

    // No job entity is registered and the staging directory of the rejected job is removed.
    verify(entityStore, never()).put(any(JobEntity.class), anyBoolean());
    File jobRunsDir = jobManager.jobStagingDir(0L).getParentFile();
    Assertions.assertTrue(jobRunsDir.isDirectory(), "The job staging directory was never created");
    Assertions.assertArrayEquals(new String[0], jobRunsDir.list());
  }

  @Test
  public void testRunJobPopulatesResolvedRuntimeJobTemplate() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    ShellJobTemplate templateWithPlaceholder =
        ShellJobTemplate.builder()
            .withName("shell_job_with_placeholder")
            .withComment("A shell job template with a placeholder")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("{{greeting}}"))
            .build();
    JobTemplateEntity jobTemplateEntity =
        JobTemplateEntity.builder()
            .withId(new Random().nextLong())
            .withName(templateWithPlaceholder.name())
            .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(templateWithPlaceholder))
            .withComment(templateWithPlaceholder.comment())
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(jobManager.getJobTemplate(metalake, jobTemplateEntity.name()))
        .thenReturn(jobTemplateEntity);

    when(jobExecutor.submitJob(any())).thenReturn("job_execution_id_for_test");
    doNothing().when(entityStore).put(any(JobEntity.class), anyBoolean());

    JobEntity jobEntity =
        jobManager.runJob(
            metalake, jobTemplateEntity.name(), Collections.singletonMap("greeting", "Hello!"));

    Assertions.assertNotNull(jobEntity.runtimeJobTemplate());
    ShellJobTemplateDTO runtimeJobTemplateDTO =
        (ShellJobTemplateDTO)
            JsonUtils.anyFieldMapper()
                .readValue(jobEntity.runtimeJobTemplate(), JobTemplateDTO.class);

    // The resolved runtime template must carry the actual value substituted for the placeholder,
    // not the original template's raw {{greeting}} string.
    Assertions.assertEquals(Lists.newArrayList("Hello!"), runtimeJobTemplateDTO.arguments());
    Assertions.assertEquals(jobTemplateEntity.name(), runtimeJobTemplateDTO.name());
    Assertions.assertEquals(jobTemplateEntity.comment(), runtimeJobTemplateDTO.comment());
    // createRuntimeJobTemplate() also resolves the executable by fetching it into the job's
    // staging directory, so it ends up as a local staging-dir path rather than the original
    // "/bin/echo" - just confirm it was actually resolved to something under that directory.
    Assertions.assertTrue(
        runtimeJobTemplateDTO.executable().endsWith("echo"),
        () -> "Unexpected resolved executable: " + runtimeJobTemplateDTO.executable());
  }

  @Test
  public void testRunJobSucceedsWhenStagingDirectoryAlreadyExists() throws Exception {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobExecutor.submitJob(any())).thenReturn("job_execution_id_for_test");
    doNothing().when(entityStore).put(any(JobEntity.class), anyBoolean());

    // Use a fixed job ID so that both runs resolve to the same staging directory.
    IdGenerator fixedIdGenerator = Mockito.mock(IdGenerator.class);
    when(fixedIdGenerator.nextId()).thenReturn(12345L);
    JobManager fixedIdJobManager =
        Mockito.spy(new JobManager(config, entityStore, fixedIdGenerator, jobExecutor));
    try {
      // Stop the background schedulers to prevent interference with the test, like setUp does.
      fixedIdJobManager.cleanUpExecutor.shutdownNow();
      fixedIdJobManager.statusPullExecutor.shutdownNow();
      when(fixedIdJobManager.getJobTemplate(metalake, shellJobTemplate.name()))
          .thenReturn(shellJobTemplate);

      JobEntity first = fixedIdJobManager.runJob(metalake, "shell_job", Collections.emptyMap());
      Assertions.assertEquals(12345L, first.id());

      // The staging directory for job 12345 exists now; running the job again must not fail on
      // directory creation.
      JobEntity second = fixedIdJobManager.runJob(metalake, "shell_job", Collections.emptyMap());
      Assertions.assertEquals(12345L, second.id());
    } finally {
      fixedIdJobManager.close();
    }
  }

  /** A metadata conflict must not replay the external cancellation operation. */
  @Test
  public void testCancelJobDoesNotReplayExecutorOnOccConflict() throws IOException {
    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, job.name(), false)).thenReturn(job);
    doNothing().when(jobExecutor).cancelJob(job.jobExecutionId());
    when(entityStore.update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any()))
        .thenThrow(new OptimisticLockException("job changed"));
    Assertions.assertThrows(
        OptimisticLockException.class, () -> jobManager.cancelJob(metalake, job.name()));
    verify(jobExecutor, times(1)).cancelJob(job.jobExecutionId());
  }

  @Test
  public void testCancelJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, job.name(), false)).thenReturn(job);
    doNothing().when(jobExecutor).cancelJob(job.jobExecutionId());
    stubEntityStoreUpdateToApply(job);

    // Cancel an existing job
    JobEntity cancelledJob = jobManager.cancelJob(metalake, job.name());
    Assertions.assertEquals(job.jobExecutionId(), cancelledJob.jobExecutionId());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancelledJob.status());

    // Test cancel a nonexistent job
    when(jobManager.getJob(metalake, "non_existent", false))
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
              when(jobManager.getJob(metalake, finishedJob.name(), false)).thenReturn(finishedJob);

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
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());

    Assertions.assertThrows(
        RuntimeException.class, () -> jobManager.cancelJob(metalake, job.name()));
  }

  @Test
  public void testCancelJobThrowsNoSuchJobExceptionWhenJobDeletedConcurrently() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, job.name(), false)).thenReturn(job);
    doNothing().when(jobExecutor).cancelJob(job.jobExecutionId());

    // Simulate the job having been deleted concurrently (e.g. by legacy-timeline cleanup) in the
    // gap between the getJob() snapshot above and the entityStore.update() call.
    when(entityStore.update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any()))
        .thenThrow(new NoSuchEntityException("Job does not exist"));

    Assertions.assertThrows(
        NoSuchJobException.class, () -> jobManager.cancelJob(metalake, job.name()));
  }

  @Test
  public void testCancelJobDoesNotRegressConcurrentlyFinishedJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    // getJob() observes the job as QUEUED (active), so the external cancel call fires. But by
    // the time entityStore.update() re-fetches the entity, a concurrent status poll has already
    // persisted a terminal status - that must not be regressed back to CANCELLING.
    JobEntity queuedSnapshot = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, queuedSnapshot.name(), false)).thenReturn(queuedSnapshot);
    doNothing().when(jobExecutor).cancelJob(queuedSnapshot.jobExecutionId());

    JobEntity latestSucceeded =
        JobEntity.builder()
            .withId(queuedSnapshot.id())
            .withJobExecutionId(queuedSnapshot.jobExecutionId())
            .withNamespace(queuedSnapshot.namespace())
            .withJobTemplateName(queuedSnapshot.jobTemplateName())
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withAuditInfo(queuedSnapshot.auditInfo())
            .withStartedAt(12345L)
            .withFinishedAt(67890L)
            .build();
    stubEntityStoreUpdateToApply(latestSucceeded);

    JobEntity result = jobManager.cancelJob(metalake, queuedSnapshot.name());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, result.status());
    Assertions.assertEquals(12345L, result.startedAt());
    Assertions.assertEquals(67890L, result.finishedAt());
  }

  @Test
  public void testCancelJobDoesNotRegressAlreadyCancellingJob() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    // A concurrent cancelJob() call already moved the job to CANCELLING between the getJob()
    // snapshot and this update; re-applying CANCELLING here must not stamp a fresh
    // lastModifiedTime over the entity the other writer already wrote.
    JobEntity queuedSnapshot = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    when(jobManager.getJob(metalake, queuedSnapshot.name(), false)).thenReturn(queuedSnapshot);
    doNothing().when(jobExecutor).cancelJob(queuedSnapshot.jobExecutionId());

    JobEntity latestCancelling =
        JobEntity.builder()
            .withId(queuedSnapshot.id())
            .withJobExecutionId(queuedSnapshot.jobExecutionId())
            .withNamespace(queuedSnapshot.namespace())
            .withJobTemplateName(queuedSnapshot.jobTemplateName())
            .withStatus(JobHandle.Status.CANCELLING)
            .withAuditInfo(queuedSnapshot.auditInfo())
            .withStartedAt(12345L)
            .withFinishedAt(0L)
            .build();
    stubEntityStoreUpdateToApply(latestCancelling);

    JobEntity result = jobManager.cancelJob(metalake, queuedSnapshot.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, result.status());
    Assertions.assertEquals(12345L, result.startedAt());
    Assertions.assertEquals(0L, result.finishedAt());
  }

  @Test
  public void testCancelJobPreservesRuntimeJobTemplate() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    String runtimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"shell_job\",\"executable\":\"/bin/echo\"}";
    JobEntity job =
        JobEntity.builder()
            .withId(new Random().nextLong())
            .withJobExecutionId(new Random().nextLong() + "")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.QUEUED)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(jobManager.getJob(metalake, job.name(), false)).thenReturn(job);
    doNothing().when(jobExecutor).cancelJob(job.jobExecutionId());
    stubEntityStoreUpdateToApply(job);

    // The runtime job template is fixed at job creation, so cancelling must carry it forward
    // unchanged rather than dropping it while rebuilding the entity for the CANCELLING status.
    JobEntity cancelledJob = jobManager.cancelJob(metalake, job.name());
    Assertions.assertEquals(runtimeJobTemplateJson, cancelledJob.runtimeJobTemplate());
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

    // Mock MetalakeManager.listInUseMetalakes to return the test metalake
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(job));

    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.QUEUED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());

    stubEntityStoreUpdateToApply(job);
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    // Once a job transitions to a terminal status, finishedAt must be set.
    JobEntity updatedJob = captureUpdatedJobEntity(job);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, updatedJob.status());
    Assertions.assertNotNull(updatedJob.finishedAt());
    Assertions.assertTrue(updatedJob.finishedAt() > 0);
  }

  @Test
  public void testPullJobStatusPreservesRuntimeJobTemplate() throws IOException {
    String runtimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"shell_job\",\"executable\":\"/bin/echo\"}";
    JobEntity job =
        JobEntity.builder()
            .withId(new Random().nextLong())
            .withJobExecutionId(new Random().nextLong() + "")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.QUEUED)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));
    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(job));
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    // The runtime job template is fixed at job creation, so a status-poll update must carry it
    // forward unchanged rather than dropping it while rebuilding the entity for the new status.
    JobEntity updatedJob = captureUpdatedJobEntity(job);
    Assertions.assertEquals(runtimeJobTemplateJson, updatedJob.runtimeJobTemplate());
  }

  @Test
  public void testPullJobStatusStartedAt() throws IOException {
    JobEntity job =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.QUEUED)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(job));

    // QUEUED -> STARTED: startedAt must be set, finishedAt must remain unset.
    stubEntityStoreUpdateToApply(job);
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.STARTED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity startedJob = captureUpdatedJobEntity(job);
    Assertions.assertEquals(JobHandle.Status.STARTED, startedJob.status());
    Assertions.assertNotNull(startedJob.startedAt());
    Assertions.assertTrue(startedJob.startedAt() > 0);
    Assertions.assertEquals(0L, startedJob.finishedAt());

    // STARTED -> SUCCEEDED: finishedAt must be set, and the previously-recorded startedAt must
    // be carried forward unchanged, not overwritten.
    Mockito.clearInvocations(entityStore);
    stubEntityStoreUpdateToApply(startedJob);
    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(startedJob));
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity finishedJob = captureUpdatedJobEntity(startedJob);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, finishedJob.status());
    Assertions.assertEquals(startedJob.startedAt(), finishedJob.startedAt());
    Assertions.assertNotNull(finishedJob.finishedAt());
    Assertions.assertTrue(finishedJob.finishedAt() > 0);
  }

  @Test
  public void testPullJobStatusStartedAtNotBackfilledOnDirectTerminalTransition()
      throws IOException {
    // A job that transitions QUEUED -> SUCCEEDED directly (skipping any poll that observes it
    // as STARTED) does not prove exactly when it started - e.g. LocalJobExecutor can also reach
    // FAILED directly from QUEUED without ever recording STARTED. Backfilling startedAt from the
    // queued time would understate queue latency and overstate execution duration, so startedAt
    // stays unset (0) unless a STARTED transition was actually observed.
    Instant queuedAt = Instant.now();
    JobEntity queuedJob =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.QUEUED)
            .withAuditInfo(AuditInfo.builder().withCreator("test").withCreateTime(queuedAt).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty())).thenReturn(ImmutableList.of(queuedJob));
    stubEntityStoreUpdateToApply(queuedJob);
    when(jobExecutor.getJobExecutionInfo(queuedJob.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity succeededJob = captureUpdatedJobEntity(queuedJob);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, succeededJob.status());
    Assertions.assertEquals(0L, succeededJob.startedAt());
    Assertions.assertNotNull(succeededJob.finishedAt());
    Assertions.assertTrue(succeededJob.finishedAt() > 0);
  }

  @Test
  public void testPullJobStatusStartedAtNotBackfilledOnDirectCancellation() throws IOException {
    // CANCELLED does not prove the job ever started (it may have been cancelled while still
    // QUEUED), so startedAt must NOT fall back to the queued time here - it stays unset.
    JobEntity cancellingJob =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.CANCELLING)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty()))
        .thenReturn(ImmutableList.of(cancellingJob));
    stubEntityStoreUpdateToApply(cancellingJob);
    when(jobExecutor.getJobExecutionInfo(cancellingJob.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.CANCELLED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity cancelledJob = captureUpdatedJobEntity(cancellingJob);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelledJob.status());
    Assertions.assertEquals(0L, cancelledJob.startedAt());
    Assertions.assertNotNull(cancelledJob.finishedAt());
    Assertions.assertTrue(cancelledJob.finishedAt() > 0);
  }

  @Test
  public void testPullJobStatusSkipsJobDeletedConcurrently() throws IOException {
    assertStatusPollingContinues(new NoSuchEntityException("Job does not exist"));
  }

  /** Verifies OCC conflicts do not cancel future status polls. */
  @Test
  public void testPullJobStatusContinuesAfterOccConflict() throws IOException {
    assertStatusPollingContinues(new OptimisticLockException("Job changed concurrently"));
  }

  @Test
  public void testPullJobStatusDoesNotRegressConcurrentlyFinishedJob() throws IOException {
    // listJobs() observes the job as QUEUED, but by the time entityStore.update() re-fetches it,
    // a concurrent writer (e.g. another poll run) has already finished the job with a different
    // terminal status. The stale QUEUED snapshot - and the executor status derived from it - must
    // not be allowed to regress that terminal state or clobber its recorded startedAt/finishedAt.
    JobEntity queuedSnapshot = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    JobEntity latestSucceeded =
        JobEntity.builder()
            .withId(queuedSnapshot.id())
            .withJobExecutionId(queuedSnapshot.jobExecutionId())
            .withNamespace(queuedSnapshot.namespace())
            .withJobTemplateName(queuedSnapshot.jobTemplateName())
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withAuditInfo(queuedSnapshot.auditInfo())
            .withStartedAt(12345L)
            .withFinishedAt(67890L)
            .build();

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty()))
        .thenReturn(ImmutableList.of(queuedSnapshot));
    stubEntityStoreUpdateToApply(latestSucceeded);
    // The stale QUEUED snapshot leads the poll to observe (and try to apply) FAILED - a
    // different terminal status than the one the job has actually already settled into.
    when(jobExecutor.getJobExecutionInfo(queuedSnapshot.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.FAILED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity result = captureUpdatedJobEntity(latestSucceeded);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, result.status());
    Assertions.assertEquals(12345L, result.startedAt());
    Assertions.assertEquals(67890L, result.finishedAt());
  }

  @Test
  public void testPullJobStatusDoesNotRegressConcurrentlyCancellingJob() throws IOException {
    // listJobs() observes the job as QUEUED, but a concurrent cancelJob() moves it to CANCELLING
    // before entityStore.update() re-fetches it. The executor reports STARTED for this poll
    // (a legitimate observation for the same jobExecutionId) - that must not move the job back
    // out of CANCELLING, nor overwrite the startedAt it already carries.
    JobEntity queuedSnapshot = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    JobEntity latestCancelling =
        JobEntity.builder()
            .withId(queuedSnapshot.id())
            .withJobExecutionId(queuedSnapshot.jobExecutionId())
            .withNamespace(queuedSnapshot.namespace())
            .withJobTemplateName(queuedSnapshot.jobTemplateName())
            .withStatus(JobHandle.Status.CANCELLING)
            .withAuditInfo(queuedSnapshot.auditInfo())
            .withStartedAt(12345L)
            .withFinishedAt(0L)
            .build();

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty()))
        .thenReturn(ImmutableList.of(queuedSnapshot));
    stubEntityStoreUpdateToApply(latestCancelling);
    when(jobExecutor.getJobExecutionInfo(queuedSnapshot.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.STARTED));
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity result = captureUpdatedJobEntity(latestCancelling);
    Assertions.assertEquals(JobHandle.Status.CANCELLING, result.status());
    Assertions.assertEquals(12345L, result.startedAt());
    Assertions.assertEquals(0L, result.finishedAt());
  }

  @Test
  public void testPullJobStatusSkipsJobOwnedByAnotherExecutor() throws IOException {
    // The jobs are run by the job executor on another server, so this server must neither query
    // their status (the local executor can't find them) nor touch the job entities, no matter how
    // long they have not been updated.
    Instant longAgo = Instant.now().minus(30, ChronoUnit.DAYS);
    JobEntity freshJob =
        newJobEntity("local-job-other-1", JobHandle.Status.QUEUED, Instant.now(), null);
    JobEntity staleJob =
        newJobEntity("local-job-other-2", JobHandle.Status.STARTED, longAgo, longAgo);
    mockListActiveJobs(freshJob, staleJob);
    when(jobExecutor.ownsJob(any())).thenReturn(false);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(jobExecutor, never()).getJobExecutionInfo(any());
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
  }

  @Test
  public void testPullJobStatusCancelsOwnedJobMarkedCancellingByAnotherServer() throws IOException {
    // A STARTED job marked as CANCELLING by another server is cancelled by its owner, and stays
    // CANCELLING until the process exits, so there's nothing to update.
    JobEntity startedJob =
        newJobEntity("local-job-mine-1", JobHandle.Status.CANCELLING, Instant.now(), null);
    mockListActiveJobs(startedJob);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    when(jobExecutor.getJobExecutionInfo(startedJob.jobExecutionId()))
        .thenReturn(
            JobExecutionInfo.of(JobHandle.Status.STARTED),
            JobExecutionInfo.of(JobHandle.Status.CANCELLING));
    stubEntityStoreUpdateToApply(startedJob);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(jobExecutor, times(1)).cancelJob(startedJob.jobExecutionId());
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());

    // A QUEUED job marked as CANCELLING by another server is cancelled right away.
    Mockito.clearInvocations(entityStore, jobExecutor);
    JobEntity queuedJob =
        newJobEntity("local-job-mine-2", JobHandle.Status.CANCELLING, Instant.now(), null);
    mockListActiveJobs(queuedJob);
    when(jobExecutor.getJobExecutionInfo(queuedJob.jobExecutionId()))
        .thenReturn(
            JobExecutionInfo.of(JobHandle.Status.QUEUED),
            JobExecutionInfo.of(JobHandle.Status.CANCELLED));
    stubEntityStoreUpdateToApply(queuedJob);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(jobExecutor, times(1)).cancelJob(queuedJob.jobExecutionId());
    JobEntity cancelled = captureUpdatedJobEntity(queuedJob);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelled.status());
    Assertions.assertTrue(cancelled.finishedAt() > 0);
  }

  @Test
  public void testPullJobStatusKeepsCancellingWhenOwnerFailsToCancel() throws IOException {
    JobEntity job =
        newJobEntity("local-job-mine-1", JobHandle.Status.CANCELLING, Instant.now(), null);
    mockListActiveJobs(job);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.STARTED));
    doThrow(new RuntimeException("cancel failed")).when(jobExecutor).cancelJob(any());
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    // Nothing changes, and the next poll retries the cancellation.
    verify(jobExecutor, times(1)).cancelJob(job.jobExecutionId());
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
  }

  @Test
  public void testPullJobStatusDoesNotRecancelJobOfNonNodeLocalExecutor() throws IOException {
    // An external job executor is cancelled directly by the server handling the cancel request,
    // and may keep reporting the job as running while cancelling it asynchronously. The status
    // pull must not cancel it again on every poll.
    JobEntity job =
        newJobEntity("external-job-1", JobHandle.Status.CANCELLING, Instant.now(), null);
    mockListActiveJobs(job);
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.STARTED));
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(jobExecutor, never()).cancelJob(any());
    // The observed STARTED status never regresses the CANCELLING job, so nothing is written.
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
  }

  @Test
  public void testPullJobStatusDoesNotCancelFinishedJobMarkedCancelling() throws IOException {
    // The job finished before its owner noticed the cancellation request.
    JobEntity job =
        newJobEntity("local-job-mine-1", JobHandle.Status.CANCELLING, Instant.now(), null);
    mockListActiveJobs(job);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    when(jobExecutor.getJobExecutionInfo(job.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(jobExecutor, never()).cancelJob(any());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, captureUpdatedJobEntity(job).status());
  }

  @Test
  public void testCancelJobOwnedByAnotherExecutor() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job =
        newJobEntity("local-job-other-1", JobHandle.Status.STARTED, Instant.now(), null);
    doReturn(job).when(jobManager).getJob(metalake, job.name(), false);
    when(jobExecutor.ownsJob(job.jobExecutionId())).thenReturn(false);
    stubEntityStoreUpdateToApply(job);

    // The job runs on another server, so only mark it as CANCELLING for its owner to cancel.
    JobEntity cancellingJob = jobManager.cancelJob(metalake, job.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancellingJob.status());
    verify(jobExecutor, never()).cancelJob(any());
  }

  @Test
  public void testCancelJobNotFoundInOwnedExecutor() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobEntity job = newJobEntity("local-job-mine-1", JobHandle.Status.STARTED, Instant.now(), null);
    doReturn(job).when(jobManager).getJob(metalake, job.name(), false);
    doThrow(new NoSuchJobException("lost")).when(jobExecutor).cancelJob(job.jobExecutionId());
    stubEntityStoreUpdateToApply(job);

    // The status pull later settles a job lost by the executor as CANCELLED.
    JobEntity cancellingJob = jobManager.cancelJob(metalake, job.name());
    Assertions.assertEquals(JobHandle.Status.CANCELLING, cancellingJob.status());
  }

  @Test
  public void testPullJobStatusAcrossServersWithLocalJobExecutors() throws Exception {
    // Reproduces the multi-node deployment: two servers share the same metadata store, and each
    // has its own local job executor. The server that didn't run the job must not mark it FAILED.
    File stagingRoot = Files.createTempDirectory("gravitino-test-multi-node-job").toFile();
    File jobStagingDir = new File(stagingRoot, "job");
    Assertions.assertTrue(jobStagingDir.mkdirs());
    Map<String, String> executorConfigs =
        ImmutableMap.of(LocalJobExecutorConfigs.STAGING_DIR, stagingRoot.getAbsolutePath());
    LocalJobExecutor ownerExecutor = new LocalJobExecutor();
    LocalJobExecutor otherExecutor = new LocalJobExecutor();
    ownerExecutor.initialize(executorConfigs);
    otherExecutor.initialize(executorConfigs);
    JobManager ownerManager =
        Mockito.spy(new JobManager(config, entityStore, idGenerator, ownerExecutor));
    JobManager otherManager =
        Mockito.spy(new JobManager(config, entityStore, idGenerator, otherExecutor));

    try {
      JobTemplate jobTemplate =
          JobManager.createRuntimeJobTemplate(
              newShellJobTemplateEntity("shell_job", "echo"),
              Collections.emptyMap(),
              jobStagingDir);
      String executionId = ownerExecutor.submitJob(jobTemplate);
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .until(() -> ownerExecutor.getJobStatus(executionId) == JobHandle.Status.SUCCEEDED);

      JobEntity job =
          newJobEntity(
              idGenerator.nextId(), executionId, JobHandle.Status.QUEUED, Instant.now(), null);
      mockedMetalake
          .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
          .thenReturn(ImmutableList.of(metalake));
      doReturn(ImmutableList.of(job)).when(otherManager).listJobs(metalake, Optional.empty());
      doReturn(ImmutableList.of(job)).when(ownerManager).listJobs(metalake, Optional.empty());
      stubEntityStoreUpdateToApply(job);

      otherManager.pullAndUpdateJobStatus();
      verify(entityStore, never())
          .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());

      ownerManager.pullAndUpdateJobStatus();
      Assertions.assertEquals(JobHandle.Status.SUCCEEDED, captureUpdatedJobEntity(job).status());
    } finally {
      ownerManager.close();
      otherManager.close();
      FileUtils.deleteDirectory(stagingRoot);
    }
  }

  @Test
  public void testCleanUpStagingDirsExpiresStaleActiveJobs() throws IOException {
    // Active jobs that have not been updated for the whole retention time are left behind, e.g. by
    // a server that exited while running them. They are marked as finished, and kept for another
    // retention time before being cleaned up.
    Instant longAgo = Instant.now().minus(30, ChronoUnit.DAYS);
    JobEntity queuedJob = newJobEntity("local-job-gone-1", JobHandle.Status.QUEUED, longAgo, null);
    JobEntity startedJob =
        newJobEntity("local-job-gone-2", JobHandle.Status.STARTED, longAgo, longAgo);
    JobEntity cancellingJob =
        newJobEntity("local-job-gone-3", JobHandle.Status.CANCELLING, longAgo, longAgo);
    JobEntity activeJob =
        newJobEntity("local-job-mine-1", JobHandle.Status.STARTED, longAgo, Instant.now());
    mockListActiveJobs(queuedJob, startedJob, cancellingJob, activeJob);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    for (JobEntity job : ImmutableList.of(queuedJob, startedJob, cancellingJob, activeJob)) {
      stubEntityStoreUpdateToApply(job, job);
    }
    File jobStagingDir = jobManager.jobStagingDir(startedJob.id());
    Assertions.assertTrue(jobStagingDir.mkdirs());

    long beforeCleanUp = System.currentTimeMillis();
    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    Assertions.assertEquals(
        JobHandle.Status.FAILED, captureUpdatedJobEntity(queuedJob, queuedJob).status());
    JobEntity expiredStartedJob = captureUpdatedJobEntity(startedJob, startedJob);
    Assertions.assertEquals(JobHandle.Status.FAILED, expiredStartedJob.status());
    Assertions.assertEquals(startedJob.startedAt(), expiredStartedJob.startedAt());
    // The expire time is used as the finished time, so the job is kept for another retention time.
    Assertions.assertTrue(expiredStartedJob.finishedAt() >= beforeCleanUp);
    Assertions.assertEquals(
        JobHandle.Status.CANCELLED, captureUpdatedJobEntity(cancellingJob, cancellingJob).status());
    verify(entityStore, never()).delete(any(), any());
    Assertions.assertTrue(jobStagingDir.exists());

    // A job updated recently is still active, so it's not expired.
    verify(entityStore, never())
        .update(
            eq(NameIdentifierUtil.ofJob(metalake, activeJob.name())),
            eq(JobEntity.class),
            eq(Entity.EntityType.JOB),
            any());
  }

  @Test
  public void testCleanUpStagingDirsDoesNotExpireJobOfNonNodeLocalExecutor() throws IOException {
    // Any server can track the jobs of an external job executor, so they are never considered left
    // behind, no matter how long their status has not changed.
    Instant longAgo = Instant.now().minus(30, ChronoUnit.DAYS);
    JobEntity job = newJobEntity("external-job-1", JobHandle.Status.STARTED, longAgo, longAgo);
    mockListActiveJobs(job);

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
    verify(entityStore, never()).delete(any(), any());
  }

  @Test
  public void testCleanUpStagingDirsDoesNotExpireJobUpdatedConcurrently() throws IOException {
    // listJobs() observes a stale job, but it is updated before entityStore.update() re-fetches
    // it, so it's still active and must be kept.
    Instant longAgo = Instant.now().minus(30, ChronoUnit.DAYS);
    JobEntity staleSnapshot =
        newJobEntity("local-job-other-1", JobHandle.Status.STARTED, longAgo, longAgo);
    JobEntity latestUpdated =
        newJobEntity(
            staleSnapshot.id(),
            staleSnapshot.jobExecutionId(),
            JobHandle.Status.STARTED,
            longAgo,
            Instant.now());
    mockListActiveJobs(staleSnapshot);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    stubEntityStoreUpdateToApply(latestUpdated);

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    Assertions.assertSame(latestUpdated, captureUpdatedJobEntity(latestUpdated));
    verify(entityStore, never()).delete(any(), any());
  }

  @Test
  public void testCleanUpStagingDirsContinuesAfterFailingToExpireJob() throws IOException {
    Instant longAgo = Instant.now().minus(30, ChronoUnit.DAYS);
    JobEntity failingJob =
        newJobEntity("local-job-gone-1", JobHandle.Status.STARTED, longAgo, longAgo);
    JobEntity otherJob =
        newJobEntity("local-job-gone-2", JobHandle.Status.STARTED, longAgo, longAgo);
    mockListActiveJobs(failingJob, otherJob);
    when(jobExecutor.isJobStateNodeLocal()).thenReturn(true);
    when(entityStore.update(
            eq(NameIdentifierUtil.ofJob(metalake, failingJob.name())),
            eq(JobEntity.class),
            eq(Entity.EntityType.JOB),
            any()))
        .thenThrow(new IOException("store error"));
    stubEntityStoreUpdateToApply(otherJob, otherJob);

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    Assertions.assertEquals(
        JobHandle.Status.FAILED, captureUpdatedJobEntity(otherJob, otherJob).status());
  }

  /** Conflicts preserve files without stopping this cleanup batch or its next scheduled run. */
  @Test
  public void testCleanUpStagingDirsContinuesAfterOccConflict() throws IOException {
    JobEntity conflicted = expiredJob();
    JobEntity other = expiredJob();
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));
    when(jobManager.listJobs(metalake, Optional.empty()))
        .thenReturn(ImmutableList.of(conflicted, other), ImmutableList.of(conflicted));
    NameIdentifier conflictedIdent = NameIdentifierUtil.ofJob(metalake, conflicted.name());
    NameIdentifier otherIdent = NameIdentifierUtil.ofJob(metalake, other.name());
    when(entityStore.delete(conflictedIdent, Entity.EntityType.JOB))
        .thenThrow(new OptimisticLockException("job changed"))
        .thenReturn(true);
    when(entityStore.delete(otherIdent, Entity.EntityType.JOB)).thenReturn(true);
    File conflictedDir = jobManager.jobStagingDir(conflicted.id());
    File otherDir = jobManager.jobStagingDir(other.id());
    Assertions.assertTrue(conflictedDir.mkdirs());
    Assertions.assertTrue(otherDir.mkdirs());
    File artifact = new File(conflictedDir, "artifact");
    Assertions.assertTrue(artifact.createNewFile());

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());
    Assertions.assertTrue(artifact.isFile());
    Assertions.assertFalse(otherDir.exists());
    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());
    Assertions.assertFalse(conflictedDir.exists());
    verify(entityStore, times(2)).delete(conflictedIdent, Entity.EntityType.JOB);
    verify(entityStore, times(1)).delete(otherIdent, Entity.EntityType.JOB);
  }

  @Test
  public void testCleanUpStagingDirsDeletesLegacyStagingDirOfRenamedTemplate() throws IOException {
    // The job ran from template "shell_job" before an upgrade, in the legacy layout, and the
    // template was renamed afterwards, so the job now reports the new name.
    JobEntity job = expiredJob();
    JobEntity renamedJob =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId(job.jobExecutionId())
            .withNamespace(job.namespace())
            .withJobTemplateName("renamed_shell_job")
            .withStartedAt(job.startedAt())
            .withFinishedAt(job.finishedAt())
            .withStatus(job.status())
            .withAuditInfo(job.auditInfo())
            .build();
    mockListActiveJobs(renamedJob);
    when(entityStore.delete(NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB))
        .thenReturn(true);
    File legacyDir = new File(testStagingDir, metalake + "/shell_job/" + job.name());
    Assertions.assertTrue(legacyDir.mkdirs());
    Assertions.assertTrue(new File(legacyDir, "output.log").createNewFile());
    File otherJobDir = new File(testStagingDir, metalake + "/shell_job/job-" + (job.id() + 1));
    Assertions.assertTrue(otherJobDir.mkdirs());

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    Assertions.assertFalse(legacyDir.exists());
    Assertions.assertTrue(otherJobDir.isDirectory());
  }

  @Test
  public void testDeleteJobTemplateDeletesLegacyStagingDirOfRenamedMetalake() throws IOException {
    // The job ran in the legacy layout under the metalake's former name.
    JobEntity job = expiredJob();
    doReturn(Collections.singletonList(job)).when(jobManager).listJobs(metalake, Optional.of("t"));
    doReturn(true)
        .when(entityStore)
        .delete(NameIdentifierUtil.ofJobTemplate(metalake, "t"), Entity.EntityType.JOB_TEMPLATE);
    File legacyDir = new File(testStagingDir, "old_metalake_name/t/" + job.name());
    Assertions.assertTrue(legacyDir.mkdirs());

    Assertions.assertTrue(jobManager.deleteJobTemplate(metalake, "t"));

    Assertions.assertFalse(legacyDir.exists());
    // Only the job's own directory is deleted, never the template or metalake directory.
    Assertions.assertTrue(legacyDir.getParentFile().isDirectory());
  }

  @Test
  public void testDeleteJobStagingDirPrefersCurrentLayout() throws IOException {
    JobEntity job = expiredJob();
    doReturn(Collections.singletonList(job)).when(jobManager).listJobs(metalake, Optional.of("t"));
    doReturn(true)
        .when(entityStore)
        .delete(NameIdentifierUtil.ofJobTemplate(metalake, "t"), Entity.EntityType.JOB_TEMPLATE);
    File jobStagingDir = jobManager.jobStagingDir(job.id());
    Assertions.assertTrue(jobStagingDir.mkdirs());

    Assertions.assertTrue(jobManager.deleteJobTemplate(metalake, "t"));

    Assertions.assertFalse(jobStagingDir.exists());
    verify(jobManager, never()).findLegacyJobStagingDirs(job.id());
  }

  @Test
  public void testFindLegacyJobStagingDirs() throws IOException {
    long jobId = idGenerator.nextId();
    String jobDirName = JobHandle.JOB_ID_PREFIX + jobId;
    File stagingDir = new File(testStagingDir).getAbsoluteFile();
    File legacyDir = new File(stagingDir, metalake + "/shell_job/" + jobDirName);
    Assertions.assertTrue(legacyDir.mkdirs());

    // Not the job's directory: a file of the same name, a directory at the wrong depth, a hidden
    // top-level directory and the directory of the current layout.
    File sameNameFile = new File(stagingDir, metalake + "/other_job/" + jobDirName);
    Assertions.assertTrue(sameNameFile.getParentFile().mkdirs());
    Assertions.assertTrue(sameNameFile.createNewFile());
    Assertions.assertTrue(new File(stagingDir, metalake + "/" + jobDirName).mkdirs());
    Assertions.assertTrue(new File(stagingDir, ".job-output-index/x/" + jobDirName).mkdirs());
    Assertions.assertTrue(jobManager.jobStagingDir(jobId).mkdirs());
    Assertions.assertTrue(
        new File(jobManager.jobStagingDir(jobId).getParentFile(), "x/" + jobDirName).mkdirs());
    Assertions.assertTrue(new File(stagingDir, "a_file").createNewFile());

    // Symbolic links are never followed, so nothing outside the staging directory is found.
    File outsideDir = Files.createTempDirectory("gravitino-test-outside-staging").toFile();
    try {
      Assertions.assertTrue(new File(outsideDir, "t/" + jobDirName).mkdirs());
      Files.createSymbolicLink(
          new File(stagingDir, "linked_metalake").toPath(), outsideDir.toPath());
      Assertions.assertTrue(new File(stagingDir, "metalake_2").mkdirs());
      Files.createSymbolicLink(
          new File(stagingDir, "metalake_2/linked_template").toPath(),
          new File(outsideDir, "t").toPath());
      File linkedJobTemplateDir = new File(stagingDir, metalake + "/linked_job");
      Assertions.assertTrue(linkedJobTemplateDir.mkdirs());
      Files.createSymbolicLink(
          new File(linkedJobTemplateDir, jobDirName).toPath(),
          new File(outsideDir, "t/" + jobDirName).toPath());
    } catch (IOException e) {
      FileUtils.deleteDirectory(outsideDir);
      throw e;
    }

    try {
      Assertions.assertEquals(
          ImmutableList.of(legacyDir.getAbsolutePath()),
          jobManager.findLegacyJobStagingDirs(jobId).stream()
              .map(File::getAbsolutePath)
              .collect(Collectors.toList()));
      Assertions.assertTrue(
          jobManager.findLegacyJobStagingDirs(idGenerator.nextId()).isEmpty(),
          "No directory of an unknown job");
    } finally {
      FileUtils.deleteDirectory(outsideDir);
    }
  }

  @Test
  public void testCleanUpStagingDirsDeletesLegacyStagingDirOfNestedTemplateName()
      throws IOException {
    // Template names may contain '/', which the legacy layout put in the path as is, so the job
    // directory is nested deeper. The template was never renamed: this is an upgraded job.
    JobEntity job = expiredJob();
    JobEntity nestedTemplateJob =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId(job.jobExecutionId())
            .withNamespace(job.namespace())
            .withJobTemplateName("team/etl")
            .withStartedAt(job.startedAt())
            .withFinishedAt(job.finishedAt())
            .withStatus(job.status())
            .withAuditInfo(job.auditInfo())
            .build();
    mockListActiveJobs(nestedTemplateJob);
    when(entityStore.delete(NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB))
        .thenReturn(true);
    File legacyDir = new File(testStagingDir, metalake + "/team/etl/" + job.name());
    Assertions.assertTrue(legacyDir.mkdirs());

    Assertions.assertDoesNotThrow(() -> jobManager.cleanUpStagingDirs());

    Assertions.assertFalse(legacyDir.exists());
  }

  @Test
  public void testFindLegacyJobStagingDirsOfNestedTemplateNames() throws IOException {
    long jobId = idGenerator.nextId();
    String jobDirName = JobHandle.JOB_ID_PREFIX + jobId;
    File stagingDir = new File(testStagingDir).getAbsoluteFile();
    File nestedDir = new File(stagingDir, metalake + "/team/etl/" + jobDirName);
    Assertions.assertTrue(nestedDir.mkdirs());
    File deeplyNestedDir = new File(stagingDir, metalake + "/a/b/c/d/" + jobDirName);
    Assertions.assertTrue(deeplyNestedDir.mkdirs());

    // The staging directory of another job is not descended into: a directory of its own files
    // named like this job is not this job's staging directory.
    File otherJobDir =
        new File(
            stagingDir,
            metalake
                + "/shell_job/"
                + JobHandle.JOB_ID_PREFIX
                + (jobId + 1)
                + File.separator
                + jobDirName);
    Assertions.assertTrue(otherJobDir.mkdirs());

    // A template name nested deeper than the lookup goes keeps its directory.
    StringBuilder tooDeepPath = new StringBuilder(metalake);
    for (int i = 0; i < 20; i++) {
      tooDeepPath.append(File.separator).append("d").append(i);
    }
    File tooDeepDir = new File(stagingDir, tooDeepPath + File.separator + jobDirName);
    Assertions.assertTrue(tooDeepDir.mkdirs());

    Assertions.assertEquals(
        ImmutableSet.of(nestedDir.getAbsolutePath(), deeplyNestedDir.getAbsolutePath()),
        jobManager.findLegacyJobStagingDirs(jobId).stream()
            .map(File::getAbsolutePath)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testFindLegacyJobStagingDirsSkipsUnreadableDirectory() throws IOException {
    long jobId = idGenerator.nextId();
    File stagingDir = new File(testStagingDir);
    File legacyDir =
        new File(stagingDir, metalake + "/shell_job/" + JobHandle.JOB_ID_PREFIX + jobId);
    Assertions.assertTrue(legacyDir.mkdirs());
    // E.g. "lost+found" when the staging directory is the root of a file system.
    File unreadableDir = new File(stagingDir, "lost+found");
    Assertions.assertTrue(unreadableDir.mkdirs());
    Assertions.assertTrue(unreadableDir.setReadable(false, false));

    try {
      Assertions.assertEquals(
          ImmutableList.of(legacyDir.getAbsolutePath()),
          jobManager.findLegacyJobStagingDirs(jobId).stream()
              .map(File::getAbsolutePath)
              .collect(Collectors.toList()));
    } finally {
      Assertions.assertTrue(unreadableDir.setReadable(true, false));
    }
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

    // Mock MetalakeManager.listInUseMetalakes to return the test metalake
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

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

  @Test
  public void testUpdateShellJobTemplateEntity() {
    String jobTemplateName = "old_shell_job";
    String jobTemplateComment = "An old shell job template";
    JobTemplateEntity oldJobTemplateEntity =
        newShellJobTemplateEntity(jobTemplateName, jobTemplateComment);

    // Update name and comment
    String newJobTemplateName = "new_shell_job";
    String newJobTemplateComment = "A new shell job template";
    JobTemplateChange rename = JobTemplateChange.rename(newJobTemplateName);
    JobTemplateChange updateComment = JobTemplateChange.updateComment(newJobTemplateComment);

    JobTemplateEntity newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, rename, updateComment);

    Assertions.assertEquals(oldJobTemplateEntity.id(), newJobTemplateEntity.id());
    Assertions.assertEquals(newJobTemplateName, newJobTemplateEntity.name());
    Assertions.assertEquals(oldJobTemplateEntity.namespace(), newJobTemplateEntity.namespace());
    Assertions.assertEquals(newJobTemplateComment, newJobTemplateEntity.comment());
    Assertions.assertEquals(
        oldJobTemplateEntity.templateContent(), newJobTemplateEntity.templateContent());

    // Update the executable of the shell job template
    JobTemplateChange updateShellTemplate =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.ShellTemplateUpdate.builder().withNewExecutable("/bin/ls").build());

    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateShellTemplate);
    Assertions.assertEquals(oldJobTemplateEntity.id(), newJobTemplateEntity.id());
    Assertions.assertEquals(oldJobTemplateEntity.name(), newJobTemplateEntity.name());
    Assertions.assertEquals(oldJobTemplateEntity.namespace(), newJobTemplateEntity.namespace());
    Assertions.assertEquals(oldJobTemplateEntity.comment(), newJobTemplateEntity.comment());
    Assertions.assertNotEquals(
        oldJobTemplateEntity.templateContent(), newJobTemplateEntity.templateContent());
    JobTemplateEntity.TemplateContent oldContent = oldJobTemplateEntity.templateContent();
    JobTemplateEntity.TemplateContent newContent = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent.jobType());
    Assertions.assertEquals("/bin/ls", newContent.executable());
    Assertions.assertEquals(oldContent.arguments(), newContent.arguments());
    Assertions.assertEquals(oldContent.environments(), newContent.environments());
    Assertions.assertEquals(oldContent.customFields(), newContent.customFields());

    // Update the arguments, environments, custom fields of the shell job template
    JobTemplateChange updateShellTemplate2 =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.ShellTemplateUpdate.builder()
                .withNewArguments(ImmutableList.of("arg1", "arg2"))
                .withNewEnvironments(Collections.singletonMap("env1", "value1"))
                .withNewCustomFields(Collections.singletonMap("field1", "value1"))
                .build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateShellTemplate2);

    JobTemplateEntity.TemplateContent newContent2 = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent2.jobType());
    Assertions.assertEquals(oldContent.executable(), newContent2.executable());
    Assertions.assertEquals(ImmutableList.of("arg1", "arg2"), newContent2.arguments());
    Assertions.assertEquals(Collections.singletonMap("env1", "value1"), newContent2.environments());
    Assertions.assertEquals(
        Collections.singletonMap("field1", "value1"), newContent2.customFields());
    Assertions.assertEquals(oldContent.scripts(), newContent2.scripts());

    // Update the scripts of the shell job template
    JobTemplateChange updateShellTemplate3 =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.ShellTemplateUpdate.builder()
                .withNewScripts(ImmutableList.of("echo Hello", "echo World"))
                .build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateShellTemplate3);

    JobTemplateEntity.TemplateContent newContent3 = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent3.jobType());
    Assertions.assertEquals(oldContent.executable(), newContent3.executable());
    Assertions.assertEquals(oldContent.arguments(), newContent3.arguments());
    Assertions.assertEquals(oldContent.environments(), newContent3.environments());
    Assertions.assertEquals(oldContent.customFields(), newContent3.customFields());
    Assertions.assertEquals(ImmutableList.of("echo Hello", "echo World"), newContent3.scripts());

    // Update with no changes
    JobTemplateChange noChange =
        JobTemplateChange.updateTemplate(JobTemplateChange.ShellTemplateUpdate.builder().build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, noChange);
    Assertions.assertEquals(
        oldJobTemplateEntity.templateContent(), newJobTemplateEntity.templateContent());

    // Update job template with SparkJobTemplateChange should throw IllegalArgumentException
    JobTemplateChange invalidChange =
        JobTemplateChange.updateTemplate(JobTemplateChange.SparkTemplateUpdate.builder().build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            jobManager.updateJobTemplateEntity(
                oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, invalidChange));
  }

  @Test
  public void testUpdateSparkJobTemplateEntity() {
    String jobTemplateName = "old_spark_job";
    String jobTemplateComment = "An old spark job template";
    JobTemplateEntity oldJobTemplateEntity =
        newSparkJobTemplateEntity(jobTemplateName, jobTemplateComment);

    // Update the executable and class name of the spark job template
    JobTemplateChange updateSparkTemplate =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.SparkTemplateUpdate.builder()
                .withNewExecutable("file:/new/path/to/spark-examples.jar")
                .withNewClassName("org.apache.spark.examples.SparkWordCount")
                .build());
    JobTemplateEntity newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateSparkTemplate);
    Assertions.assertEquals(oldJobTemplateEntity.id(), newJobTemplateEntity.id());
    Assertions.assertEquals(oldJobTemplateEntity.name(), newJobTemplateEntity.name());
    Assertions.assertEquals(oldJobTemplateEntity.namespace(), newJobTemplateEntity.namespace());
    Assertions.assertEquals(oldJobTemplateEntity.comment(), newJobTemplateEntity.comment());
    Assertions.assertNotEquals(
        oldJobTemplateEntity.templateContent(), newJobTemplateEntity.templateContent());
    JobTemplateEntity.TemplateContent oldContent = oldJobTemplateEntity.templateContent();
    JobTemplateEntity.TemplateContent newContent = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent.jobType());
    Assertions.assertEquals("file:/new/path/to/spark-examples.jar", newContent.executable());
    Assertions.assertEquals("org.apache.spark.examples.SparkWordCount", newContent.className());
    Assertions.assertEquals(oldContent.arguments(), newContent.arguments());
    Assertions.assertEquals(oldContent.environments(), newContent.environments());
    Assertions.assertEquals(oldContent.customFields(), newContent.customFields());
    Assertions.assertEquals(oldContent.jars(), newContent.jars());
    Assertions.assertEquals(oldContent.files(), newContent.files());
    Assertions.assertEquals(oldContent.archives(), newContent.archives());
    Assertions.assertEquals(oldContent.configs(), newContent.configs());

    // Update the arguments, environments, custom fields of the spark job template
    JobTemplateChange updateSparkTemplate2 =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.SparkTemplateUpdate.builder()
                .withNewArguments(ImmutableList.of("arg1", "arg2"))
                .withNewEnvironments(Collections.singletonMap("env1", "value1"))
                .withNewCustomFields(Collections.singletonMap("field1", "value1"))
                .build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateSparkTemplate2);
    JobTemplateEntity.TemplateContent newContent2 = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent2.jobType());
    Assertions.assertEquals(oldContent.executable(), newContent2.executable());
    Assertions.assertEquals(oldContent.className(), newContent2.className());
    Assertions.assertEquals(ImmutableList.of("arg1", "arg2"), newContent2.arguments());
    Assertions.assertEquals(Collections.singletonMap("env1", "value1"), newContent2.environments());
    Assertions.assertEquals(
        Collections.singletonMap("field1", "value1"), newContent2.customFields());
    Assertions.assertEquals(oldContent.jars(), newContent2.jars());
    Assertions.assertEquals(oldContent.files(), newContent2.files());
    Assertions.assertEquals(oldContent.archives(), newContent2.archives());
    Assertions.assertEquals(oldContent.configs(), newContent2.configs());

    // Update the jars, files, archives, configs of the spark job template
    JobTemplateChange updateSparkTemplate3 =
        JobTemplateChange.updateTemplate(
            JobTemplateChange.SparkTemplateUpdate.builder()
                .withNewJars(ImmutableList.of("file:/new/path/to/jar1 ", "file:/new/path/to/jar2"))
                .withNewFiles(
                    ImmutableList.of("file:/new/path/to/file1", "file:/new/path/to/file2"))
                .withNewArchives(
                    ImmutableList.of("file:/new/path/to/archive1", "file:/new/path/to/archive2"))
                .withNewConfigs(Collections.singletonMap("spark.executor.memory", "4g"))
                .build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, updateSparkTemplate3);
    JobTemplateEntity.TemplateContent newContent3 = newJobTemplateEntity.templateContent();
    Assertions.assertEquals(oldContent.jobType(), newContent3.jobType());
    Assertions.assertEquals(oldContent.executable(), newContent3.executable());
    Assertions.assertEquals(oldContent.className(), newContent3.className());
    Assertions.assertEquals(oldContent.arguments(), newContent3.arguments());
    Assertions.assertEquals(oldContent.environments(), newContent3.environments());
    Assertions.assertEquals(oldContent.customFields(), newContent3.customFields());
    Assertions.assertEquals(
        ImmutableList.of("file:/new/path/to/jar1 ", "file:/new/path/to/jar2"), newContent3.jars());
    Assertions.assertEquals(
        ImmutableList.of("file:/new/path/to/file1", "file:/new/path/to/file2"),
        newContent3.files());
    Assertions.assertEquals(
        ImmutableList.of("file:/new/path/to/archive1", "file:/new/path/to/archive2"),
        newContent3.archives());
    Assertions.assertEquals(
        Collections.singletonMap("spark.executor.memory", "4g"), newContent3.configs());

    // Update with no changes
    JobTemplateChange noChange =
        JobTemplateChange.updateTemplate(JobTemplateChange.SparkTemplateUpdate.builder().build());
    newJobTemplateEntity =
        jobManager.updateJobTemplateEntity(
            oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, noChange);
    Assertions.assertEquals(
        oldJobTemplateEntity.templateContent(), newJobTemplateEntity.templateContent());

    // Update job template with ShellJobTemplateChange should throw IllegalArgumentException
    JobTemplateChange invalidChange =
        JobTemplateChange.updateTemplate(JobTemplateChange.ShellTemplateUpdate.builder().build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            jobManager.updateJobTemplateEntity(
                oldJobTemplateEntity.nameIdentifier(), oldJobTemplateEntity, invalidChange));
  }

  private void assertStatusPollingContinues(RuntimeException failure) throws IOException {
    JobEntity conflictedJob = newJobEntity("shell_job", JobHandle.Status.QUEUED);
    JobEntity survivingJob = newJobEntity("shell_job", JobHandle.Status.QUEUED);

    BaseMetalake mockMetalake =
        BaseMetalake.builder()
            .withName(metalake)
            .withId(idGenerator.nextId())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    when(entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE))
        .thenReturn(ImmutableList.of(mockMetalake));
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));

    when(jobManager.listJobs(metalake, Optional.empty()))
        .thenReturn(ImmutableList.of(conflictedJob, survivingJob));
    when(jobExecutor.getJobExecutionInfo(conflictedJob.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));
    when(jobExecutor.getJobExecutionInfo(survivingJob.jobExecutionId()))
        .thenReturn(JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));

    // A losing CAS must not stop this batch or future scheduled polls.
    NameIdentifier conflictedJobIdent = NameIdentifierUtil.ofJob(metalake, conflictedJob.name());
    NameIdentifier survivingJobIdent = NameIdentifierUtil.ofJob(metalake, survivingJob.name());
    when(entityStore.update(
            eq(conflictedJobIdent), eq(JobEntity.class), eq(Entity.EntityType.JOB), any()))
        .thenThrow(failure);
    when(entityStore.update(
            eq(survivingJobIdent), eq(JobEntity.class), eq(Entity.EntityType.JOB), any()))
        .thenAnswer(
            invocation -> {
              Function<JobEntity, JobEntity> updater = invocation.getArgument(3);
              return updater.apply(survivingJob);
            });

    // Both polls process the other job even when this job keeps conflicting.
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(entityStore, times(2))
        .update(eq(conflictedJobIdent), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
    verify(entityStore, times(2))
        .update(eq(survivingJobIdent), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
  }

  private JobTemplateEntity newShellJobTemplateEntity(String name, String comment) {
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

  private JobTemplateEntity newSparkJobTemplateEntity(String name, String comment) {
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

  private JobEntity expiredJob() {
    long id = idGenerator.nextId();
    return JobEntity.builder()
        .withId(id)
        .withJobExecutionId(Long.toString(id))
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName("shell_job")
        .withStartedAt(1L)
        .withFinishedAt(2L)
        .withStatus(JobHandle.Status.SUCCEEDED)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Test
  public void testPullJobStatusUsesExecutorReportedTimestamps() throws IOException {
    // The job starts and finishes between two polls, so it is never observed as STARTED. The
    // times reported by the job executor are recorded instead of the poll time.
    Instant queuedAt = Instant.now().minusSeconds(60);
    JobEntity job = newJobEntity("job-execution-1", JobHandle.Status.QUEUED, queuedAt, null);
    mockListActiveJobs(job);
    Instant startedAt = queuedAt.plusSeconds(1);
    Instant finishedAt = queuedAt.plusSeconds(2);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.SUCCEEDED)
                .withStartedAt(startedAt)
                .withFinishedAt(finishedAt)
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(job.jobExecutionId());
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity updatedJob = captureUpdatedJobEntity(job);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, updatedJob.status());
    Assertions.assertEquals(startedAt.toEpochMilli(), updatedJob.startedAt());
    Assertions.assertEquals(finishedAt.toEpochMilli(), updatedJob.finishedAt());
  }

  @Test
  public void testPullJobStatusReportedStartedAtReplacesPolledTime() throws IOException {
    // An earlier poll recorded its own time as the started time, as the job executor didn't report
    // one at that time. The time reported later replaces it.
    Instant queuedAt = Instant.now().minusSeconds(60);
    JobEntity job =
        JobEntity.builder()
            .withId(idGenerator.nextId())
            .withJobExecutionId("job-execution-1")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.STARTED)
            .withStartedAt(queuedAt.plusSeconds(30).toEpochMilli())
            .withFinishedAt(0L)
            .withAuditInfo(AuditInfo.builder().withCreator("test").withCreateTime(queuedAt).build())
            .build();
    mockListActiveJobs(job);
    Instant startedAt = queuedAt.plusSeconds(1);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.STARTED)
                .withStartedAt(startedAt)
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(job.jobExecutionId());
    stubEntityStoreUpdateToApply(job);

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    // The job is updated even though its status doesn't change.
    JobEntity updatedJob = captureUpdatedJobEntity(job);
    Assertions.assertEquals(JobHandle.Status.STARTED, updatedJob.status());
    Assertions.assertEquals(startedAt.toEpochMilli(), updatedJob.startedAt());
    Assertions.assertEquals(0L, updatedJob.finishedAt());
  }

  @Test
  public void testPullJobStatusSkipsUpdateWhenNothingChanges() throws IOException {
    Instant queuedAt = Instant.now().minusSeconds(60);
    Instant startedAt = queuedAt.plusSeconds(1);
    JobEntity job =
        JobEntity.builder()
            .withId(idGenerator.nextId())
            .withJobExecutionId("job-execution-1")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_job")
            .withStatus(JobHandle.Status.STARTED)
            .withStartedAt(startedAt.toEpochMilli())
            .withFinishedAt(0L)
            .withAuditInfo(AuditInfo.builder().withCreator("test").withCreateTime(queuedAt).build())
            .build();
    mockListActiveJobs(job);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.STARTED)
                .withStartedAt(startedAt)
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(job.jobExecutionId());

    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());
  }

  @Test
  public void testPullJobStatusIgnoresInconsistentReportedTimestamps() throws IOException {
    Instant queuedAt = Instant.now().minusSeconds(60);

    // A queued job has no started time, so a reported one is ignored and nothing changes.
    JobEntity queuedJob = newJobEntity("job-execution-1", JobHandle.Status.QUEUED, queuedAt, null);
    mockListActiveJobs(queuedJob);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.QUEUED)
                .withStartedAt(queuedAt.plusSeconds(1))
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(queuedJob.jobExecutionId());
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());
    verify(entityStore, never())
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any());

    // A running job has no finished time, so a reported one is ignored.
    JobEntity startingJob =
        newJobEntity("job-execution-2", JobHandle.Status.QUEUED, queuedAt, null);
    mockListActiveJobs(startingJob);
    Instant startedAt = queuedAt.plusSeconds(1);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.STARTED)
                .withStartedAt(startedAt)
                .withFinishedAt(queuedAt.plusSeconds(2))
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(startingJob.jobExecutionId());
    stubEntityStoreUpdateToApply(startingJob);
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity startedJob = captureUpdatedJobEntity(startingJob);
    Assertions.assertEquals(JobHandle.Status.STARTED, startedJob.status());
    Assertions.assertEquals(startedAt.toEpochMilli(), startedJob.startedAt());
    Assertions.assertEquals(0L, startedJob.finishedAt());
  }

  @Test
  public void testPullJobStatusCorrectsReportedTimestamps() throws IOException {
    Instant queuedAt = Instant.now().minusSeconds(60);

    // The clock of the job runner is behind, so the job is reported to start and finish before it
    // was queued. Both times are raised to the queued time.
    JobEntity skewedJob = newJobEntity("job-execution-1", JobHandle.Status.QUEUED, queuedAt, null);
    mockListActiveJobs(skewedJob);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.SUCCEEDED)
                .withStartedAt(queuedAt.minusSeconds(2))
                .withFinishedAt(queuedAt.minusSeconds(1))
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(skewedJob.jobExecutionId());
    stubEntityStoreUpdateToApply(skewedJob);
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity correctedJob = captureUpdatedJobEntity(skewedJob);
    Assertions.assertEquals(queuedAt.toEpochMilli(), correctedJob.startedAt());
    Assertions.assertEquals(queuedAt.toEpochMilli(), correctedJob.finishedAt());

    // The job is reported to finish before it started. The started time is dropped, while the
    // finished time is kept for the cleanup of the finished job.
    Mockito.clearInvocations(entityStore);
    JobEntity invalidJob = newJobEntity("job-execution-2", JobHandle.Status.QUEUED, queuedAt, null);
    mockListActiveJobs(invalidJob);
    Instant finishedAt = queuedAt.plusSeconds(1);
    doReturn(
            JobExecutionInfo.builder()
                .withStatus(JobHandle.Status.FAILED)
                .withStartedAt(queuedAt.plusSeconds(2))
                .withFinishedAt(finishedAt)
                .build())
        .when(jobExecutor)
        .getJobExecutionInfo(invalidJob.jobExecutionId());
    stubEntityStoreUpdateToApply(invalidJob);
    Assertions.assertDoesNotThrow(() -> jobManager.pullAndUpdateJobStatus());

    JobEntity failedJob = captureUpdatedJobEntity(invalidJob);
    Assertions.assertEquals(JobHandle.Status.FAILED, failedJob.status());
    Assertions.assertEquals(0L, failedJob.startedAt());
    Assertions.assertEquals(finishedAt.toEpochMilli(), failedJob.finishedAt());
  }

  private JobEntity newJobEntity(String templateName, JobHandle.Status status) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withJobExecutionId(rand.nextLong() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStartedAt(System.currentTimeMillis())
        .withFinishedAt(System.currentTimeMillis())
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private void mockListActiveJobs(JobEntity... jobs) {
    mockedMetalake
        .when(() -> MetalakeManager.listInUseMetalakes(entityStore))
        .thenReturn(ImmutableList.of(metalake));
    doReturn(ImmutableList.copyOf(jobs)).when(jobManager).listJobs(metalake, Optional.empty());
  }

  private JobEntity newJobEntity(
      String executionId,
      JobHandle.Status status,
      Instant createTime,
      @Nullable Instant lastModifiedTime) {
    return newJobEntity(idGenerator.nextId(), executionId, status, createTime, lastModifiedTime);
  }

  private JobEntity newJobEntity(
      long id,
      String executionId,
      JobHandle.Status status,
      Instant createTime,
      @Nullable Instant lastModifiedTime) {
    return JobEntity.builder()
        .withId(id)
        .withJobExecutionId(executionId)
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName("shell_job")
        .withStatus(status)
        .withStartedAt(0L)
        .withFinishedAt(0L)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator("test")
                .withCreateTime(createTime)
                .withLastModifier(lastModifiedTime == null ? null : "modifier")
                .withLastModifiedTime(lastModifiedTime)
                .build())
        .build();
  }

  // cancelJob/pullAndUpdateJobStatus now go through entityStore.update(), which re-fetches the
  // latest entity and applies an updater function internally. Since entityStore is a full mock,
  // this stubs that re-fetch to hand back the given entity, mirroring what the real
  // JobMetaService.updateJob would read from storage.
  @SuppressWarnings("unchecked")
  private void stubEntityStoreUpdateToApply(JobEntity latestJobEntity) throws IOException {
    when(entityStore.update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), any()))
        .thenAnswer(
            invocation -> {
              Function<JobEntity, JobEntity> updater = invocation.getArgument(3);
              return updater.apply(latestJobEntity);
            });
  }

  @SuppressWarnings("unchecked")
  private JobEntity captureUpdatedJobEntity(JobEntity latestJobEntity) throws IOException {
    ArgumentCaptor<Function<JobEntity, JobEntity>> captor = ArgumentCaptor.forClass(Function.class);
    verify(entityStore, times(1))
        .update(any(), eq(JobEntity.class), eq(Entity.EntityType.JOB), captor.capture());
    return captor.getValue().apply(latestJobEntity);
  }

  @SuppressWarnings("unchecked")
  private void stubEntityStoreUpdateToApply(JobEntity job, JobEntity latestJobEntity)
      throws IOException {
    when(entityStore.update(
            eq(NameIdentifierUtil.ofJob(metalake, job.name())),
            eq(JobEntity.class),
            eq(Entity.EntityType.JOB),
            any()))
        .thenAnswer(
            invocation -> {
              Function<JobEntity, JobEntity> updater = invocation.getArgument(3);
              return updater.apply(latestJobEntity);
            });
  }

  @SuppressWarnings("unchecked")
  private JobEntity captureUpdatedJobEntity(JobEntity job, JobEntity latestJobEntity)
      throws IOException {
    ArgumentCaptor<Function<JobEntity, JobEntity>> captor = ArgumentCaptor.forClass(Function.class);
    verify(entityStore, times(1))
        .update(
            eq(NameIdentifierUtil.ofJob(metalake, job.name())),
            eq(JobEntity.class),
            eq(Entity.EntityType.JOB),
            captor.capture());
    return captor.getValue().apply(latestJobEntity);
  }

  private HttpServer createLoopbackHttpServer(String response) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/artifact.jar",
        exchange -> {
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
          }
        });
    return server;
  }

  @Test
  public void testFetchFileFromUriWithMissingLocalFileShouldFail() throws IOException {
    File stagingDir = new File(testStagingDir);
    Assertions.assertTrue(stagingDir.mkdirs() || stagingDir.exists());

    Path missingFilePath =
        Path.of(System.getProperty("java.io.tmpdir"), "missing-job-file-" + UUID.randomUUID());
    String uri = missingFilePath.toUri().toString();

    Assertions.assertThrows(
        RuntimeException.class, () -> JobManager.fetchFileFromUri(uri, stagingDir, 1000));
  }

  @Test
  public void testFetchFileFromUriSsrfBlocked() {
    File stagingDir = new File(testStagingDir);
    Assertions.assertTrue(stagingDir.mkdirs() || stagingDir.exists());
    FileFetcher.get().initialize(true);

    // Loopback address
    RuntimeException e1 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobManager.fetchFileFromUri("http://127.0.0.1:8090/configs", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e1);

    // AWS / GCP / Azure cloud-metadata endpoint (link-local 169.254.x.x)
    RuntimeException e2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JobManager.fetchFileFromUri(
                    "http://169.254.169.254/latest/meta-data/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e2);

    // RFC-1918 private range
    RuntimeException e3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobManager.fetchFileFromUri("http://192.168.1.1/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e3);

    // Alibaba Cloud / Oracle Cloud metadata endpoint
    RuntimeException e4 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobManager.fetchFileFromUri("http://100.100.100.200/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e4);
  }

  @Test
  public void testFetchFileFromUriShouldAllowLocalhostWhenBlockingDisabled() throws Exception {
    File stagingDir = new File(testStagingDir);
    Assertions.assertTrue(stagingDir.mkdirs() || stagingDir.exists());
    HttpServer server = createLoopbackHttpServer("job artifact");

    try {
      server.start();
      int port = server.getAddress().getPort();
      FileFetcher.get().initialize(false);

      String fetchedFile =
          JobManager.fetchFileFromUri(
              String.format("http://127.0.0.1:%d/artifact.jar", port), stagingDir, 1000);

      Assertions.assertEquals("job artifact", Files.readString(Path.of(fetchedFile)));
    } finally {
      FileFetcher.get().initialize(true);
      server.stop(0);
    }
  }

  private static void assertRemoteUriBlockedMessage(RuntimeException exception) {
    Assertions.assertTrue(exception.getCause().getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(
        exception.getCause().getMessage().contains(FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG));
  }

  @Test
  public void testCloseShouldShutdownExecutorsWhenJobExecutorCloseFails() throws IOException {
    JobExecutor failingJobExecutor = Mockito.mock(JobExecutor.class);
    doThrow(new IOException("close failed")).when(failingJobExecutor).close();

    JobManager manager = new JobManager(config, entityStore, idGenerator, failingJobExecutor);
    try {
      Assertions.assertThrows(IOException.class, manager::close);
      Assertions.assertTrue(manager.statusPullExecutor.isShutdown());
      Assertions.assertTrue(manager.cleanUpExecutor.isShutdown());
    } finally {
      manager.statusPullExecutor.shutdownNow();
      manager.cleanUpExecutor.shutdownNow();
    }
  }
}
