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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
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

  @BeforeAll
  public static void setUp() throws IllegalAccessException {
    config = new Config(false) {};
    Random rand = new Random();
    testStagingDir = "test_staging_dir_" + rand.nextInt(1000);
    config.set(Configs.JOB_STAGING_DIR, testStagingDir);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    entityStore = Mockito.mock(EntityStore.class);

    JobManager jm = new JobManager(config, entityStore);
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
  }

  @Test
  public void testListJobs() throws IOException {
    mockedMetalake
        .when(() -> MetalakeManager.checkMetalake(metalakeIdent, entityStore))
        .thenAnswer(a -> null);

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_job", "A shell job template");
    when(jobManager.getJobTemplate(metalake, shellJobTemplate.name())).thenReturn(shellJobTemplate);

    JobEntity job1 = newJobEntity("shell_job");
    JobEntity job2 = newJobEntity("spark_job");

    SupportsRelationOperations supportsRelationOperations =
        Mockito.mock(SupportsRelationOperations.class);
    when(supportsRelationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.JOB_TEMPLATE_JOB_REL,
            NameIdentifierUtil.ofJobTemplate(metalake, shellJobTemplate.name()),
            Entity.EntityType.JOB_TEMPLATE))
        .thenReturn(Lists.newArrayList(job1));
    when(entityStore.relationOperations()).thenReturn(supportsRelationOperations);

    // Mock the listJobs method to return a list of jobs associated with the job template
    when(entityStore.list(NamespaceUtil.ofJob(metalake), JobEntity.class, Entity.EntityType.JOB))
        .thenReturn(Lists.newArrayList(job1, job2));

    List<JobEntity> jobs = jobManager.listJobs(metalake, Optional.of(shellJobTemplate.name()));
    Assertions.assertEquals(1, jobs.size());
    Assertions.assertTrue(jobs.contains(job1));
    Assertions.assertFalse(jobs.contains(job2));

    // List all jobs without filtering by job template
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

    JobEntity job = newJobEntity("shell_job");
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

  private static JobTemplateEntity newShellJobTemplateEntity(String name, String comment) {
    ShellJobTemplate shellJobTemplate =
        new ShellJobTemplate.Builder()
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
        new SparkJobTemplate.Builder()
            .withName(name)
            .withComment(comment)
            .withClassName("org.apache.spark.examples.SparkPi")
            .withExecutable("local:///path/to/spark-examples.jar")
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

  private static JobEntity newJobEntity(String templateName) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStatus(JobHandle.Status.QUEUED)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
