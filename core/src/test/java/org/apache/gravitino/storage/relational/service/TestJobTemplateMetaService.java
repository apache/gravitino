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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_job_template_test";

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @Test
  public void testInsertAndListJobTemplates() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateMetaService jobTemplateMetaService = JobTemplateMetaService.getInstance();
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            jobTemplateMetaService.getJobTemplateByIdentifier(
                NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "non_existent_template")));

    JobTemplateEntity testJobTemplateEntity1 =
        newShellJobTemplateEntity(
            "test_shell_template_1", "This is a test shell job template 1", METALAKE_NAME);

    Assertions.assertDoesNotThrow(
        () -> jobTemplateMetaService.insertJobTemplate(testJobTemplateEntity1, false));

    JobTemplateEntity testJobTemplateEntity2 =
        newSparkJobTemplateEntity(
            "test_spark_template_1", "This is a test spark job template 1", METALAKE_NAME);

    Assertions.assertDoesNotThrow(
        () -> jobTemplateMetaService.insertJobTemplate(testJobTemplateEntity2, false));

    List<JobTemplateEntity> jobTemplates =
        jobTemplateMetaService.listJobTemplatesByNamespace(
            NamespaceUtil.ofJobTemplate(METALAKE_NAME));

    Assertions.assertEquals(2, jobTemplates.size());
    Assertions.assertTrue(jobTemplates.contains(testJobTemplateEntity1));
    Assertions.assertTrue(jobTemplates.contains(testJobTemplateEntity2));

    // Test insert duplicate job template without overwrite
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> jobTemplateMetaService.insertJobTemplate(testJobTemplateEntity1, false));

    // Test insert duplicate job template with overwrite
    JobTemplateEntity updatedJobTemplateEntity1 =
        JobTemplateEntity.builder()
            .withName(testJobTemplateEntity1.name())
            .withComment("Updated comment for test shell job template 1")
            .withId(testJobTemplateEntity1.id())
            .withNamespace(testJobTemplateEntity1.namespace())
            .withTemplateContent(testJobTemplateEntity1.templateContent())
            .withAuditInfo(testJobTemplateEntity1.auditInfo())
            .build();
    Assertions.assertDoesNotThrow(
        () -> jobTemplateMetaService.insertJobTemplate(updatedJobTemplateEntity1, true));
    JobTemplateEntity fetchedJobTemplateEntity1 =
        jobTemplateMetaService.getJobTemplateByIdentifier(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, testJobTemplateEntity1.name()));
    Assertions.assertEquals(updatedJobTemplateEntity1, fetchedJobTemplateEntity1);

    jobTemplates =
        jobTemplateMetaService.listJobTemplatesByNamespace(
            NamespaceUtil.ofJobTemplate(METALAKE_NAME));
    Assertions.assertEquals(2, jobTemplates.size());
    Assertions.assertTrue(jobTemplates.contains(updatedJobTemplateEntity1));
    Assertions.assertTrue(jobTemplates.contains(testJobTemplateEntity2));
    Assertions.assertFalse(jobTemplates.contains(testJobTemplateEntity1));
  }

  @Test
  public void testInsertAndSelectJobTemplate() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateMetaService jobTemplateMetaService = JobTemplateMetaService.getInstance();

    JobTemplateEntity shellJobTemplate =
        newShellJobTemplateEntity("shell_template", "A shell job template", METALAKE_NAME);
    jobTemplateMetaService.insertJobTemplate(shellJobTemplate, false);

    JobTemplateEntity sparkJobTemplate =
        newSparkJobTemplateEntity("spark_template", "A spark job template", METALAKE_NAME);
    jobTemplateMetaService.insertJobTemplate(sparkJobTemplate, false);

    // Test select by identifier
    JobTemplateEntity fetchedShellJobTemplate =
        jobTemplateMetaService.getJobTemplateByIdentifier(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "shell_template"));
    Assertions.assertEquals(shellJobTemplate, fetchedShellJobTemplate);

    JobTemplateEntity fetchedSparkJobTemplate =
        jobTemplateMetaService.getJobTemplateByIdentifier(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "spark_template"));
    Assertions.assertEquals(sparkJobTemplate, fetchedSparkJobTemplate);

    // Test select non-existent template
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            jobTemplateMetaService.getJobTemplateByIdentifier(
                NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "non_existent_template")));
  }

  @Test
  public void testInsertAndDeleteJobTemplate() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateMetaService jobTemplateMetaService = JobTemplateMetaService.getInstance();

    JobTemplateEntity jobTemplateEntity =
        newShellJobTemplateEntity(
            "job_template_to_delete", "A job template to delete", METALAKE_NAME);
    jobTemplateMetaService.insertJobTemplate(jobTemplateEntity, false);

    // Verify insertion
    JobTemplateEntity fetchedJobTemplate =
        jobTemplateMetaService.getJobTemplateByIdentifier(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "job_template_to_delete"));
    Assertions.assertEquals(jobTemplateEntity, fetchedJobTemplate);

    // Delete the job template
    boolean deleted =
        jobTemplateMetaService.deleteJobTemplate(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "job_template_to_delete"));
    Assertions.assertTrue(deleted);

    deleted =
        jobTemplateMetaService.deleteJobTemplate(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "job_template_to_delete"));
    Assertions.assertFalse(deleted);
  }

  @Test
  public void testDeleteJobTemplateWithJobs() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateMetaService jobTemplateMetaService = JobTemplateMetaService.getInstance();

    JobTemplateEntity jobTemplateEntity =
        newShellJobTemplateEntity(
            "job_template_with_jobs", "A job template with jobs", METALAKE_NAME);
    jobTemplateMetaService.insertJobTemplate(jobTemplateEntity, false);

    // Create a job using the template
    JobEntity jobEntity1 =
        newJobEntity("job_template_with_jobs", JobHandle.Status.STARTED, METALAKE_NAME);
    backend.insert(jobEntity1, false);

    JobEntity jobEntity2 =
        newJobEntity("job_template_with_jobs", JobHandle.Status.SUCCEEDED, METALAKE_NAME);
    backend.insert(jobEntity2, false);

    boolean deleted =
        jobTemplateMetaService.deleteJobTemplate(
            NameIdentifierUtil.ofJobTemplate(METALAKE_NAME, "job_template_with_jobs"));
    Assertions.assertTrue(deleted);

    // Verify that the jobs are deleted
    List<JobEntity> jobs =
        JobMetaService.getInstance().listJobsByNamespace(NamespaceUtil.ofJob(METALAKE_NAME));
    Assertions.assertEquals(0, jobs.size());
  }

  static JobTemplateEntity newShellJobTemplateEntity(String name, String comment, String metalake) {
    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withExecutable("/bin/echo")
            .build();

    return JobTemplateEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  static JobTemplateEntity newSparkJobTemplateEntity(String name, String comment, String metalake) {
    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withClassName("org.apache.spark.examples.SparkPi")
            .withExecutable("file:/path/to/spark-examples.jar")
            .build();

    return JobTemplateEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  static JobEntity newJobEntity(String templateName, JobHandle.Status status, String metalake) {
    return JobEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withJobExecutionId(RandomIdGenerator.INSTANCE.nextId() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStatus(status)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
