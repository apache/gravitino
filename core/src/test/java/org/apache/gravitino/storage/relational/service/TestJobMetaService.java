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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.job.JobHandle;
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

public class TestJobMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_test_job_meta_service";

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @Test
  public void testInsertAndListJobs() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    JobEntity job1 =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job1, false));

    JobEntity job2 =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job2, false));

    List<JobEntity> jobs =
        JobMetaService.getInstance().listJobsByNamespace(NamespaceUtil.ofJob(METALAKE_NAME));
    Assertions.assertEquals(2, jobs.size());
    Assertions.assertTrue(jobs.contains(job1));
    Assertions.assertTrue(jobs.contains(job2));

    // Test listing jobs by job template identifier
    String[] levels = ArrayUtils.add(jobTemplate.namespace().levels(), jobTemplate.name());
    Namespace jobTemplateIdentNs = Namespace.of(levels);
    List<JobEntity> jobsByTemplate =
        JobMetaService.getInstance().listJobsByNamespace(jobTemplateIdentNs);
    Assertions.assertEquals(2, jobsByTemplate.size());
    Assertions.assertTrue(jobsByTemplate.contains(job1));
    Assertions.assertTrue(jobsByTemplate.contains(job2));

    // Test listing jobs by non-existing template identifier
    levels = ArrayUtils.add(jobTemplate.namespace().levels(), "non_existing_template");
    List<JobEntity> emptyJobs =
        JobMetaService.getInstance().listJobsByNamespace(Namespace.of(levels));
    Assertions.assertTrue(emptyJobs.isEmpty());
  }

  @Test
  public void testInsertAndGetJob() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    JobEntity job =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job, false));

    JobEntity retrievedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(job, retrievedJob);

    // Test getting a job with a non-existing identifier
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, "non_existing_job")));

    // Test insert duplicate job
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> JobMetaService.getInstance().insertJob(job, false));

    // Test insert job with overwrite
    JobEntity jobOverwrite =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId("job-execution-new")
            .withStatus(JobHandle.Status.FAILED)
            .withNamespace(job.namespace())
            .withAuditInfo(job.auditInfo())
            .withJobTemplateName(job.jobTemplateName())
            .build();
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(jobOverwrite, true));
    JobEntity updatedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, jobOverwrite.name()));
    Assertions.assertEquals(jobOverwrite, updatedJob);

    // Test insert and get job with finishedAt
    JobEntity finishedJob =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.SUCCEEDED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(finishedJob, false));

    JobEntity retrievedFinishedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, finishedJob.name()));
    Assertions.assertTrue(retrievedFinishedJob.finishedAt() > 0);
  }

  @Test
  public void testDeleteJobsByLegacyTimeline() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    JobEntity job =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job, false));

    JobEntity retrievedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(job, retrievedJob);

    long timestamp = System.currentTimeMillis();
    JobEntity updatedJob =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId(job.jobExecutionId())
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withNamespace(job.namespace())
            .withAuditInfo(job.auditInfo())
            .withJobTemplateName(job.jobTemplateName())
            .build();
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(updatedJob, true));

    retrievedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, updatedJob.name()));
    Assertions.assertEquals(updatedJob, retrievedJob);

    long newTimestamp = timestamp + 1000;
    Assertions.assertDoesNotThrow(
        () -> JobMetaService.getInstance().deleteJobsByLegacyTimeline(newTimestamp, 10));

    List<JobEntity> jobs =
        JobMetaService.getInstance().listJobsByNamespace(NamespaceUtil.ofJob(METALAKE_NAME));
    Assertions.assertTrue(jobs.isEmpty(), "Jobs should be deleted by legacy timeline");
  }

  @Test
  public void testDeleteJobByIdentifier() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    JobEntity job =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job, false));

    JobEntity retrievedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(job, retrievedJob);

    Assertions.assertTrue(
        JobMetaService.getInstance()
            .deleteJob(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name())));

    // Verify that the job is deleted
    Assertions.assertFalse(
        JobMetaService.getInstance()
            .deleteJob(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name())));
  }
}
