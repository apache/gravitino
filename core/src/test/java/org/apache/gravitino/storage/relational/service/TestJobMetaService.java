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
import org.junit.jupiter.api.TestTemplate;

public class TestJobMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_test_job_meta_service";

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @TestTemplate
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

  @TestTemplate
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
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(System.currentTimeMillis())
            .build();
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(jobOverwrite, true));
    JobEntity updatedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, jobOverwrite.name()));
    Assertions.assertEquals(jobOverwrite, updatedJob);

    // Test insert and get job with startedAt/finishedAt
    JobEntity finishedJob =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.SUCCEEDED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(finishedJob, false));

    JobEntity retrievedFinishedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, finishedJob.name()));
    Assertions.assertTrue(retrievedFinishedJob.startedAt() > 0);
    Assertions.assertTrue(retrievedFinishedJob.finishedAt() > 0);
  }

  @TestTemplate
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
            .withStartedAt(timestamp)
            .withFinishedAt(timestamp)
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

  @TestTemplate
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

  @TestTemplate
  public void testUpdateJob() throws IOException {
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
    JobMetaService.getInstance().insertJob(job, false);

    // Update the job to STARTED, setting startedAt and a new audit info.
    long startedAt = System.currentTimeMillis();
    AuditInfo startedAuditInfo =
        AuditInfo.builder()
            .withCreator(job.auditInfo().creator())
            .withCreateTime(job.auditInfo().createTime())
            .withLastModifier("updater")
            .withLastModifiedTime(Instant.now())
            .build();

    JobEntity startedJob =
        JobMetaService.getInstance()
            .updateJob(
                NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()),
                (JobEntity oldJob) ->
                    JobEntity.builder()
                        .withId(oldJob.id())
                        .withJobExecutionId(oldJob.jobExecutionId())
                        .withJobTemplateName(oldJob.jobTemplateName())
                        .withNamespace(oldJob.namespace())
                        .withStatus(JobHandle.Status.STARTED)
                        .withAuditInfo(startedAuditInfo)
                        .withStartedAt(startedAt)
                        .withFinishedAt(oldJob.finishedAt())
                        .build());

    Assertions.assertEquals(JobHandle.Status.STARTED, startedJob.status());
    Assertions.assertEquals(startedAt, startedJob.startedAt());
    Assertions.assertEquals(startedAuditInfo, startedJob.auditInfo());
    Assertions.assertEquals(0L, startedJob.finishedAt());

    // The update must actually be persisted, not just returned.
    JobEntity fetchedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(startedJob, fetchedJob);

    // A second, independent update must keep working (e.g. an internal version counter bumped by
    // the first update must not break a subsequent one).
    long finishedAt = startedAt + 1000;
    JobEntity finishedJob =
        JobMetaService.getInstance()
            .updateJob(
                NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()),
                (JobEntity oldJob) ->
                    JobEntity.builder()
                        .withId(oldJob.id())
                        .withJobExecutionId(oldJob.jobExecutionId())
                        .withJobTemplateName(oldJob.jobTemplateName())
                        .withNamespace(oldJob.namespace())
                        .withStatus(JobHandle.Status.SUCCEEDED)
                        .withAuditInfo(oldJob.auditInfo())
                        .withStartedAt(oldJob.startedAt())
                        .withFinishedAt(finishedAt)
                        .build());

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, finishedJob.status());
    Assertions.assertEquals(startedAt, finishedJob.startedAt());
    Assertions.assertEquals(finishedAt, finishedJob.finishedAt());

    fetchedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(finishedJob, fetchedJob);
  }

  @TestTemplate
  public void testInsertAndGetJobWithRuntimeJobTemplate() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    String runtimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"test_job_template\",\"executable\":\"/bin/echo\"}";
    JobEntity job =
        JobEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withJobExecutionId("job-execution-runtime-template")
            .withJobTemplateName(jobTemplate.name())
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob(METALAKE_NAME))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .build();
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(job, false));

    JobEntity retrievedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(runtimeJobTemplateJson, retrievedJob.runtimeJobTemplate());
    Assertions.assertEquals(job, retrievedJob);

    // A job inserted without a runtime job template (e.g. a row from before this field existed)
    // must round-trip as null rather than failing to insert/select.
    JobEntity jobWithoutTemplate =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);
    Assertions.assertDoesNotThrow(
        () -> JobMetaService.getInstance().insertJob(jobWithoutTemplate, false));
    JobEntity retrievedJobWithoutTemplate =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, jobWithoutTemplate.name()));
    Assertions.assertNull(retrievedJobWithoutTemplate.runtimeJobTemplate());

    // Overwriting a job must also overwrite its stored runtime job template.
    String updatedRuntimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"test_job_template\",\"executable\":\"/bin/echo\","
            + "\"arguments\":[\"resolved\"]}";
    JobEntity jobOverwrite =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId(job.jobExecutionId())
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(job.namespace())
            .withAuditInfo(job.auditInfo())
            .withJobTemplateName(job.jobTemplateName())
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(updatedRuntimeJobTemplateJson)
            .build();
    Assertions.assertDoesNotThrow(() -> JobMetaService.getInstance().insertJob(jobOverwrite, true));

    JobEntity retrievedOverwrittenJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, jobOverwrite.name()));
    Assertions.assertEquals(
        updatedRuntimeJobTemplateJson, retrievedOverwrittenJob.runtimeJobTemplate());
  }

  @TestTemplate
  public void testUpdateJobPersistsRuntimeJobTemplateChange() throws IOException {
    // Unlike jobTemplateName, this storage layer does not enforce that the runtime job template
    // never changes - it persists whatever the updater lambda's returned entity says, the same
    // way it persists status/timestamps/audit info. Keeping the resolved template unchanged
    // across status transitions is JobManager's responsibility (its updater functions always
    // carry the existing value forward), not something guarded here.
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "test_job_template", "test_comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    String originalRuntimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"test_job_template\",\"executable\":\"/bin/echo\"}";
    JobEntity job =
        JobEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withJobExecutionId("job-execution-update-changes-template")
            .withJobTemplateName(jobTemplate.name())
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob(METALAKE_NAME))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(originalRuntimeJobTemplateJson)
            .build();
    JobMetaService.getInstance().insertJob(job, false);

    String newRuntimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"test_job_template\",\"executable\":\"/bin/echo\","
            + "\"arguments\":[\"resolved\"]}";
    JobEntity updatedJob =
        JobMetaService.getInstance()
            .updateJob(
                NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()),
                (JobEntity oldJob) ->
                    JobEntity.builder()
                        .withId(oldJob.id())
                        .withJobExecutionId(oldJob.jobExecutionId())
                        .withJobTemplateName(oldJob.jobTemplateName())
                        .withNamespace(oldJob.namespace())
                        .withStatus(JobHandle.Status.STARTED)
                        .withAuditInfo(oldJob.auditInfo())
                        .withStartedAt(System.currentTimeMillis())
                        .withFinishedAt(oldJob.finishedAt())
                        .withRuntimeJobTemplate(newRuntimeJobTemplateJson)
                        .build());
    Assertions.assertEquals(newRuntimeJobTemplateJson, updatedJob.runtimeJobTemplate());

    // The change must actually be persisted, not just returned.
    JobEntity persistedJob =
        JobMetaService.getInstance()
            .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()));
    Assertions.assertEquals(JobHandle.Status.STARTED, persistedJob.status());
    Assertions.assertEquals(newRuntimeJobTemplateJson, persistedJob.runtimeJobTemplate());
  }

  @TestTemplate
  public void testUpdateNonExistentJobThrowsNoSuchEntityException() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .updateJob(NameIdentifierUtil.ofJob(METALAKE_NAME, "job-999999"), e -> e));
  }

  @TestTemplate
  public void testUpdateJobWithMismatchedIdThrowsIllegalArgumentException() throws IOException {
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
    JobMetaService.getInstance().insertJob(job, false);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobMetaService.getInstance()
                .updateJob(
                    NameIdentifierUtil.ofJob(METALAKE_NAME, job.name()),
                    (JobEntity oldJob) ->
                        JobEntity.builder()
                            .withId(oldJob.id() + 1)
                            .withJobExecutionId(oldJob.jobExecutionId())
                            .withJobTemplateName(oldJob.jobTemplateName())
                            .withNamespace(oldJob.namespace())
                            .withStatus(oldJob.status())
                            .withAuditInfo(oldJob.auditInfo())
                            .withStartedAt(oldJob.startedAt())
                            .withFinishedAt(oldJob.finishedAt())
                            .build()));
  }

  @Test
  public void testUpdateJobWithMalformedIdentifierThrowsNoSuchEntityException() {
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .updateJob(NameIdentifierUtil.ofJob(METALAKE_NAME, "invalid"), e -> e));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .updateJob(
                    NameIdentifierUtil.ofJob(METALAKE_NAME, JobHandle.JOB_ID_PREFIX), e -> e));
  }

  @Test
  public void testGetJobWithMalformedIdentifierThrowsNoSuchEntityException() {
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE_NAME, "invalid")));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .getJobByIdentifier(
                    NameIdentifierUtil.ofJob(METALAKE_NAME, JobHandle.JOB_ID_PREFIX)));
  }

  @Test
  public void testDeleteJobWithMalformedIdentifierThrowsNoSuchEntityException() {
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .deleteJob(NameIdentifierUtil.ofJob(METALAKE_NAME, "invalid")));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            JobMetaService.getInstance()
                .deleteJob(NameIdentifierUtil.ofJob(METALAKE_NAME, JobHandle.JOB_ID_PREFIX)));
  }
}
