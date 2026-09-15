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
package org.apache.gravitino.meta;

import java.time.Instant;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobEntity {

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @Test
  public void testBuildRequiresStartedAt() {
    // startedAt is a required field - it must be explicitly set (using the storage layer's
    // "not started" sentinel, <= 0, when the job hasn't started), regardless of status. finishedAt
    // is set here so only the missing startedAt triggers the failure.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobEntity.builder()
                .withId(1L)
                .withJobExecutionId("job-execution-1")
                .withJobTemplateName("test-job-template")
                .withStatus(JobHandle.Status.QUEUED)
                .withNamespace(NamespaceUtil.ofJob("test"))
                .withAuditInfo(AUDIT_INFO)
                .withFinishedAt(0L)
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobEntity.builder()
                .withId(2L)
                .withJobExecutionId("job-execution-2")
                .withJobTemplateName("test-job-template")
                .withStatus(JobHandle.Status.SUCCEEDED)
                .withNamespace(NamespaceUtil.ofJob("test"))
                .withAuditInfo(AUDIT_INFO)
                .withFinishedAt(1700000000000L)
                .build());
  }

  @Test
  public void testBuildRequiresFinishedAt() {
    // finishedAt is a required field - it must be explicitly set (using the storage layer's
    // "not finished" sentinel, <= 0, when the job hasn't finished), regardless of status.
    // startedAt is set here so only the missing finishedAt triggers the failure. Together with
    // testBuildRequiresStartedAt, this prevents JobPO from having to silently fabricate or
    // default either value.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobEntity.builder()
                .withId(1L)
                .withJobExecutionId("job-execution-1")
                .withJobTemplateName("test-job-template")
                .withStatus(JobHandle.Status.QUEUED)
                .withNamespace(NamespaceUtil.ofJob("test"))
                .withAuditInfo(AUDIT_INFO)
                .withStartedAt(0L)
                .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JobEntity.builder()
                .withId(2L)
                .withJobExecutionId("job-execution-2")
                .withJobTemplateName("test-job-template")
                .withStatus(JobHandle.Status.SUCCEEDED)
                .withNamespace(NamespaceUtil.ofJob("test"))
                .withAuditInfo(AUDIT_INFO)
                .withStartedAt(1700000000000L)
                .build());
  }

  @Test
  public void testStartedAt() {
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertEquals(1700000000000L, jobEntity.startedAt());
  }

  @Test
  public void testFinishedAt() {
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1699999999000L)
            .withFinishedAt(1700000000000L)
            .build();

    Assertions.assertEquals(1700000000000L, jobEntity.finishedAt());
  }

  @Test
  public void testStartedAtAsInstantWhenNotStarted() {
    // The storage layer's sentinel (<= 0) means "not started".
    JobEntity zeroStartedAt =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();
    Assertions.assertNull(zeroStartedAt.startedAtAsInstant());

    JobEntity negativeStartedAt =
        JobEntity.builder()
            .withId(2L)
            .withJobExecutionId("job-execution-2")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(-1L)
            .withFinishedAt(0L)
            .build();
    Assertions.assertNull(negativeStartedAt.startedAtAsInstant());
  }

  @Test
  public void testFinishedAtAsInstantWhenNotFinished() {
    // The storage layer's sentinel (<= 0) means "not finished".
    JobEntity zeroFinishedAt =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();
    Assertions.assertNull(zeroFinishedAt.finishedAtAsInstant());

    JobEntity negativeFinishedAt =
        JobEntity.builder()
            .withId(2L)
            .withJobExecutionId("job-execution-2")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(-1L)
            .build();
    Assertions.assertNull(negativeFinishedAt.finishedAtAsInstant());
  }

  @Test
  public void testStartedAtAsInstantWhenStarted() {
    long epochMilli = Instant.now().toEpochMilli();
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(epochMilli)
            .withFinishedAt(0L)
            .build();

    Assertions.assertEquals(Instant.ofEpochMilli(epochMilli), jobEntity.startedAtAsInstant());
  }

  @Test
  public void testFinishedAtAsInstantWhenFinished() {
    long epochMilli = Instant.now().toEpochMilli();
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(epochMilli - 1000)
            .withFinishedAt(epochMilli)
            .build();

    Assertions.assertEquals(Instant.ofEpochMilli(epochMilli), jobEntity.finishedAtAsInstant());
  }

  @Test
  public void testEqualsAndHashCodeIncludeStartedAt() {
    JobEntity notStarted =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    JobEntity sameAsNotStarted =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertEquals(notStarted, sameAsNotStarted);
    Assertions.assertEquals(notStarted.hashCode(), sameAsNotStarted.hashCode());

    // Same identity/status/audit but a different startedAt (e.g. the same job just after it
    // transitioned to STARTED) must not compare equal.
    JobEntity started =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertNotEquals(notStarted, started);
    Assertions.assertNotEquals(notStarted.hashCode(), started.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeIncludeFinishedAt() {
    JobEntity notFinished =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(0L)
            .build();

    JobEntity sameAsNotFinished =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertEquals(notFinished, sameAsNotFinished);
    Assertions.assertEquals(notFinished.hashCode(), sameAsNotFinished.hashCode());

    // Same identity/status/audit but a different finishedAt (e.g. the same job just after it
    // transitioned to a terminal state) must not compare equal.
    JobEntity finished =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.STARTED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(1700000000000L)
            .withFinishedAt(1700000001000L)
            .build();

    Assertions.assertNotEquals(notFinished, finished);
    Assertions.assertNotEquals(notFinished.hashCode(), finished.hashCode());
  }

  @Test
  public void testRuntimeJobTemplateDefaultsToNullWhenNotSet() {
    // Unlike startedAt/finishedAt, runtimeJobTemplate is optional - jobs run before this field
    // was introduced have no resolved template to backfill, so building without it must not
    // throw.
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertNull(jobEntity.runtimeJobTemplate());
  }

  @Test
  public void testRuntimeJobTemplate() {
    String runtimeJobTemplateJson = "{\"jobType\":\"shell\",\"name\":\"test-job-template\"}";
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .build();

    Assertions.assertEquals(runtimeJobTemplateJson, jobEntity.runtimeJobTemplate());
  }

  @Test
  public void testEqualsAndHashCodeIncludeRuntimeJobTemplate() {
    JobEntity withoutTemplate =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    JobEntity sameWithoutTemplate =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    Assertions.assertEquals(withoutTemplate, sameWithoutTemplate);
    Assertions.assertEquals(withoutTemplate.hashCode(), sameWithoutTemplate.hashCode());

    // Same identity/status/audit but a resolved runtime template must not compare equal.
    JobEntity withTemplate =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate("{\"jobType\":\"shell\",\"name\":\"test-job-template\"}")
            .build();

    Assertions.assertNotEquals(withoutTemplate, withTemplate);
    Assertions.assertNotEquals(withoutTemplate.hashCode(), withTemplate.hashCode());
  }
}
