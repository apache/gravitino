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
  public void testBuildRequiresFinishedAt() {
    // finishedAt is a required field - it must be explicitly set (using the storage layer's
    // "not finished" sentinel, <= 0, when the job hasn't finished), regardless of status. This
    // prevents JobPO from having to silently fabricate or default the value.
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
                .build());
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
            .withFinishedAt(1700000000000L)
            .build();

    Assertions.assertEquals(1700000000000L, jobEntity.finishedAt());
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
            .withFinishedAt(-1L)
            .build();
    Assertions.assertNull(negativeFinishedAt.finishedAtAsInstant());
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
            .withFinishedAt(epochMilli)
            .build();

    Assertions.assertEquals(Instant.ofEpochMilli(epochMilli), jobEntity.finishedAtAsInstant());
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
            .withFinishedAt(1700000000000L)
            .build();

    Assertions.assertNotEquals(notFinished, finished);
    Assertions.assertNotEquals(notFinished.hashCode(), finished.hashCode());
  }
}
