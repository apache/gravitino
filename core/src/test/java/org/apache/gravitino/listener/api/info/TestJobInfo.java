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
package org.apache.gravitino.listener.api.info;

import java.time.Instant;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobInfo {

  @Test
  public void testFromJobEntityWhenNotStartedOrFinished() {
    Instant queuedAt = Instant.now();
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AuditInfo.builder().withCreator("test").withCreateTime(queuedAt).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    JobInfo jobInfo = JobInfo.fromJobEntity(jobEntity);

    Assertions.assertEquals(jobEntity.name(), jobInfo.jobId());
    Assertions.assertEquals(jobEntity.jobTemplateName(), jobInfo.jobTemplateName());
    Assertions.assertEquals(jobEntity.status(), jobInfo.jobStatus());
    Assertions.assertEquals(queuedAt, jobInfo.queuedAt());
    Assertions.assertNull(jobInfo.startedAt());
    Assertions.assertNull(jobInfo.finishedAt());
  }

  @Test
  public void testFromJobEntityWhenStartedAndFinished() {
    Instant queuedAt = Instant.now();
    long startedAt = queuedAt.toEpochMilli() + 1000;
    long finishedAt = queuedAt.toEpochMilli() + 2000;
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(AuditInfo.builder().withCreator("test").withCreateTime(queuedAt).build())
            .withStartedAt(startedAt)
            .withFinishedAt(finishedAt)
            .build();

    JobInfo jobInfo = JobInfo.fromJobEntity(jobEntity);

    Assertions.assertEquals(queuedAt, jobInfo.queuedAt());
    Assertions.assertEquals(Instant.ofEpochMilli(startedAt), jobInfo.startedAt());
    Assertions.assertEquals(Instant.ofEpochMilli(finishedAt), jobInfo.finishedAt());
  }
}
