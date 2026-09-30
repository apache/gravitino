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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.json.JsonUtils;
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

  @Test
  public void testFromJobEntityWithoutRuntimeJobTemplate() {
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .build();

    JobInfo jobInfo = JobInfo.fromJobEntity(jobEntity);

    // No runtime job template stored (e.g. a job run before this field was introduced) - must
    // round-trip as null rather than failing to convert.
    Assertions.assertNull(jobInfo.runtimeJobTemplate());
  }

  @Test
  public void testFromJobEntityWithRuntimeJobTemplate() throws Exception {
    JobTemplate resolvedTemplate =
        ShellJobTemplate.builder()
            .withName("test-job-template")
            .withComment("resolved")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("resolved-arg"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "resolved-value"))
            .build();
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    String runtimeJobTemplateJson =
        JsonUtils.anyFieldMapper()
            .writeValueAsString(
                DTOConverters.toDTO(resolvedTemplate, DTOConverters.toDTO(auditInfo)));

    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(auditInfo)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .build();

    JobInfo jobInfo = JobInfo.fromJobEntity(jobEntity);

    Assertions.assertEquals(resolvedTemplate, jobInfo.runtimeJobTemplate());
  }

  @Test
  public void testFromJobEntityWithMalformedRuntimeJobTemplateDoesNotThrow() {
    // fromJobEntity() is called inside JobEventDispatcher's try block for getJob/runJob/
    // cancelJob, after the underlying operation has already succeeded - a malformed or
    // forward-incompatible stored runtime job template (e.g. a job type unknown to this server
    // version) must not turn that already-completed operation into a failure. It should just be
    // omitted from the built JobInfo.
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(1L)
            .withJobExecutionId("job-execution-1")
            .withJobTemplateName("test-job-template")
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob("test"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate("{not-valid-json")
            .build();

    JobInfo jobInfo = Assertions.assertDoesNotThrow(() -> JobInfo.fromJobEntity(jobEntity));

    Assertions.assertNull(jobInfo.runtimeJobTemplate());
    Assertions.assertEquals(jobEntity.name(), jobInfo.jobId());
  }
}
