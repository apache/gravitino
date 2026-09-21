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

package org.apache.gravitino.connector.job;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobExecutionInfo {

  @Test
  public void testOf() {
    JobExecutionInfo info = JobExecutionInfo.of(JobHandle.Status.QUEUED);
    Assertions.assertEquals(JobHandle.Status.QUEUED, info.status());
    Assertions.assertNull(info.startedAt());
    Assertions.assertNull(info.finishedAt());
  }

  @Test
  public void testBuilder() {
    Instant startedAt = Instant.ofEpochMilli(1000L);
    Instant finishedAt = Instant.ofEpochMilli(2000L);
    JobExecutionInfo info =
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withStartedAt(startedAt)
            .withFinishedAt(finishedAt)
            .build();

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, info.status());
    Assertions.assertEquals(startedAt, info.startedAt());
    Assertions.assertEquals(finishedAt, info.finishedAt());
    Assertions.assertEquals(
        info,
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withStartedAt(startedAt)
            .withFinishedAt(finishedAt)
            .build());
    Assertions.assertNotEquals(info, JobExecutionInfo.of(JobHandle.Status.SUCCEEDED));

    Assertions.assertThrows(NullPointerException.class, () -> JobExecutionInfo.of(null));
    Assertions.assertThrows(
        NullPointerException.class,
        () -> JobExecutionInfo.builder().withStartedAt(startedAt).build());
    Assertions.assertEquals(
        JobExecutionInfo.builder()
            .withStatus(JobHandle.Status.FAILED)
            .withStartedAt(startedAt)
            .withFinishedAt(finishedAt)
            .build(),
        info.toBuilder().withStatus(JobHandle.Status.FAILED).build());
  }

  @Test
  public void testStartedAndFinished() {
    Instant startedAt = Instant.ofEpochMilli(1000L);
    Instant finishedAt = Instant.ofEpochMilli(2000L);

    JobExecutionInfo started = JobExecutionInfo.of(JobHandle.Status.QUEUED).started(startedAt);
    Assertions.assertEquals(JobHandle.Status.STARTED, started.status());
    Assertions.assertEquals(startedAt, started.startedAt());
    Assertions.assertNull(started.finishedAt());

    // The started time is carried forward to the finished job.
    JobExecutionInfo succeeded = started.finished(JobHandle.Status.SUCCEEDED, finishedAt);
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, succeeded.status());
    Assertions.assertEquals(startedAt, succeeded.startedAt());
    Assertions.assertEquals(finishedAt, succeeded.finishedAt());

    // A job cancelled before it started has no started time.
    JobExecutionInfo cancelled =
        JobExecutionInfo.of(JobHandle.Status.QUEUED)
            .finished(JobHandle.Status.CANCELLED, finishedAt);
    Assertions.assertEquals(JobHandle.Status.CANCELLED, cancelled.status());
    Assertions.assertNull(cancelled.startedAt());
    Assertions.assertEquals(finishedAt, cancelled.finishedAt());

    Assertions.assertThrows(IllegalArgumentException.class, () -> started.started(null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> started.finished(JobHandle.Status.CANCELLING, finishedAt));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> started.finished(JobHandle.Status.FAILED, null));
  }

  @Test
  public void testDefaultGetJobStatus() {
    // getJobStatus is derived from the execution info reported by the job executor.
    JobExecutionInfo info =
        JobExecutionInfo.of(JobHandle.Status.QUEUED)
            .started(Instant.ofEpochMilli(1000L))
            .finished(JobHandle.Status.SUCCEEDED, Instant.ofEpochMilli(2000L));
    JobExecutor jobExecutor =
        new JobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {}

          @Override
          public String submitJob(JobTemplate jobTemplate) {
            return "job-1";
          }

          @Override
          public JobExecutionInfo getJobExecutionInfo(String jobId) {
            return info;
          }

          @Override
          public void cancelJob(String jobId) {}

          @Override
          public void close() throws IOException {}
        };

    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, jobExecutor.getJobStatus("job-1"));
  }

  @Test
  public void testGetJobExecutionInfoNotImplemented() {
    // A job executor that doesn't override getJobExecutionInfo fails loudly instead of silently
    // reporting no timestamps.
    JobExecutor jobExecutor =
        new JobExecutor() {
          @Override
          public void initialize(Map<String, String> configs) {}

          @Override
          public String submitJob(JobTemplate jobTemplate) {
            return "job-1";
          }

          @Override
          public void cancelJob(String jobId) {}

          @Override
          public void close() throws IOException {}
        };

    UnsupportedOperationException e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> jobExecutor.getJobExecutionInfo("job-1"));
    Assertions.assertTrue(e.getMessage().contains("getJobExecutionInfo()"), e.getMessage());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> jobExecutor.getJobStatus("job-1"));
  }
}
