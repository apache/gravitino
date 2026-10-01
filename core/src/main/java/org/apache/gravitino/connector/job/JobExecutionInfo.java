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

import com.google.common.base.Preconditions;
import java.time.Instant;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.job.JobHandle;

/**
 * A snapshot of a job's execution state, as reported by a {@link JobExecutor}.
 *
 * <p>The timestamps are attributes of the job, not of a particular status. Once a job has started,
 * every later snapshot carries its start time, including the terminal ones, so no information is
 * lost when Gravitino doesn't observe every status the job goes through.
 *
 * <p>More optional fields may be added in the future. Create instances with {@link
 * #of(JobHandle.Status)} or {@code builder()}, so that the existing code keeps working when fields
 * are added.
 */
@DeveloperApi
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with", toBuilder = true)
public final class JobExecutionInfo {

  /** The status of the job. */
  @NonNull private final JobHandle.Status status;

  /**
   * The time when the job actually started executing, or null if the job hasn't started executing,
   * or the started time is unknown to the job executor.
   */
  @Nullable private final Instant startedAt;

  /**
   * The time when the job actually finished, or null if the job hasn't finished, or the finished
   * time is unknown to the job executor.
   */
  @Nullable private final Instant finishedAt;

  /**
   * Create a job execution info that only carries the status of the job, without any timestamp.
   *
   * @param status The status of the job.
   * @return the job execution info.
   */
  public static JobExecutionInfo of(JobHandle.Status status) {
    return builder().withStatus(status).build();
  }

  /**
   * Create a copy of this job execution info with the job moved to {@link
   * JobHandle.Status#STARTED}.
   *
   * @param startedAt The time when the job started executing.
   * @return the job execution info of the started job.
   */
  public JobExecutionInfo started(Instant startedAt) {
    Preconditions.checkArgument(startedAt != null, "The started time must not be null");
    return toBuilder().withStatus(JobHandle.Status.STARTED).withStartedAt(startedAt).build();
  }

  /**
   * Create a copy of this job execution info with the job moved to the given terminal status. The
   * started time, if any, is carried forward.
   *
   * @param terminalStatus The terminal status of the job, one of {@link
   *     JobHandle.Status#SUCCEEDED}, {@link JobHandle.Status#FAILED} and {@link
   *     JobHandle.Status#CANCELLED}.
   * @param finishedAt The time when the job finished.
   * @return the job execution info of the finished job.
   */
  public JobExecutionInfo finished(JobHandle.Status terminalStatus, Instant finishedAt) {
    Preconditions.checkArgument(
        terminalStatus == JobHandle.Status.SUCCEEDED
            || terminalStatus == JobHandle.Status.FAILED
            || terminalStatus == JobHandle.Status.CANCELLED,
        "The status of a finished job must be SUCCEEDED, FAILED or CANCELLED, but got: %s",
        terminalStatus);
    Preconditions.checkArgument(finishedAt != null, "The finished time must not be null");
    return toBuilder().withStatus(terminalStatus).withFinishedAt(finishedAt).build();
  }
}
