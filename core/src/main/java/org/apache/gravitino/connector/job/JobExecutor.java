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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;

/**
 * The JobExecutor interface defines the API for executing jobs in a specific Job runner, for
 * example, Airflow, local runner, etc. The developer can implement this interface to adapt to the
 * specific job runner. The Gravitino core will use this interface to submit jobs, get job status,
 * etc.
 */
@DeveloperApi
public interface JobExecutor extends Closeable {

  /**
   * Initialize the job executor with the given configurations.
   *
   * @param configs A map of configuration key-value pairs.
   */
  void initialize(Map<String, String> configs);

  /**
   * Submit a job with the given name and job template to the external job runner.
   *
   * <p>The returned job identifier is unique in the external job runner, and can be used to track
   * the job status or cancel it later.
   *
   * <p>The placeholders in the job template has already been replaced with the actual values before
   * calling this method. So the implementors can directly use this job template to submit the job
   * to the external job runner.
   *
   * @param jobTemplate The job template containing the job configuration and parameters.
   * @return A unique identifier for the submitted job.
   */
  String submitJob(JobTemplate jobTemplate);

  /**
   * Get the status of a job by its unique identifier. The status should be one of the values in
   * {@link JobHandle.Status}. The implementors should query the external job runner to get the job
   * status, and map the status to the values in {@link JobHandle.Status}.
   *
   * @param jobId The unique identifier of the job.
   * @return The status of the job.
   * @throws NoSuchJobException If the job with the given identifier does not exist.
   */
  JobHandle.Status getJobStatus(String jobId) throws NoSuchJobException;

  /**
   * Cancel a job by its unique identifier. The job runner should stop the job if it is currently
   * running. If the job is already completed, it should return directly without any error. If the
   * job does not exist, it should throw a {@link NoSuchJobException}.
   *
   * <p>This method should not be blocked on the job cancellation, it should return immediately, and
   * Gravitino can further query the job status to check if the job is cancelled successfully.
   *
   * @param jobId The unique identifier of the job to cancel.
   * @throws NoSuchJobException If the job with the given identifier does not exist.
   */
  void cancelJob(String jobId) throws NoSuchJobException;

  /**
   * Whether the job with the given identifier is owned by this job executor instance, which means
   * this instance is able to query and cancel it.
   *
   * <p>In a multi-node deployment every Gravitino server has its own job executor instance, and all
   * of them see the same jobs from the shared metadata store. Gravitino only queries the status of,
   * or cancels, a job through the executor instance that owns it. The default implementation
   * returns {@code true}, which fits job executors backed by an external job runner that any
   * Gravitino server can reach.
   *
   * @param jobId The unique identifier of the job.
   * @return {@code true} if this executor instance owns the job, {@code false} otherwise.
   */
  default boolean ownsJob(String jobId) {
    return true;
  }

  /**
   * Whether the job state is only kept by the executor instance that owns the job, for example the
   * processes launched on the local node. If so, only the owning instance can cancel the job, so a
   * cancellation requested on another Gravitino server is carried out by the owner when it pulls
   * the job status.
   *
   * <p>The default implementation returns {@code false}.
   *
   * @return {@code true} if the job state is local to the owning executor instance, {@code false}
   *     otherwise.
   */
  default boolean isJobStateNodeLocal() {
    return false;
  }

  /**
   * Get the captured standard output of the job, as a list of lines.
   *
   * <p>The default implementation returns an empty list, so implementors that don't support output
   * retrieval don't need to override this method. Unlike {@link #getJobStatus(String)}/{@link
   * #cancelJob(String)}, this method never throws for a job the executor doesn't (or no longer)
   * know about - the job entity itself may still exist even after the executor's own bookkeeping
   * for its output has expired or been lost (e.g. across a restart), so "unknown to this executor"
   * is reported as empty output, not as an error.
   *
   * @param jobId The unique identifier of the job.
   * @param maxLines The maximum number of (most recent) lines to return, resolved by the caller
   *     from the {@code gravitino.job.outputMaxLines} configuration.
   * @param maxBytes The maximum number of (most recent) bytes to read from the underlying output,
   *     resolved by the caller from the {@code gravitino.job.outputMaxBytes} configuration. Bounds
   *     both the read cost and the response size regardless of how the content is shaped.
   * @return the stdout lines of the job, or an empty list if not available.
   */
  default List<String> getJobStdout(String jobId, int maxLines, int maxBytes) {
    return Collections.emptyList();
  }

  /**
   * Get the captured standard error output of the job, as a list of lines.
   *
   * <p>The default implementation returns an empty list, so implementors that don't support output
   * retrieval don't need to override this method. Unlike {@link #getJobStatus(String)}/{@link
   * #cancelJob(String)}, this method never throws for a job the executor doesn't (or no longer)
   * know about - the job entity itself may still exist even after the executor's own bookkeeping
   * for its output has expired or been lost (e.g. across a restart), so "unknown to this executor"
   * is reported as empty output, not as an error.
   *
   * @param jobId The unique identifier of the job.
   * @param maxLines The maximum number of (most recent) lines to return, resolved by the caller
   *     from the {@code gravitino.job.outputMaxLines} configuration.
   * @param maxBytes The maximum number of (most recent) bytes to read from the underlying output,
   *     resolved by the caller from the {@code gravitino.job.outputMaxBytes} configuration. Bounds
   *     both the read cost and the response size regardless of how the content is shaped.
   * @return the stderr lines of the job, or an empty list if not available.
   */
  default List<String> getJobStderr(String jobId, int maxLines, int maxBytes) {
    return Collections.emptyList();
  }
}
