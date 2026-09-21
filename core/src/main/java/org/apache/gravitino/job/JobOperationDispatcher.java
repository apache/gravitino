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

package org.apache.gravitino.job;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;

/** The interface for job operation dispatcher. */
public interface JobOperationDispatcher extends Closeable {

  /**
   * Lists all the job templates in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @return a list of job templates
   */
  List<JobTemplateEntity> listJobTemplates(String metalake);

  /**
   * Registers a job template in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateEntity the job template entity to register
   * @throws JobTemplateAlreadyExistsException if a job template with the same name already exists
   */
  void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException;

  /**
   * Retrieves a job template by its name in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateName the name of the job template to retrieve
   * @return the job template entity associated with the specified name
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException;

  /**
   * Deletes a job template by its name in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateName the name of the job template to delete
   * @return true if the job template was successfully deleted, false if the job template does not
   *     exist
   * @throws InUseException if there are still queued or started jobs associated with the job
   */
  boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException;

  /**
   * Alters a job template by applying the specified changes in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateName the name of the job template to alter
   * @param changes the changes to apply to the job template
   * @return the updated job template entity after applying the changes
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   * @throws IllegalArgumentException if any of the changes cannot be applied to the job template.
   */
  JobTemplateEntity alterJobTemplate(
      String metalake, String jobTemplateName, JobTemplateChange... changes)
      throws NoSuchJobTemplateException, IllegalArgumentException;

  /**
   * List all the jobs. If the jobTemplateName is provided, it will list the jobs associated with
   * that job template, if not, it will list all the jobs in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateName the name of the job template to filter jobs by, if present
   * @return a list of job entities
   * @throws NoSuchJobTemplateException if the job template does not exist
   */
  List<JobEntity> listJobs(String metalake, Optional<String> jobTemplateName)
      throws NoSuchJobTemplateException;

  /**
   * Retrieves a job by its ID in the specified metalake, optionally including its captured
   * stdout/stderr output (see {@link JobEntity#stdout()}/{@link JobEntity#stderr()}), using the
   * globally configured {@code gravitino.job.outputMaxLines}/{@code outputMaxBytes} caps.
   *
   * <p>Output is fetched live from the {@code JobExecutor} on every call, not persisted, so {@code
   * includeOutput} should only be set to {@code true} when the caller actually needs the output -
   * it is never included in {@link #listJobs(String, Optional)}, and callers that don't need output
   * should pass {@code false} here.
   *
   * @param metalake the name of the metalake
   * @param jobId the ID of the job to retrieve
   * @param includeOutput whether to also fetch and attach the job's stdout/stderr output
   * @return the job entity associated with the specified ID
   * @throws NoSuchJobException if no job with the specified ID exists
   */
  default JobEntity getJob(String metalake, String jobId, boolean includeOutput)
      throws NoSuchJobException {
    return getJob(metalake, jobId, includeOutput, null, null);
  }

  /**
   * Retrieves a job by its ID in the specified metalake, optionally including its captured
   * stdout/stderr output, with caller-specified caps on how much of it to return.
   *
   * <p>{@code maxLines}/{@code maxBytes} let a caller ask for less output than the globally
   * configured {@code gravitino.job.outputMaxLines}/{@code outputMaxBytes} caps, but never more - a
   * value larger than the global cap is clamped down to it, so the global configuration always
   * remains a hard upper bound. Passing {@code null} for either uses the global default. Both are
   * ignored when {@code includeOutput} is {@code false}.
   *
   * @param metalake the name of the metalake
   * @param jobId the ID of the job to retrieve
   * @param includeOutput whether to also fetch and attach the job's stdout/stderr output
   * @param maxLines the maximum number of (most recent) output lines to return, or {@code null} to
   *     use the global default
   * @param maxBytes the maximum number of (most recent) output bytes to read, or {@code null} to
   *     use the global default
   * @return the job entity associated with the specified ID
   * @throws NoSuchJobException if no job with the specified ID exists
   * @throws IllegalArgumentException if {@code maxLines} or {@code maxBytes} is specified and not
   *     positive
   */
  JobEntity getJob(
      String metalake, String jobId, boolean includeOutput, Integer maxLines, Integer maxBytes)
      throws NoSuchJobException;

  /**
   * Runs a job based on the specified job template and configuration in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobTemplateName the name of the job template to use for running the job
   * @param jobConf the runtime configuration for the job, which contains key-value pairs
   * @return the job entity representing the job
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  JobEntity runJob(String metalake, String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException;

  /**
   * Cancels a job by its ID in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobId the ID of the job to cancel
   * @return the job entity representing the job after cancellation
   * @throws NoSuchJobException if no job with the specified ID exists
   */
  JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException;
}
