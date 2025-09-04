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
   * Retrieves a job by its ID in the specified metalake.
   *
   * @param metalake the name of the metalake
   * @param jobId the ID of the job to retrieve
   * @return the job entity associated with the specified ID
   * @throws NoSuchJobException if no job with the specified ID exists
   */
  JobEntity getJob(String metalake, String jobId) throws NoSuchJobException;

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
