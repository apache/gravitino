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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;

/**
 * Interface for job management operations. This interface will be mixed with GravitinoClient to
 * provide the ability to manage jobs within the Gravitino system.
 */
public interface SupportsJobs {

  /**
   * Lists all the registered job templates in Gravitino.
   *
   * @return a list of job templates
   */
  List<JobTemplate> listJobTemplates();

  /**
   * Register a job template with the specified job template to Gravitino. The registered job will
   * be maintained in Gravitino, allowing it to be executed later.
   *
   * @param jobTemplate the template for the job
   * @throws JobTemplateAlreadyExistsException if a job template with the same name already exists
   */
  void registerJobTemplate(JobTemplate jobTemplate) throws JobTemplateAlreadyExistsException;

  /**
   * Retrieves a job template by its name.
   *
   * @param jobTemplateName the name of the job template to retrieve
   * @return the job template associated with the specified name
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  JobTemplate getJobTemplate(String jobTemplateName) throws NoSuchJobTemplateException;

  /**
   * Deletes a job template by its name. This will remove the job template from Gravitino, and it
   * will no longer be available for execution.
   *
   * @param jobTemplateName the name of the job template to delete
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  void deleteJobTemplate(String jobTemplateName) throws NoSuchJobTemplateException;

  /**
   * Lists all the jobs by the specified job template name. This will return a list of job handles
   * associated with the specified job template. Each job handle represents a specific job.
   *
   * @param jobTemplateName the name of the job template to list jobs for
   * @return a list of job handles associated with the specified job template
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  List<JobHandle> listJobs(String jobTemplateName) throws NoSuchJobTemplateException;

  /**
   * run a job with the template name and configuration. The jobConf is a map of key-value contains
   * the variables that will be used to replace the templated parameters in the job template.
   *
   * @param jobTemplateName the name of the job template to run
   * @param jobConf the configuration for the job
   * @return a handle to the run job
   * @throws NoSuchJobTemplateException if no job template with the specified name exists
   */
  JobHandle runJob(String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException;

  /**
   * Retrieves a job by its ID.
   *
   * @param jobId the ID of the job to retrieve
   * @return a handle to the job
   * @throws NoSuchJobException if the job with the specified ID does not exist
   */
  JobHandle getJob(String jobId) throws NoSuchJobException;

  /**
   * Cancels a job by its ID.
   *
   * @param jobId the ID of the job to cancel
   * @throws NoSuchJobException if the job with the specified ID does not exist
   */
  void cancelJob(String jobId) throws NoSuchJobException;
}
