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

import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.meta.JobEntity;

/**
 * Represents information about a job, including its ID, template name, status, and audit details.
 */
@DeveloperApi
public final class JobInfo {

  private final String jobId;

  private final String jobTemplateName;

  private final JobHandle.Status jobStatus;

  private final Audit audit;

  private JobInfo(String jobId, String jobTemplateName, JobHandle.Status jobStatus, Audit audit) {
    this.jobId = jobId;
    this.jobTemplateName = jobTemplateName;
    this.jobStatus = jobStatus;
    this.audit = audit;
  }

  /**
   * Creates a JobInfo instance from a JobEntity.
   *
   * @param jobEntity the JobEntity to convert
   * @return a JobInfo instance containing information from the JobEntity
   */
  public static JobInfo fromJobEntity(JobEntity jobEntity) {
    return new JobInfo(
        jobEntity.name(), jobEntity.jobTemplateName(), jobEntity.status(), jobEntity.auditInfo());
  }

  /**
   * Returns the unique identifier of the job.
   *
   * @return the job ID
   */
  public String jobId() {
    return jobId;
  }

  /**
   * Returns the name of the job template associated with the job.
   *
   * @return the job template name
   */
  public String jobTemplateName() {
    return jobTemplateName;
  }

  /**
   * Returns the current status of the job.
   *
   * @return the job status
   */
  public JobHandle.Status jobStatus() {
    return jobStatus;
  }

  /**
   * Returns the audit information associated with the job.
   *
   * @return the audit information
   */
  public Audit auditInfo() {
    return audit;
  }
}
