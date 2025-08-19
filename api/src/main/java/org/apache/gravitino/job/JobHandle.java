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

/**
 * JobHandle is an interface that is returned by the job submission, which provides methods to get
 * the job name, job ID, job status, and to add listeners for jobs.
 */
public interface JobHandle {

  /** The prefix for job IDs, every job ID returned from Gravitino will be like job-uuid */
  String JOB_ID_PREFIX = "job-";

  /** The status of the job. */
  enum Status {

    /** The job is in the queue and waiting to be executed. */
    QUEUED,

    /** The job is currently being executed. */
    STARTED,

    /** The job has failed during execution. */
    FAILED,

    /** The job has completed successfully. */
    SUCCEEDED,

    /** The job is being cancelled. */
    CANCELLING,

    /** The job has been cancelled. */
    CANCELLED;
  }

  /**
   * Get the name of the job template.
   *
   * @return the name of the job template
   */
  String jobTemplateName();

  /**
   * Get the unique identifier of the job.
   *
   * @return the job ID
   */
  String jobId();

  /**
   * Get the current status of the job.
   *
   * @return the status of the job
   */
  Status jobStatus();
}
