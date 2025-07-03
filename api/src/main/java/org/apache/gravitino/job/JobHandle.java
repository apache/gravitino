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

    /** The job has been cancelled. */
    CANCELLED;
  }

  /**
   * Listener interface for job events. Users can implement this interface to hook in some
   * customized behavior when a job is queued, started, failed, or succeeded. Note that the listener
   * is running in the client side, so if the client is terminated, the listener will not work
   * anymore.
   */
  interface Listener {

    /** Called when the job is queued. */
    void onJobQueued();

    /** Called when the job is started. */
    void onJobStarted();

    /** Called when the job has failed. */
    void onJobFailed();

    /** Called when the job has succeeded. */
    void onJobSucceeded();

    /** Called when the job has been cancelled. */
    void onJobCancelled();
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

  /**
   * Add a listener to the job handle. The listener will be notified when the job is queued,
   * started, failed, or succeeded.
   *
   * @param l the listener to be added
   */
  void addListener(Listener l);
}
