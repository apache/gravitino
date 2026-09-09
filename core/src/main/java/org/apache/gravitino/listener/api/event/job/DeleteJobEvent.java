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

package org.apache.gravitino.listener.api.event.job;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.JobInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Represents an event triggered when a job has been deleted.
 *
 * <p>Jobs are deleted by the staging directory cleanup, which runs on the server's own schedule
 * rather than for a request. That makes this event the only signal a listener receives that a job
 * is gone, so a listener keeping a projection of jobs needs it to avoid serving jobs that no longer
 * exist.
 */
@DeveloperApi
public class DeleteJobEvent extends JobEvent {

  /**
   * The user reported for a deletion the server performs on its own schedule, where no request and
   * therefore no caller exists.
   */
  public static final String SYSTEM_USER = "system";

  private final JobInfo jobInfo;

  /**
   * Constructs a new {@code DeleteJobEvent} instance.
   *
   * @param user The user who initiated the deletion, or {@link #SYSTEM_USER} when the server
   *     deleted the job on its own schedule.
   * @param metalake The metalake name where the job resided.
   * @param jobInfo The information of the job that has been deleted.
   */
  public DeleteJobEvent(String user, String metalake, JobInfo jobInfo) {
    super(user, NameIdentifierUtil.ofJob(metalake, jobInfo.jobId()));
    this.jobInfo = jobInfo;
  }

  /**
   * Returns the information of the job that has been deleted.
   *
   * @return the job information.
   */
  public JobInfo deletedJobInfo() {
    return jobInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_JOB;
  }
}
