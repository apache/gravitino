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

/** Represents an event triggered when a job has been successfully loaded. */
@DeveloperApi
public class GetJobEvent extends JobEvent {

  private final JobInfo jobInfo;

  /**
   * Constructs a new {@code GetJobEvent} instance.
   *
   * @param user The user who initiated the job retrieval operation.
   * @param metalake The metalake name where the job resides.
   * @param jobInfo The information of the job that has been loaded.
   */
  public GetJobEvent(String user, String metalake, JobInfo jobInfo) {
    super(user, NameIdentifierUtil.ofJob(metalake, jobInfo.jobId()));
    this.jobInfo = jobInfo;
  }

  /**
   * Returns the information of the job that has been loaded.
   *
   * @return the job information.
   */
  public JobInfo loadedJobInfo() {
    return jobInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_JOB;
  }
}
