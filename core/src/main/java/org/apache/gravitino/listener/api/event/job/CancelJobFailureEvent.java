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
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered when cancelling a job has failed. */
@DeveloperApi
public class CancelJobFailureEvent extends JobFailureEvent {

  /**
   * Constructs a new {@code CancelJobFailureEvent} instance.
   *
   * @param user The user who initiated the job cancellation operation.
   * @param metalake The metalake name where the job resides.
   * @param jobId The ID of the job that failed to be cancelled.
   * @param exception The exception encountered during the job cancellation operation, providing
   *     insights into the reasons behind the failure.
   */
  public CancelJobFailureEvent(String user, String metalake, String jobId, Exception exception) {
    super(user, NameIdentifierUtil.ofJob(metalake, jobId), exception);
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.CANCEL_JOB;
  }
}
