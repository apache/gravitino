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

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Represents an event triggered when an attempt to list jobs fails due to an exception. */
@DeveloperApi
public class ListJobsFailureEvent extends JobFailureEvent {

  private final Optional<String> jobTemplateName;

  /**
   * Constructs a new {@code ListJobsFailureEvent} instance.
   *
   * @param user The user who initiated the job listing operation.
   * @param metalake The metalake name where the jobs are being listed.
   * @param jobTemplateName The optional name of the job template used to filter the listed jobs.
   * @param exception The exception encountered during the job listing operation, providing insights
   *     into the reasons behind the failure.
   */
  public ListJobsFailureEvent(
      String user, String metalake, Optional<String> jobTemplateName, Exception exception) {
    super(user, NameIdentifier.of(metalake), exception);
    this.jobTemplateName = jobTemplateName;
  }

  /**
   * Returns the optional job template name used to filter the listed jobs.
   *
   * @return an {@code Optional} containing the job template name if provided, otherwise an empty
   *     {@code Optional}.
   */
  public Optional<String> jobTemplateName() {
    return jobTemplateName;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_JOBS;
  }
}
