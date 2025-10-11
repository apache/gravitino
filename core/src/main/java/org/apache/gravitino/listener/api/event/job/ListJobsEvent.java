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

/** Represents an event that is triggered upon the successful listing of jobs. */
@DeveloperApi
public final class ListJobsEvent extends JobEvent {

  private final Optional<String> jobTemplateName;

  /**
   * Constructs an instance of {@code ListJobsEvent}.
   *
   * @param user The username of the individual who initiated the job listing.
   * @param metalake The namespace from which jobs were listed.
   * @param jobTemplateName An optional job template name used to filter the listed jobs.
   */
  public ListJobsEvent(String user, String metalake, Optional<String> jobTemplateName) {
    super(user, NameIdentifier.of(metalake));
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
