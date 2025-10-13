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
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before the registration of a job template. */
@DeveloperApi
public class RegisterJobTemplatePreEvent extends JobTemplatePreEvent {

  private final JobTemplate jobTemplate;

  /**
   * Constructs a new {@code RegisterJobTemplatePreEvent} instance.
   *
   * @param user The user who initiated the job template registration operation.
   * @param metalake The metalake name where the job template resides.
   * @param jobTemplate The job template to be registered.
   */
  public RegisterJobTemplatePreEvent(String user, String metalake, JobTemplate jobTemplate) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplate.name()));
    this.jobTemplate = jobTemplate;
  }

  /**
   * Returns the job template to be registered.
   *
   * @return the job template
   */
  public JobTemplate jobTemplate() {
    return jobTemplate;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_JOB_TEMPLATE;
  }
}
