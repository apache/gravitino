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
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before altering a job template. */
@DeveloperApi
public class AlterJobTemplatePreEvent extends JobTemplatePreEvent {

  private final JobTemplateChange[] changes;

  /**
   * Constructs a new AlterJobTemplatePreEvent instance.
   *
   * @param user The user responsible for the operation.
   * @param metalake The namespace of the job template.
   * @param jobTemplateName The name of the job template being altered.
   * @param changes The changes being applied to the job template.
   */
  public AlterJobTemplatePreEvent(
      String user, String metalake, String jobTemplateName, JobTemplateChange[] changes) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName));
    this.changes = changes;
  }

  /**
   * Returns the changes being applied to the job template.
   *
   * @return An array of {@link JobTemplateChange}.
   */
  public JobTemplateChange[] changes() {
    return changes;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return The operation type {@link OperationType#ALTER_JOB_TEMPLATE}.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_JOB_TEMPLATE;
  }
}
