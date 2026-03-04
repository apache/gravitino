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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered upon the successful alteration of a job template. */
@DeveloperApi
public final class AlterJobTemplateEvent extends JobTemplateEvent {
  private final JobTemplate updatedJobTemplate;
  private final JobTemplateChange[] jobTemplateChanges;

  /**
   * Constructs an instance of {@code AlterJobTemplateEvent}, encapsulating the key details about
   * the successful alteration of a job template.
   *
   * @param user The username of the individual responsible for initiating the job template
   *     alteration.
   * @param metalake The metalake from which the job template is being altered.
   * @param jobTemplateChanges An array of {@link JobTemplateChange} objects representing the
   *     specific changes applied to the job template during the alteration process.
   * @param updatedJobTemplate The post-alteration state of the job template.
   */
  public AlterJobTemplateEvent(
      String user,
      String metalake,
      JobTemplateChange[] jobTemplateChanges,
      JobTemplate updatedJobTemplate) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, updatedJobTemplate.name()));
    this.jobTemplateChanges = jobTemplateChanges;
    this.updatedJobTemplate = updatedJobTemplate;
  }

  /**
   * Retrieves the final state of the job template as it was returned to the user after successful
   * alteration.
   *
   * @return A {@link JobTemplate} instance encapsulating the comprehensive details of the newly
   *     altered job template.
   */
  public JobTemplate updatedJobTemplate() {
    return updatedJobTemplate;
  }

  /**
   * Retrieves the specific changes that were made to the job template during the alteration
   * process.
   *
   * @return An array of {@link JobTemplateChange} objects detailing each modification applied to
   *     the job template.
   */
  @Nullable
  public JobTemplateChange[] changes() {
    return jobTemplateChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_JOB_TEMPLATE;
  }
}
