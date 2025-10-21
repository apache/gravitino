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

/**
 * Represents an event triggered when an attempt to alter a job template in the database fails due
 * to an exception.
 */
@DeveloperApi
public class AlterJobTemplateFailureEvent extends JobTemplateFailureEvent {
  private final JobTemplateChange[] changes;

  /**
   * Constructs a new AlterJobTemplateFailureEvent.
   *
   * @param user the user who attempted to alter the job template
   * @param metalake the metalake identifier
   * @param jobTemplateName the name of the job template
   * @param changes the changes attempted to be made to the job template
   * @param exception the exception that caused the failure
   */
  public AlterJobTemplateFailureEvent(
      String user,
      String metalake,
      String jobTemplateName,
      JobTemplateChange[] changes,
      Exception exception) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName), exception);
    this.changes = changes;
  }

  /**
   * Returns the changes attempted to be made to the job template.
   *
   * @return the changes attempted to be made to the job template
   */
  public JobTemplateChange[] changes() {
    return changes;
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
