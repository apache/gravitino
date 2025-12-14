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

/** Represents an event that is generated after a job template is successfully deleted. */
@DeveloperApi
public final class DeleteJobTemplateEvent extends JobTemplateEvent {
  private final boolean isExists;

  /**
   * Constructs a new {@code DeleteJobTemplateEvent} instance, encapsulating information about the
   * outcome of a job template delete operation.
   *
   * @param user The user who initiated the delete job template operation.
   * @param metalake The metalake from which the job template was deleted.
   * @param jobTemplateName The name of the job template.
   * @param isExists A boolean flag indicating whether the job template existed at the time of the
   *     delete operation.
   */
  public DeleteJobTemplateEvent(
      String user, String metalake, String jobTemplateName, boolean isExists) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName));
    this.isExists = isExists;
  }

  /**
   * Retrieves the existence status of the job template at the time of the delete operation.
   *
   * @return A boolean value indicating whether the job template existed. {@code true} if the job
   *     template existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.DELETE_JOB_TEMPLATE;
  }
}
