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

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered when running a job has failed. */
@DeveloperApi
public class RunJobFailureEvent extends JobFailureEvent {

  private final String jobTemplateName;
  private final Map<String, String> jobConf;

  /**
   * Constructs a new {@code RunJobFailureEvent} instance.
   *
   * @param user The user who initiated the job run operation.
   * @param metalake The metalake name where the job was attempted to run.
   * @param jobTemplateName The name of the job template that was used for running the job.
   * @param jobConf The runtime configuration for the job.
   * @param exception The exception encountered during the job run operation, providing insights
   *     into the reasons behind the failure.
   */
  public RunJobFailureEvent(
      String user,
      String metalake,
      String jobTemplateName,
      Map<String, String> jobConf,
      Exception exception) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName), exception);
    this.jobTemplateName = jobTemplateName;
    this.jobConf = jobConf;
  }

  /**
   * Returns the name of the job template that was used for running the job.
   *
   * @return the job template name.
   */
  public String jobTemplateName() {
    return jobTemplateName;
  }

  /**
   * Returns the runtime configuration for the job.
   *
   * @return the job configuration.
   */
  public Map<String, String> jobConf() {
    return jobConf;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.RUN_JOB;
  }
}
