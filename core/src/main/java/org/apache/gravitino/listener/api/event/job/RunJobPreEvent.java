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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before running a job. */
@DeveloperApi
public class RunJobPreEvent extends JobPreEvent {

  private final String jobTemplateName;
  private final Map<String, String> jobConf;

  /**
   * Constructs a new {@code RunJobPreEvent} instance.
   *
   * @param user The user who initiated the job run operation.
   * @param metalake The metalake name where the job will run.
   * @param jobTemplateName The name of the job template to be used for running the job.
   * @param jobConf The runtime configuration for the job.
   */
  public RunJobPreEvent(
      String user, String metalake, String jobTemplateName, Map<String, String> jobConf) {
    super(user, NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName));
    this.jobTemplateName = jobTemplateName;
    this.jobConf = ImmutableMap.copyOf(jobConf);
  }

  /**
   * Returns the name of the job template to be used for running the job.
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
