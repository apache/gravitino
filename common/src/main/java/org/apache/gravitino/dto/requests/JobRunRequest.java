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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to run a job using a specified job template. */
@Getter
@EqualsAndHashCode
public class JobRunRequest implements RESTRequest {

  @JsonProperty("jobTemplateName")
  private final String jobTemplateName;

  @JsonProperty("jobConf")
  private final Map<String, String> jobConf;

  /**
   * Creates a new JobRunRequest with the specified job template name and job configuration.
   *
   * @param jobTemplateName The name of the job template to use for running the job.
   * @param jobConf A map containing the job configuration parameters.
   */
  public JobRunRequest(String jobTemplateName, Map<String, String> jobConf) {
    this.jobTemplateName = jobTemplateName;
    this.jobConf = jobConf;
  }

  /** Default constructor for Jackson deserialization. */
  private JobRunRequest() {
    this(null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobTemplateName),
        "\"jobTemplateName\" is required and cannot be empty");
  }
}
