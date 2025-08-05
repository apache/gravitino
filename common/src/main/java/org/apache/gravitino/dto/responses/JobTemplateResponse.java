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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.dto.job.JobTemplateDTO;

/** Represents a response containing a single job template. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class JobTemplateResponse extends BaseResponse {

  @JsonProperty("jobTemplate")
  private final JobTemplateDTO jobTemplate;

  /**
   * Creates a new JobTemplateResponse with the specified job template.
   *
   * @param jobTemplate The job template to include in the response.
   */
  public JobTemplateResponse(JobTemplateDTO jobTemplate) {
    super(0);
    this.jobTemplate = jobTemplate;
  }

  /** Default constructor for Jackson deserialization. */
  private JobTemplateResponse() {
    super();
    this.jobTemplate = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(jobTemplate != null, "jobTemplate must not be null");
    jobTemplate.validate();
  }
}
