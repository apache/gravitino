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
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.dto.job.JobDTO;

/** Represents a response containing a list of jobs. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class JobListResponse extends BaseResponse {

  @JsonProperty("jobs")
  private final List<JobDTO> jobs;

  @JsonProperty("statusCounts")
  private final Map<String, Long> statusCounts;

  /**
   * Creates a new JobListResponse with the specified list of jobs and no per-status counts.
   *
   * @param jobs The list of jobs to include in the response.
   */
  public JobListResponse(List<JobDTO> jobs) {
    this(jobs, null);
  }

  /**
   * Creates a new JobListResponse with the specified list of jobs and per-status counts.
   *
   * @param jobs The list of jobs to include in the response.
   * @param statusCounts The number of jobs in {@code jobs}, keyed by lower-case status name (e.g.
   *     "queued", "started"), with every {@link org.apache.gravitino.job.JobHandle.Status} value
   *     present even when its count is zero. May be {@code null} when deserialized from an older
   *     server that predates this field.
   */
  public JobListResponse(List<JobDTO> jobs, Map<String, Long> statusCounts) {
    super(0);
    this.jobs = jobs;
    this.statusCounts = statusCounts;
  }

  /** Default constructor for Jackson deserialization. */
  private JobListResponse() {
    this(null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(jobs != null, "\"jobs\" must not be null");
    jobs.forEach(JobDTO::validate);
    // statusCounts is intentionally not required: an older server that predates this field
    // won't include it, and a new client must still be able to talk to it.
  }
}
