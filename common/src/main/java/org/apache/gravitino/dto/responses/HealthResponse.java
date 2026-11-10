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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.HealthCheckDTO;

/** Represents a response containing aggregate health status and per-check results. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class HealthResponse extends BaseResponse {

  @JsonProperty("status")
  private final HealthCheckDTO.Status status;

  @JsonProperty("checks")
  private final List<HealthCheckDTO> checks;

  /**
   * Constructor for HealthResponse.
   *
   * @param status aggregate health status
   * @param checks per-check results; null is coerced to empty list
   */
  public HealthResponse(HealthCheckDTO.Status status, List<HealthCheckDTO> checks) {
    super(0);
    this.status = status;
    this.checks = checks == null ? Collections.emptyList() : checks;
  }

  /** Default constructor for HealthResponse. (Used for Jackson deserialization.) */
  public HealthResponse() {
    super();
    this.status = null;
    this.checks = Collections.emptyList();
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if status or checks are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(status != null, "status must be non-null");
  }

  /**
   * Returns true if the aggregate status is UP.
   *
   * @return true if the aggregate status is UP, false otherwise
   */
  @JsonIgnore
  public boolean isUp() {
    return status == HealthCheckDTO.Status.UP;
  }
}
