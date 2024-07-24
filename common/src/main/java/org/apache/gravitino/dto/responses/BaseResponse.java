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
import lombok.ToString;
import org.apache.gravitino.rest.RESTResponse;

/** Represents a base response for REST API calls. */
@Getter
@EqualsAndHashCode
@ToString
public class BaseResponse implements RESTResponse {

  @JsonProperty("code")
  private final int code;

  /**
   * Constructor for BaseResponse.
   *
   * @param code The response code.
   */
  public BaseResponse(int code) {
    this.code = code;
  }

  /** Default constructor for BaseResponse. (Used for Jackson deserialization.) */
  public BaseResponse() {
    this.code = 0;
  }

  /**
   * Validates the response code.
   *
   * @throws IllegalArgumentException if code value is negative.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(code >= 0, "code must be >= 0");
  }
}
