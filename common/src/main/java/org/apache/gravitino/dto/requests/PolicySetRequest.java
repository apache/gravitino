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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to set a policy in use. */
@Getter
@EqualsAndHashCode
@ToString
public class PolicySetRequest implements RESTRequest {

  @JsonProperty("enable")
  private final boolean enable;

  /** Default constructor for PolicySetRequest. */
  public PolicySetRequest() {
    this(false);
  }

  /**
   * Constructor for PolicySetRequest.
   *
   * @param enable The enable status to set.
   */
  public PolicySetRequest(boolean enable) {
    this.enable = enable;
  }

  /**
   * Validates the request. No validation needed.
   *
   * @throws IllegalArgumentException If the request is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    // No validation needed
  }
}
