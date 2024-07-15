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
package com.apache.gravitino.dto.requests;

import com.apache.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to add a user. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UserAddRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  /** Default constructor for UserAddRequest. (Used for Jackson deserialization.) */
  public UserAddRequest() {
    this(null);
  }

  /**
   * Creates a new UserAddRequest.
   *
   * @param name The name of the user.
   */
  public UserAddRequest(String name) {
    super();
    this.name = name;
  }

  /**
   * Validates the {@link UserAddRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
