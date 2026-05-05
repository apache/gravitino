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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a built-in IdP user. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class CreateUserRequest implements RESTRequest {

  @JsonProperty("user")
  private final String user;

  @JsonProperty("password")
  private final String password;

  /** Default constructor for CreateUserRequest. (Used for Jackson deserialization.) */
  public CreateUserRequest() {
    this(null, null);
  }

  /**
   * Creates a new CreateUserRequest.
   *
   * @param user The user name of the built-in IdP user.
   * @param password The password of the built-in IdP user.
   */
  public CreateUserRequest(String user, String password) {
    super();
    this.user = user;
    this.password = password;
  }

  /**
   * Validates the {@link CreateUserRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(user), "\"user\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(password), "\"password\" field is required and cannot be empty");
  }
}
