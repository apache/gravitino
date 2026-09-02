/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.idp.basic.IdpCredentialValidator;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a built-in IdP user password and/or enabled flag. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UpdateUserRequest implements RESTRequest {

  @JsonProperty("password")
  @ToString.Exclude
  private final String password;

  @JsonProperty("enabled")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Boolean enabled;

  /** Default constructor for UpdateUserRequest. (Used for Jackson deserialization.) */
  public UpdateUserRequest() {
    this(null, null);
  }

  /**
   * Creates a new UpdateUserRequest that updates only the password.
   *
   * @param password The new password of the built-in IdP user.
   */
  public UpdateUserRequest(String password) {
    this(password, null);
  }

  /**
   * Creates a new UpdateUserRequest.
   *
   * @param password The new password of the built-in IdP user, or {@code null} to leave it
   *     unchanged.
   * @param enabled Whether the built-in IdP user should be enabled, or {@code null} to leave it
   *     unchanged.
   */
  public UpdateUserRequest(String password, Boolean enabled) {
    super();
    this.password = password;
    this.enabled = enabled;
  }

  /**
   * Validates the {@link UpdateUserRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        password != null || enabled != null, "\"password\" or \"enabled\" field is required");
    if (password != null) {
      IdpCredentialValidator.validatePassword(password);
    }
  }
}
