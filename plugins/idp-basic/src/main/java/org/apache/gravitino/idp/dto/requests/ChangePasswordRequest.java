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

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.idp.basic.IdpCredentialValidator;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to change a built-in IdP user password. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class ChangePasswordRequest implements RESTRequest {

  @JsonProperty("newPassword")
  @ToString.Exclude
  private final String newPassword;

<<<<<<< HEAD
  /** Default constructor for ChangePasswordRequest. (Used for Jackson deserialization.) */
  public ChangePasswordRequest() {
    this(null);
  }

  /**
   * Creates a new ChangePasswordRequest.
=======
  @JsonProperty("oldPassword")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @ToString.Exclude
  private final String oldPassword;

  /** Default constructor for ChangePasswordRequest. (Used for Jackson deserialization.) */
  public ChangePasswordRequest() {
    this(null, null);
  }

  /**
   * Creates a ChangePasswordRequest for an admin password reset (no old password).
>>>>>>> d7e8b22ca ([#13925] improvement(idp-basic): Require currentPassword for self password change (#13926))
   *
   * @param newPassword The new password.
   */
<<<<<<< HEAD
  public ChangePasswordRequest(String password) {
    super();
    this.password = password;
=======
  public ChangePasswordRequest(String newPassword) {
    this(newPassword, null);
  }

  /**
   * Creates a ChangePasswordRequest.
   *
   * @param newPassword The new password.
   * @param oldPassword The caller's current password when changing their own password, or {@code
   *     null} when a service admin resets another user's password.
   */
  public ChangePasswordRequest(String newPassword, String oldPassword) {
    super();
    this.newPassword = newPassword;
    this.oldPassword = oldPassword;
>>>>>>> d7e8b22ca ([#13925] improvement(idp-basic): Require currentPassword for self password change (#13926))
  }

  /**
   * Validates the {@link ChangePasswordRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
<<<<<<< HEAD
    IdpCredentialValidator.validatePassword(password);
=======
    IdpCredentialValidator.validatePassword(newPassword, "newPassword");
>>>>>>> d7e8b22ca ([#13925] improvement(idp-basic): Require currentPassword for self password change (#13926))
  }
}
