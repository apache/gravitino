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
import java.util.Arrays;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to add multiple users. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class BulkUserAddRequest implements RESTRequest {

  @JsonProperty("users")
  private final UserAddRequest[] users;

  /** Default constructor for BulkUserAddRequest. (Used for Jackson deserialization.) */
  public BulkUserAddRequest() {
    this(null);
  }

  /**
   * Creates a new BulkUserAddRequest.
   *
   * @param users The users to add.
   */
  public BulkUserAddRequest(UserAddRequest[] users) {
    this.users = users;
  }

  /**
   * Validates the {@link BulkUserAddRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(users != null, "\"users\" field is required");
    Preconditions.checkArgument(users.length > 0, "\"users\" field cannot be empty");
    Arrays.stream(users)
        .forEach(
            user -> {
              Preconditions.checkArgument(user != null, "\"users\" cannot contain null item");
              user.validate();
            });
    BulkRequestValidator.checkNoDuplicateNames("users", users);
    BulkRequestValidator.checkNoDuplicateExternalIds("users", users);
  }
}
