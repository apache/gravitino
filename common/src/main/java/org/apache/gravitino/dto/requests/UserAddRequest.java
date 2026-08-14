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
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to add a user. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UserAddRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("externalId")
  private final String externalId;

  @Nullable
  @JsonProperty("enabled")
  private final Boolean enabled;

  /** Default constructor for UserAddRequest. (Used for Jackson deserialization.) */
  public UserAddRequest() {
    this(null, null, null);
  }

  /**
   * Creates a new UserAddRequest.
   *
   * @param name The name of the user.
   */
  public UserAddRequest(String name) {
    this(name, null, null);
  }

  /**
   * Creates a new UserAddRequest.
   *
   * @param name The name of the user.
   * @param externalId The external identifier of the user.
   * @param enabled Whether the user is enabled.
   */
  public UserAddRequest(String name, String externalId, Boolean enabled) {
    super();
    this.name = name;
    this.externalId = externalId;
    this.enabled = enabled;
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
    if (externalId != null) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(externalId), "\"externalId\" field cannot be blank when provided");
    }
  }
}
