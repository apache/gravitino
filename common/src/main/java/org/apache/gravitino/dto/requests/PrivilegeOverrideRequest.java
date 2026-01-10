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
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a role by overriding its securable objects. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class PrivilegeOverrideRequest implements RESTRequest {

  @JsonProperty("overrides")
  private SecurableObjectDTO[] overrides;

  /** Default constructor for PrivilegeOverrideRequest. (Used for Jackson deserialization.) */
  public PrivilegeOverrideRequest() {
    this(null);
  }

  /**
   * Creates a new PrivilegeOverrideRequest.
   *
   * @param overrides The securable objects to override for the role.
   */
  public PrivilegeOverrideRequest(SecurableObjectDTO[] overrides) {
    this.overrides = overrides;
  }

  /**
   * Validates the {@link PrivilegeOverrideRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    for (SecurableObjectDTO objectDTO : overrides) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(objectDTO.name()), "\"securable object name\" cannot be blank");
      Preconditions.checkArgument(
          objectDTO.type() != null, "\"securable object type\" cannot be null");
      Preconditions.checkArgument(
          objectDTO.privileges() != null && !objectDTO.privileges().isEmpty(),
          "\"securable object privileges\" cannot be null or empty");
    }
  }
}
