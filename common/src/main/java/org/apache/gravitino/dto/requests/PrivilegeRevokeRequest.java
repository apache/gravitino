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
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Request to revoke a privilege from a role. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class PrivilegeRevokeRequest implements RESTRequest {

  @JsonProperty("privileges")
  private final List<PrivilegeDTO> privileges;

  /**
   * Constructor for privilegeRevokeRequest.
   *
   * @param privileges The privileges for the PrivilegeRevokeRequest.
   */
  public PrivilegeRevokeRequest(List<PrivilegeDTO> privileges) {
    this.privileges = privileges;
  }

  /** Default constructor for PrivilegeRevokeRequest. */
  public PrivilegeRevokeRequest() {
    this(null);
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the privileges field is not set or empty.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        privileges != null && !privileges.isEmpty(),
        "\"privileges\" field is required and cannot be empty");
  }
}
