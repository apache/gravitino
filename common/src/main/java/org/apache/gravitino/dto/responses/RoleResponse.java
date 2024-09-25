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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.authorization.RoleDTO;

/** Represents a response for a role. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class RoleResponse extends BaseResponse {

  @JsonProperty("role")
  private final RoleDTO role;

  /**
   * Constructor for RoleResponse.
   *
   * @param role The role data transfer object.
   */
  public RoleResponse(RoleDTO role) {
    super(0);
    this.role = role;
  }

  /** Default constructor for RoleResponse. (Used for Jackson deserialization.) */
  public RoleResponse() {
    super();
    this.role = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the name or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(role != null, "role must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(role.name()), "role 'name' must not be null and empty");
    Preconditions.checkArgument(role.auditInfo() != null, "role 'auditInfo' must not be null");
    Preconditions.checkArgument(
        role.securableObjects() != null, "role 'securable objects' can't null");
  }
}
