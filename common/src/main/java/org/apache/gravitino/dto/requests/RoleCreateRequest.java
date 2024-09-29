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
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a role. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class RoleCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("securableObjects")
  private SecurableObjectDTO[] securableObjects;

  /** Default constructor for RoleCreateRequest. (Used for Jackson deserialization.) */
  public RoleCreateRequest() {
    this(null, null, null);
  }

  /**
   * Creates a new RoleCreateRequest.
   *
   * @param name The name of the role.
   * @param properties The properties of the role.
   * @param securableObjects The securable objects of the role.
   */
  public RoleCreateRequest(
      String name, Map<String, String> properties, SecurableObjectDTO[] securableObjects) {
    super();
    this.name = name;
    this.properties = properties;
    this.securableObjects = securableObjects;
  }

  /**
   * Validates the {@link RoleCreateRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(securableObjects != null, "\"securableObjects\" can't null ");
  }
}
