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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Response for the authenticated principal and service-administrator status. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class AuthMeResponse extends BaseResponse {

  @JsonProperty("principal")
  private final String principal;

  @JsonProperty("serviceAdmin")
  private final boolean serviceAdmin;

  /**
   * Constructor for AuthMeResponse.
   *
   * @param principal The server-resolved principal name of the authenticated user.
   */
  public AuthMeResponse(String principal) {
    this(principal, false);
  }

  /**
   * Constructor for AuthMeResponse.
   *
   * @param principal The server-resolved principal name of the authenticated user.
   * @param serviceAdmin Whether the authenticated user is a service administrator.
   */
  public AuthMeResponse(String principal, boolean serviceAdmin) {
    super(0);
    this.principal = principal;
    this.serviceAdmin = serviceAdmin;
  }

  /** Default constructor for AuthMeResponse. (Used for Jackson deserialization.) */
  public AuthMeResponse() {
    super();
    this.principal = null;
    this.serviceAdmin = false;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
  }
}
