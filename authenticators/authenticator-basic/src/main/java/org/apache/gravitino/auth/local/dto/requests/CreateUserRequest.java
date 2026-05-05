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

package org.apache.gravitino.auth.local.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Request for creating a built-in authentication user. */
public class CreateUserRequest implements RESTRequest {

  @JsonProperty("user")
  private String user;

  @JsonProperty("password")
  private String password;

  public CreateUserRequest() {}

  public CreateUserRequest(String user, String password) {
    this.user = user;
    this.password = password;
  }

  public String user() {
    return user;
  }

  public String password() {
    return password;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(user), "\"user\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(password), "\"password\" field is required and cannot be empty");
  }
}
