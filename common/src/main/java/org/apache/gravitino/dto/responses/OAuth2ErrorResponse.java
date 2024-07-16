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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.rest.RESTResponse;

/** Represents the response of an OAuth2 error. */
@Getter
@EqualsAndHashCode
public class OAuth2ErrorResponse implements RESTResponse {
  @JsonProperty("error")
  private String type;

  @Nullable
  @JsonProperty("error_description")
  private String message;

  /**
   * Creates a new OAuth2ErrorResponse.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   */
  public OAuth2ErrorResponse(String type, String message) {
    this.type = type;
    this.message = message;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public OAuth2ErrorResponse() {}

  /**
   * Validates the OAuth2ErrorResponse.
   *
   * @throws IllegalArgumentException if the OAuth2ErrorResponse is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(type != null, "OAuthErrorResponse should contain type");
  }
}
