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
import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for secret properties (secret keys resolved to plaintext). */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SecretPropertiesResponse extends BaseResponse {

  @JsonProperty("secretProperties")
  private final Map<String, String> secretProperties;

  /**
   * Creates a new SecretPropertiesResponse.
   *
   * @param secretProperties secret key to plaintext value map
   */
  public SecretPropertiesResponse(Map<String, String> secretProperties) {
    super(0);
    this.secretProperties = secretProperties;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * SecretPropertiesResponse.
   */
  public SecretPropertiesResponse() {
    super();
    this.secretProperties = Collections.emptyMap();
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(secretProperties != null, "\"secretProperties\" must not be null");
  }
}
