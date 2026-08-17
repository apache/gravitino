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
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for secret properties. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SecretsResponse extends BaseResponse {

  @JsonProperty("secrets")
  private final Map<String, String> secrets;

  /**
   * Creates a new SecretsResponse.
   *
   * @param secrets The secret properties map.
   */
  public SecretsResponse(Map<String, String> secrets) {
    super(0);
    this.secrets = secrets;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * SecretsResponse.
   */
  public SecretsResponse() {
    super();
    this.secrets = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(secrets != null, "\"secrets\" must not be null");
  }
}
