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

/** Response wrapping a property map, used for resolved plaintext property delivery. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PropertyMapResponse extends BaseResponse {

  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for Jackson deserialization. */
  public PropertyMapResponse() {
    super(0);
    this.properties = null;
  }

  /**
   * Creates a response with the given properties.
   *
   * @param properties the property map
   */
  public PropertyMapResponse(Map<String, String> properties) {
    super(0);
    this.properties = properties;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(properties != null, "properties must not be null");
  }
}
