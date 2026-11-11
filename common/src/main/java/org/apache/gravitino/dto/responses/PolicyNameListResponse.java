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
import lombok.ToString;

/** Represents a response for a list of policy names. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class PolicyNameListResponse extends NameListResponse {

  @JsonProperty("hasInaccessiblePolicies")
  private final boolean hasInaccessiblePolicies;

  /**
   * Creates a new PolicyNameListResponse.
   *
   * @param names The list of accessible policy names.
   * @param hasInaccessiblePolicies Whether inaccessible policies were omitted from the response.
   */
  public PolicyNameListResponse(String[] names, boolean hasInaccessiblePolicies) {
    super(names);
    this.hasInaccessiblePolicies = hasInaccessiblePolicies;
  }

  /** Constructor used by Jackson deserializer. */
  public PolicyNameListResponse() {
    super();
    this.hasInaccessiblePolicies = false;
  }

  /**
   * Returns whether inaccessible policies were omitted from the response.
   *
   * @return true if at least one inaccessible policy was omitted, false otherwise
   */
  public boolean hasInaccessiblePolicies() {
    return hasInaccessiblePolicies;
  }
}
