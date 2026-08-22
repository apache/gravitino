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
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.dto.policy.PolicyTagSelectorDTO;
import org.apache.gravitino.policy.PolicyTagSelector;
import org.apache.gravitino.rest.RESTRequest;

/** Request to create or replace one policy-to-tag association. */
@ToString
@EqualsAndHashCode
public class PolicyTagSetRequest implements RESTRequest {

  @JsonProperty("selector")
  @Nullable
  private PolicyTagSelectorDTO selector;

  /** Creates an empty request that matches by tag presence. */
  public PolicyTagSetRequest() {}

  /**
   * Creates a request with an optional selector.
   *
   * @param selector The selector, or null for tag-presence matching.
   */
  public PolicyTagSetRequest(@Nullable PolicyTagSelectorDTO selector) {
    this.selector = selector;
  }

  /**
   * @return The selector, or empty for tag-presence matching.
   */
  public Optional<PolicyTagSelector> selector() {
    return Optional.ofNullable(selector).map(PolicyTagSelectorDTO::toSelector);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    if (selector != null) {
      selector.validate();
    }
  }
}
