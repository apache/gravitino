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
package org.apache.gravitino.dto.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** A policy and selector returned while listing associations for a tag. */
@Getter
@ToString
@EqualsAndHashCode
public class PolicyForTagAssociationDTO {

  @JsonProperty("policy")
  private final PolicyDTO policy;

  @JsonProperty("selector")
  @Nullable
  private final PolicyTagSelectorDTO selector;

  /**
   * Creates a policy association DTO.
   *
   * @param policy The associated policy.
   * @param selector The selector, or null for tag-presence matching.
   */
  public PolicyForTagAssociationDTO(PolicyDTO policy, @Nullable PolicyTagSelectorDTO selector) {
    this.policy = policy;
    this.selector = selector;
  }

  private PolicyForTagAssociationDTO() {
    this(null, null);
  }
}
