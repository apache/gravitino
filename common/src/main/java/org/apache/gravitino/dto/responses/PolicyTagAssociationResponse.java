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
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.policy.PolicyTagSelectorDTO;

/** Response for creating or replacing one policy-to-tag association. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PolicyTagAssociationResponse extends BaseResponse {

  @JsonProperty("policy")
  private final String policy;

  @JsonProperty("tag")
  private final String tag;

  @JsonProperty("selector")
  @Nullable
  private final PolicyTagSelectorDTO selector;

  /**
   * Creates a response.
   *
   * @param policy The policy name.
   * @param tag The tag name.
   * @param selector The selector, or null for tag-presence matching.
   */
  public PolicyTagAssociationResponse(
      String policy, String tag, @Nullable PolicyTagSelectorDTO selector) {
    this.policy = policy;
    this.tag = tag;
    this.selector = selector;
  }

  private PolicyTagAssociationResponse() {
    this(null, null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(StringUtils.isNotBlank(policy), "policy must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(tag), "tag must not be blank");
    if (selector != null) {
      selector.validate();
    }
  }
}
