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
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.dto.policy.PolicyAssociationSelectorDTO;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.rest.RESTRequest;

/** Request to add one policy-to-tag association. */
@ToString
@EqualsAndHashCode
public class PolicyTagAddRequest implements RESTRequest {

  @JsonProperty("selector")
  @Nullable
  private PolicyAssociationSelectorDTO selector;

  /** Creates an empty request for Jackson deserialization. */
  public PolicyTagAddRequest() {}

  /**
   * Creates a request with a selector.
   *
   * @param selector The policy association selector.
   */
  public PolicyTagAddRequest(PolicyAssociationSelectorDTO selector) {
    this.selector = selector;
  }

  /**
   * @return The policy association selector.
   */
  public PolicyAssociationSelector selector() {
    Preconditions.checkArgument(selector != null, "selector must not be null");
    return selector.toSelector();
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(selector != null, "selector must not be null");
    selector.validate();
  }
}
