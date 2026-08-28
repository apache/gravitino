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
package org.apache.gravitino.listener.api.event;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.PolicyTagAssociationInfo;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before adding a policy to a tag. */
@DeveloperApi
public final class AddPolicyForTagPreEvent extends TagPreEvent {
  private final String metalake;
  private final String tagName;
  private final String policyName;
  @Nullable private final PolicyTagAssociationInfo previousAssociation;
  private final PolicyAssociationSelector requestedSelector;

  /**
   * Constructs an event triggered before adding a policy to a tag.
   *
   * @param user The user who initiated the operation.
   * @param metalake The metalake containing the tag and policy.
   * @param tagName The tag name.
   * @param policyName The policy name.
   * @param previousAssociation The previous association, or null if no association exists.
   * @param requestedSelector The requested policy association selector.
   */
  public AddPolicyForTagPreEvent(
      String user,
      String metalake,
      String tagName,
      String policyName,
      @Nullable PolicyTagAssociationInfo previousAssociation,
      PolicyAssociationSelector requestedSelector) {
    super(user, NameIdentifierUtil.ofTag(metalake, tagName));
    this.metalake = metalake;
    this.tagName = tagName;
    this.policyName = policyName;
    this.previousAssociation = previousAssociation;
    this.requestedSelector = requestedSelector;
  }

  /**
   * @return The metalake containing the tag and policy.
   */
  public String metalake() {
    return metalake;
  }

  /**
   * @return The tag name.
   */
  public String tagName() {
    return tagName;
  }

  /**
   * @return The policy name.
   */
  public String policyName() {
    return policyName;
  }

  /**
   * @return The previous association, or empty when no association exists.
   */
  public Optional<PolicyTagAssociationInfo> previousAssociation() {
    return Optional.ofNullable(previousAssociation);
  }

  /**
   * @return The previous selector, or empty when no previous association exists.
   */
  public Optional<PolicyAssociationSelector> previousSelector() {
    return previousAssociation == null
        ? Optional.empty()
        : Optional.of(previousAssociation.selector());
  }

  /**
   * @return The requested policy association selector.
   */
  public PolicyAssociationSelector requestedSelector() {
    return requestedSelector;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ADD_POLICY_FOR_TAG;
  }
}
