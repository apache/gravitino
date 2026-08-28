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
package org.apache.gravitino.listener.api.info;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.policy.PolicyAssociationSelector;

/** Describes one policy-to-tag association exposed to an event listener. */
@DeveloperApi
public final class PolicyTagAssociationInfo {
  private final String metalake;
  private final String tagName;
  private final String policyName;
  private final PolicyAssociationSelector selector;

  /**
   * Constructs a policy-to-tag association description.
   *
   * @param metalake The metalake containing the association.
   * @param tagName The associated tag name.
   * @param policyName The associated policy name.
   * @param selector The policy association selector.
   */
  public PolicyTagAssociationInfo(
      String metalake, String tagName, String policyName, PolicyAssociationSelector selector) {
    this.metalake = metalake;
    this.tagName = tagName;
    this.policyName = policyName;
    this.selector = selector;
  }

  /**
   * @return The metalake containing the association.
   */
  public String metalake() {
    return metalake;
  }

  /**
   * @return The associated tag name.
   */
  public String tagName() {
    return tagName;
  }

  /**
   * @return The associated policy name.
   */
  public String policyName() {
    return policyName;
  }

  /**
   * @return The policy association selector.
   */
  public PolicyAssociationSelector selector() {
    return selector;
  }
}
