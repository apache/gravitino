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

package org.apache.gravitino.authorization;

import java.util.Objects;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.PolicyAssociationSelector;

/** An internal policy binding paired with the target object's effective tag assignment. */
public final class ResolvedTagPolicy {
  private final TagEntity tag;
  private final PolicyEntity policy;
  private final PolicyAssociationSelector selector;

  /**
   * Creates a resolved binding.
   *
   * @param tag the effective tag at the target, not an overridden ancestor assignment
   * @param policy the bound policy
   * @param selector the non-null association selector
   */
  public ResolvedTagPolicy(TagEntity tag, PolicyEntity policy, PolicyAssociationSelector selector) {
    Objects.requireNonNull(tag, "tag must not be null");
    this.tag = tag.copyWithInherited(tag.inherited().orElse(false));
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.selector = Objects.requireNonNull(selector, "selector must not be null");
  }

  /**
   * Returns the effective tag, including its ID and assignment.
   *
   * @return the effective tag
   */
  public TagEntity tag() {
    return tag;
  }

  /**
   * Returns the bound policy, including its ID and content.
   *
   * @return the policy
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * Returns the selector stored on the policy-to-tag association.
   *
   * @return the selector
   */
  public PolicyAssociationSelector selector() {
    return selector;
  }
}
