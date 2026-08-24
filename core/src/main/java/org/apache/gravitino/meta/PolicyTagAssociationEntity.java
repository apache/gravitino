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
package org.apache.gravitino.meta;

import com.google.common.base.Preconditions;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.PolicyTagSelector;

/** The core representation of a policy-to-tag association. */
public final class PolicyTagAssociationEntity {

  private final PolicyEntity policy;
  private final TagEntity tag;
  @Nullable private final PolicyTagSelector selector;

  private PolicyTagAssociationEntity(
      PolicyEntity policy, TagEntity tag, @Nullable PolicyTagSelector selector) {
    this.policy = policy;
    this.tag = tag;
    this.selector = selector;
  }

  /**
   * Creates a policy-to-tag association entity.
   *
   * @param policy The associated policy.
   * @param tag The associated tag.
   * @param selector The optional selector, or null for tag-presence matching.
   * @return The association entity.
   */
  public static PolicyTagAssociationEntity of(
      PolicyEntity policy, TagEntity tag, @Nullable PolicyTagSelector selector) {
    Preconditions.checkArgument(policy != null, "Policy cannot be null");
    Preconditions.checkArgument(tag != null, "Tag cannot be null");
    return new PolicyTagAssociationEntity(policy, tag, selector);
  }

  /**
   * Returns the associated policy entity.
   *
   * @return The associated policy entity.
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * Returns the associated tag entity.
   *
   * @return The associated tag entity.
   */
  public TagEntity tag() {
    return tag;
  }

  /**
   * Returns the optional tag selector.
   *
   * @return The selector, or an empty optional for tag-presence matching.
   */
  public Optional<PolicyTagSelector> selector() {
    return Optional.ofNullable(selector);
  }
}
