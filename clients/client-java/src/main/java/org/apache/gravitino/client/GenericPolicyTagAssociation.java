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
package org.apache.gravitino.client;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyTagAssociation;
import org.apache.gravitino.policy.PolicyTagSelector;
import org.apache.gravitino.tag.Tag;

/** Generic client representation of a policy-to-tag association. */
final class GenericPolicyTagAssociation implements PolicyTagAssociation {

  private final Policy policy;
  private final Tag tag;
  @Nullable private final PolicyTagSelector selector;

  GenericPolicyTagAssociation(Policy policy, Tag tag, @Nullable PolicyTagSelector selector) {
    this.policy = policy;
    this.tag = tag;
    this.selector = selector;
  }

  @Override
  public Policy policy() {
    return policy;
  }

  @Override
  public Tag tag() {
    return tag;
  }

  @Override
  public Optional<PolicyTagSelector> selector() {
    return Optional.ofNullable(selector);
  }
}
