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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.AccessControlContent;
import org.apache.gravitino.policy.Policy;

/**
 * Matches requested privileges and active roles against policies matched by the object resolver.
 */
public final class TagPolicyMatcher {
  private TagPolicyMatcher() {}

  /**
   * Finds the first policy that confers a privilege on an effective active role.
   *
   * @param privilege the requested privilege
   * @param activeRoleNames the caller's held role names after active-role narrowing
   * @param policies the target's matched policies in deterministic order
   * @return the first matching policy, or empty
   */
  public static Optional<PolicyEntity> match(
      Privilege.Name privilege, Set<String> activeRoleNames, List<PolicyEntity> policies) {
    Objects.requireNonNull(privilege, "privilege must not be null");
    Objects.requireNonNull(activeRoleNames, "activeRoleNames must not be null");
    Objects.requireNonNull(policies, "policies must not be null");
    if (activeRoleNames.isEmpty()
        || !AccessControlContent.PERMITTED_PRIVILEGES.contains(privilege)) {
      return Optional.empty();
    }
    for (PolicyEntity policy : policies) {
      if (!policy.enabled() || policy.policyType() != Policy.BuiltInType.ACCESS_CONTROL) {
        continue;
      }
      AccessControlContent content = (AccessControlContent) policy.content();
      if (content.privileges().contains(privilege)
          && !Collections.disjoint(content.applicableRoles(), activeRoleNames)) {
        return Optional.of(policy);
      }
    }
    return Optional.empty();
  }
}
