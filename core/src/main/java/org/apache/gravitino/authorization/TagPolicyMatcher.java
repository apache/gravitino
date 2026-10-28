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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.policy.AccessControlContent;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagAssignment;

/** Matches requested privileges and active roles against resolved tag-policy bindings. */
public final class TagPolicyMatcher {
  private TagPolicyMatcher() {}

  /**
   * Finds the first binding that confers a privilege on an effective active role.
   *
   * @param privilege the requested privilege
   * @param activeRoleNames the caller's held role names after active-role narrowing
   * @param bindings the target's resolved bindings in deterministic order
   * @return the first matching binding, carrying the tag and policy IDs, or empty
   * @throws IllegalArgumentException if a relevant binding has an unsupported selector
   */
  public static Optional<ResolvedTagPolicy> match(
      Privilege.Name privilege, Set<String> activeRoleNames, List<ResolvedTagPolicy> bindings) {
    Objects.requireNonNull(privilege, "privilege must not be null");
    Objects.requireNonNull(activeRoleNames, "activeRoleNames must not be null");
    Objects.requireNonNull(bindings, "bindings must not be null");
    if (activeRoleNames.isEmpty()
        || !AccessControlContent.PERMITTED_PRIVILEGES.contains(privilege)) {
      return Optional.empty();
    }
    for (ResolvedTagPolicy binding : bindings) {
      if (!binding.policy().enabled()
          || binding.policy().policyType() != Policy.BuiltInType.ACCESS_CONTROL) {
        continue;
      }
      AccessControlContent content = (AccessControlContent) binding.policy().content();
      if (content.privileges().contains(privilege)
          && !Collections.disjoint(content.applicableRoles(), activeRoleNames)
          && matches(
              binding.selector(), binding.tag().assignment().orElseGet(TagAssignment::noValue))) {
        return Optional.of(binding);
      }
    }
    return Optional.empty();
  }

  private static boolean matches(PolicyAssociationSelector selector, TagAssignment assignment) {
    if (selector instanceof AllValuesSelector) {
      return true;
    }
    if (selector instanceof TagValueSelector) {
      return Arrays.asList(assignment.values()).contains(((TagValueSelector) selector).value());
    }
    throw new IllegalArgumentException(
        "Unsupported policy association selector type: " + selector.type());
  }
}
