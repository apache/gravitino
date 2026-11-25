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

import static org.apache.gravitino.authorization.TagPolicyTestUtils.customPolicy;
import static org.apache.gravitino.authorization.TagPolicyTestUtils.policy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.meta.PolicyEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests privilege and active-role matching without storage. */
class TestTagPolicyMatcher {
  @Test
  void testAnyActiveRoleCanMatchEveryListedPrivilege() {
    PolicyEntity matchedPolicy = accessPolicy(true);
    for (Privilege.Name privilege :
        Arrays.asList(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE)) {
      for (String role : Arrays.asList("analyst", "engineer")) {
        PolicyEntity match =
            TagPolicyMatcher.match(privilege, Set.of("unrelated", role), List.of(matchedPolicy))
                .orElseThrow();
        Assertions.assertSame(matchedPolicy, match);
        Assertions.assertEquals(10L, match.id());
      }
    }
  }

  @Test
  void testInactiveAndNoActiveRolesDoNotMatch() {
    List<PolicyEntity> policies = List.of(accessPolicy(true));
    Assertions.assertFalse(
        TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("unrelated"), policies)
            .isPresent());
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, ActiveRoles.none().roleNames(), policies)
            .isPresent());
  }

  @Test
  void testUnlistedAndNonConferrablePrivilegesDoNotMatch() {
    List<PolicyEntity> policies = List.of(accessPolicy(true));
    for (Privilege.Name privilege :
        Arrays.asList(
            Privilege.Name.READ_FILESET, Privilege.Name.USE_SCHEMA, Privilege.Name.MANAGE_GRANTS)) {
      Assertions.assertFalse(
          TagPolicyMatcher.match(privilege, Set.of("analyst"), policies).isPresent());
    }
  }

  @Test
  void testDisabledPoliciesAndAbsentBindingsDoNotMatch() {
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, Set.of("analyst"), List.of(accessPolicy(false)))
            .isPresent());
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, Set.of("analyst"), Collections.emptyList())
            .isPresent());
  }

  @Test
  void testNonmatchingPolicyDoesNotHideLaterMatch() {
    PolicyEntity nonmatch =
        policy(9L, true, List.of(Privilege.Name.SELECT_TABLE), List.of("unrelated"));
    PolicyEntity match = accessPolicy(true);
    List<PolicyEntity> policies = List.of(nonmatch, match);
    Set<String> roles = Set.of("analyst");
    Assertions.assertSame(
        match, TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, roles, policies).orElseThrow());
    Assertions.assertSame(
        match, TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, roles, policies).orElseThrow());
    Assertions.assertEquals(List.of(nonmatch, match), policies);
    Assertions.assertEquals(Set.of("analyst"), roles);
  }

  @Test
  void testGovernancePolicyCannotConferAccess() {
    PolicyEntity matchedPolicy = customPolicy();
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, Set.of("analyst"), List.of(matchedPolicy))
            .isPresent());
  }

  private static PolicyEntity accessPolicy(boolean enabled) {
    return policy(
        10L,
        enabled,
        List.of(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE),
        List.of("analyst", "engineer"));
  }
}
