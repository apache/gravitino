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
import static org.apache.gravitino.authorization.TagPolicyTestUtils.tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagAssignment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests privilege, active-role, and selector matching without storage. */
class TestTagPolicyMatcher {
  @Test
  void testAnyActiveRoleCanMatchEveryListedPrivilegeAndReturnsProvenance() {
    ResolvedTagPolicy binding = binding(true, TagAssignment.noValue(), AllValuesSelector.get());
    for (Privilege.Name privilege :
        Arrays.asList(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE)) {
      for (String role : Arrays.asList("analyst", "engineer")) {
        ResolvedTagPolicy match =
            TagPolicyMatcher.match(privilege, Set.of("unrelated", role), List.of(binding))
                .orElseThrow();
        Assertions.assertSame(binding, match);
        Assertions.assertEquals(1L, match.tag().id());
        Assertions.assertEquals(10L, match.policy().id());
      }
    }
  }

  @Test
  void testInactiveAndNoActiveRolesDoNotMatch() {
    List<ResolvedTagPolicy> bindings =
        List.of(binding(true, TagAssignment.noValue(), AllValuesSelector.get()));
    Assertions.assertFalse(
        TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("unrelated"), bindings)
            .isPresent());
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, ActiveRoles.none().roleNames(), bindings)
            .isPresent());
  }

  @Test
  void testUnlistedAndNonConferrablePrivilegesDoNotMatch() {
    List<ResolvedTagPolicy> bindings =
        List.of(binding(true, TagAssignment.noValue(), AllValuesSelector.get()));
    for (Privilege.Name privilege :
        Arrays.asList(
            Privilege.Name.READ_FILESET, Privilege.Name.USE_SCHEMA, Privilege.Name.MANAGE_GRANTS)) {
      Assertions.assertFalse(
          TagPolicyMatcher.match(privilege, Set.of("analyst"), bindings).isPresent());
    }
  }

  @Test
  void testValueSelectorMatchesAnyExactAssignmentValue() {
    ResolvedTagPolicy binding =
        binding(true, TagAssignment.ofValues("silver", "gold"), TagValueSelector.of("gold"));
    Assertions.assertTrue(
        TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("analyst"), List.of(binding))
            .isPresent());
    for (TagAssignment assignment :
        Arrays.asList(
            TagAssignment.noValue(),
            TagAssignment.ofValues("Gold"),
            TagAssignment.ofValues("golden"))) {
      Assertions.assertFalse(
          TagPolicyMatcher.match(
                  Privilege.Name.SELECT_TABLE,
                  Set.of("analyst"),
                  List.of(binding(true, assignment, TagValueSelector.of("gold"))))
              .isPresent());
    }
  }

  @Test
  void testPresenceSelectorAcceptsValuedAndValuelessAssignments() {
    for (TagAssignment assignment :
        Arrays.asList(TagAssignment.noValue(), TagAssignment.ofValues("bronze"))) {
      Assertions.assertTrue(
          TagPolicyMatcher.match(
                  Privilege.Name.SELECT_TABLE,
                  Set.of("analyst"),
                  List.of(binding(true, assignment, AllValuesSelector.get())))
              .isPresent());
    }
  }

  @Test
  void testDisabledPoliciesAndAbsentBindingsDoNotMatch() {
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE,
                Set.of("analyst"),
                List.of(binding(false, TagAssignment.noValue(), AllValuesSelector.get())))
            .isPresent());
    Assertions.assertFalse(
        TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, Set.of("analyst"), Collections.emptyList())
            .isPresent());
  }

  @Test
  void testNonmatchingPolicyDoesNotHideLaterMatch() {
    ResolvedTagPolicy nonmatch =
        binding(true, TagAssignment.ofValues("bronze"), TagValueSelector.of("gold"));
    ResolvedTagPolicy match =
        binding(true, TagAssignment.ofValues("bronze"), AllValuesSelector.get());
    List<ResolvedTagPolicy> bindings = List.of(nonmatch, match);
    Set<String> roles = Set.of("analyst");
    Assertions.assertSame(
        match, TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, roles, bindings).orElseThrow());
    Assertions.assertSame(
        match, TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, roles, bindings).orElseThrow());
    Assertions.assertEquals(List.of(nonmatch, match), bindings);
    Assertions.assertEquals(Set.of("analyst"), roles);
  }

  @Test
  void testUnknownSelectorFailsClosed() {
    ResolvedTagPolicy binding = binding(true, TagAssignment.noValue(), () -> "FUTURE_SELECTOR");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TagPolicyMatcher.match(
                Privilege.Name.SELECT_TABLE, Set.of("analyst"), List.of(binding)));
  }

  @Test
  void testGovernancePolicyCannotConferAccess() {
    ResolvedTagPolicy binding =
        new ResolvedTagPolicy(
            tag(1L, "certified", TagAssignment.noValue()), customPolicy(), AllValuesSelector.get());
    Assertions.assertFalse(
        TagPolicyMatcher.match(Privilege.Name.SELECT_TABLE, Set.of("analyst"), List.of(binding))
            .isPresent());
  }

  private static ResolvedTagPolicy binding(
      boolean enabled, TagAssignment assignment, PolicyAssociationSelector selector) {
    TagEntity tag = tag(1L, "certified", assignment);
    PolicyEntity policy =
        policy(
            10L,
            enabled,
            List.of(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE),
            List.of("analyst", "engineer"));
    return new ResolvedTagPolicy(tag, policy, selector);
  }
}
