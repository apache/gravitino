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
package org.apache.gravitino.server.authorization.jcasbin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.junit.jupiter.api.Test;

/** Tests effective policy union, role activation and validation watermarks. */
public class TestRequestRolePolicies {
  private static final PolicyKey KEY = new PolicyKey("TABLE", 42L, "SELECT_TABLE");

  /** Named activation cannot mix an old role name with a newer policy index. */
  @Test
  public void testNamedActivationRequiresMatchingVersion() {
    Map<Long, CachedRolePolicies> roles =
        Map.of(1L, new CachedRolePolicies(2L, Map.of(KEY, Effect.ALLOW)));
    ActiveRoles active = ActiveRoles.of(Set.of("activeRole"));
    RequestRolePolicies olderName =
        new RequestRolePolicies(
            0L, roles, active, Map.of(1L, new RoleUpdatedAt(1L, "activeRole", 1L, 1L)));
    assertFalse(olderName.evaluate(KEY, true));
    RequestRolePolicies currentName =
        new RequestRolePolicies(
            0L, roles, active, Map.of(1L, new RoleUpdatedAt(1L, "activeRole", 2L, 2L)));
    assertTrue(currentName.evaluate(KEY, true));
    RequestRolePolicies unheld =
        new RequestRolePolicies(
            0L,
            roles,
            ActiveRoles.of(Set.of("unheldRole")),
            Map.of(
                1L,
                new RoleUpdatedAt(1L, "activeRole", 2L, 2L),
                2L,
                new RoleUpdatedAt(2L, "unheldRole", 2L, 2L)));
    assertFalse(unheld.evaluate(KEY, true));
  }

  /** Inactive roles still contribute global denies, including with no active roles. */
  @Test
  public void testDenyUnionIsIndependentOfActivation() {
    Map<Long, CachedRolePolicies> roles =
        Map.of(
            1L, new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW)),
            2L, new CachedRolePolicies(1L, Map.of(KEY, Effect.DENY)));
    for (ActiveRoles active :
        new ActiveRoles[] {
          ActiveRoles.all(), ActiveRoles.none(), ActiveRoles.of(Set.of("allowRole"))
        }) {
      RequestRolePolicies view =
          new RequestRolePolicies(
              0L,
              roles,
              active,
              Map.of(
                  1L,
                  new RoleUpdatedAt(1L, "allowRole", 1L, 1L),
                  2L,
                  new RoleUpdatedAt(2L, "denyRole", 1L, 1L)));
      assertFalse(view.evaluate(KEY, true));
      assertTrue(view.evaluate(KEY, false));
      assertTrue(view.hasDeny(Set.of("SELECT_TABLE")));
      assertFalse(view.hasDeny(Set.of("MODIFY_TABLE")));
    }
  }

  /** The effective view retains its data after source maps are changed or discarded. */
  @Test
  public void testViewPinsImmutablePolicyData() {
    Map<PolicyKey, Effect> index = new HashMap<>(Map.of(KEY, Effect.DENY));
    Map<Long, CachedRolePolicies> roles =
        new HashMap<>(Map.of(1L, new CachedRolePolicies(1L, index, false, Set.of("MODIFY_TABLE"))));
    RequestRolePolicies view =
        new RequestRolePolicies(0L, roles, ActiveRoles.all(), Collections.emptyMap());
    index.clear();
    roles.clear();
    assertTrue(view.evaluate(KEY, false));
    assertTrue(view.hasDeny(Set.of("MODIFY_TABLE")));
  }

  /** A single-object request pins a large role without traversing or copying its policies. */
  @Test
  public void testLargeRoleDoesNotScanPoliciesWhenCreatingView() {
    Map<PolicyKey, Effect> index = new HashMap<>();
    for (int i = 0; i < 10_000; i++) {
      index.put(new PolicyKey("TABLE", i, "SELECT_TABLE"), Effect.ALLOW);
    }
    CachedRolePolicies role = spy(new CachedRolePolicies(1L, index));
    RequestRolePolicies view =
        new RequestRolePolicies(0L, Map.of(1L, role), ActiveRoles.all(), Collections.emptyMap());
    verify(role, never()).getIndex();
    index.clear();
    assertTrue(view.evaluate(KEY, true));
    assertFalse(view.evaluate(KEY, false));
    assertFalse(view.evaluate(new PolicyKey("TABLE", 10_000L, "SELECT_TABLE"), true));
  }

  /** Repeated probes reuse the merged index without dropping an inactive role's DENY. */
  @Test
  public void testRepeatedProbesReuseMergedPolicies() {
    PolicyKey denied = new PolicyKey("TABLE", 43L, "SELECT_TABLE");
    CachedRolePolicies allow = spy(new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW)));
    CachedRolePolicies deny = spy(new CachedRolePolicies(1L, Map.of(denied, Effect.DENY)));
    RequestRolePolicies view =
        new RequestRolePolicies(
            0L,
            Map.of(1L, allow, 2L, deny),
            ActiveRoles.of(Set.of("allowRole")),
            Map.of(
                1L,
                new RoleUpdatedAt(1L, "allowRole", 1L, 1L),
                2L,
                new RoleUpdatedAt(2L, "denyRole", 1L, 1L)));
    assertTrue(view.evaluate(KEY, true));
    clearInvocations(allow, deny);
    assertTrue(view.evaluate(KEY, true));
    assertFalse(view.evaluate(denied, true));
    assertTrue(view.evaluate(denied, false));
    verify(allow, never()).getIndex();
    verify(deny, never()).getIndex();
  }

  /** A view pinning more policies than the merge limit keeps probing roles and stays correct. */
  @Test
  public void testMergeLimitKeepsProbingRoles() {
    PolicyKey denied = new PolicyKey("TABLE", 43L, "SELECT_TABLE");
    RequestRolePolicies view =
        new RequestRolePolicies(
            0L,
            Map.of(
                1L,
                new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW)),
                2L,
                new CachedRolePolicies(1L, Map.of(denied, Effect.DENY))),
            ActiveRoles.all(),
            Map.of(),
            true,
            1L);
    for (int i = 0; i < 100; i++) {
      assertTrue(view.evaluate(KEY, true));
      assertFalse(view.evaluate(denied, true));
      assertTrue(view.evaluate(denied, false));
    }
    assertFalse(view.isMerged(), "the view merged more policies than its limit");

    RequestRolePolicies unlimited =
        new RequestRolePolicies(
            0L,
            Map.of(1L, new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW))),
            ActiveRoles.all(),
            Map.of());
    for (int i = 0; i < 100; i++) {
      assertTrue(unlimited.evaluate(KEY, true));
    }
    assertTrue(unlimited.isMerged());
  }

  /** An unreadable snapshot denies access even when a pinned role otherwise grants it. */
  @Test
  public void testUnreadableViewFailsClosed() {
    RequestRolePolicies view =
        new RequestRolePolicies(
            0L,
            Map.of(1L, new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW))),
            ActiveRoles.all(),
            Collections.emptyMap(),
            false);
    assertFalse(view.evaluate(KEY, true));
    assertTrue(view.evaluate(KEY, false));
    assertTrue(view.hasDeny(Set.of("SELECT_TABLE")));
  }

  /** Validation metadata advances monotonically without changing pinned privilege data. */
  @Test
  public void testValidationWatermarkCannotRegress() {
    RequestRolePolicies view =
        new RequestRolePolicies(
            10L,
            Map.of(1L, new CachedRolePolicies(1L, Map.of(KEY, Effect.ALLOW))),
            ActiveRoles.all(),
            Collections.emptyMap());
    view.validatedAt(20L);
    view.validatedAt(5L);
    assertEquals(20L, view.generation());
    assertTrue(view.evaluate(KEY, true));
  }
}
