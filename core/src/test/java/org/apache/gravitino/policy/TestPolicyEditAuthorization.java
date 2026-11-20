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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.PolicyEntity;
import org.junit.jupiter.api.Test;

class TestPolicyEditAuthorization {
  @Test
  void testCheckedContentAndIdentity() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "policy");
    PolicyContent original = mock(PolicyContent.class);
    PolicyContent changed = mock(PolicyContent.class);
    PolicyEntity checked = policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, original);
    PolicyEditAuthorization.record(checked);
    assertDoesNotThrow(
        () ->
            PolicyEditAuthorization.check(
                identifier, policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, changed)));
    try (PolicyEditAuthorization scope = PolicyEditAuthorization.open()) {
      PolicyEditAuthorization.record(checked);
      assertDoesNotThrow(() -> PolicyEditAuthorization.check(identifier, checked));
      assertThrows(
          OptimisticLockException.class,
          () ->
              PolicyEditAuthorization.check(
                  identifier, policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, changed)));
      assertThrows(
          OptimisticLockException.class,
          () ->
              PolicyEditAuthorization.check(
                  identifier, policy(identifier, 2L, Policy.BuiltInType.ACCESS_CONTROL, original)));
      assertThrows(
          OptimisticLockException.class,
          () ->
              PolicyEditAuthorization.check(
                  identifier, policy(identifier, 1L, Policy.BuiltInType.CUSTOM, original)));
      assertDoesNotThrow(
          () -> PolicyEditAuthorization.check(NameIdentifier.of("metalake", "other"), checked));
    }
    assertDoesNotThrow(
        () ->
            PolicyEditAuthorization.check(
                identifier, policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, changed)));
  }

  @Test
  void testNestedScopeRestoresPreviousSnapshot() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "policy");
    PolicyContent original = mock(PolicyContent.class);
    PolicyContent changed = mock(PolicyContent.class);
    PolicyEntity checked = policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, original);
    PolicyEntity updated = policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, changed);
    try (PolicyEditAuthorization outer = PolicyEditAuthorization.open()) {
      PolicyEditAuthorization.record(checked);
      try (PolicyEditAuthorization inner = PolicyEditAuthorization.open()) {
        assertDoesNotThrow(() -> PolicyEditAuthorization.check(identifier, updated));
        PolicyEditAuthorization.record(updated);
        assertDoesNotThrow(() -> PolicyEditAuthorization.check(identifier, updated));
      }
      assertThrows(
          OptimisticLockException.class, () -> PolicyEditAuthorization.check(identifier, updated));
    }
  }

  @Test
  void testUnboundConditionIsScopedToCheckedPolicy() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "policy");
    PolicyEditAuthorization.requireUnbound();
    assertFalse(PolicyEditAuthorization.requiresUnbound(identifier));
    assertFalse(PolicyEditAuthorization.isChecked(identifier));
    try (PolicyEditAuthorization scope = PolicyEditAuthorization.open()) {
      PolicyEditAuthorization.requireUnbound();
      assertFalse(PolicyEditAuthorization.requiresUnbound(identifier));
      PolicyEditAuthorization.record(
          policy(identifier, 1L, Policy.BuiltInType.ACCESS_CONTROL, mock(PolicyContent.class)));
      assertTrue(PolicyEditAuthorization.isChecked(identifier));
      assertFalse(PolicyEditAuthorization.isChecked(NameIdentifier.of("metalake", "other")));
      assertTrue(PolicyEditAuthorization.requiresUnbound(identifier));
      assertFalse(PolicyEditAuthorization.requiresUnbound(NameIdentifier.of("metalake", "other")));
    }
    assertFalse(PolicyEditAuthorization.requiresUnbound(identifier));
    assertFalse(PolicyEditAuthorization.isChecked(identifier));
  }

  private PolicyEntity policy(
      NameIdentifier identifier, long id, Policy.BuiltInType type, PolicyContent content) {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.nameIdentifier()).thenReturn(identifier);
    when(policy.id()).thenReturn(id);
    when(policy.policyType()).thenReturn(type);
    when(policy.content()).thenReturn(content);
    return policy;
  }
}
