/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.filter.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PolicyTagAddRequest;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.rest.TagOperations;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests authorization selection for binding policies to tags. */
public class TestBindTagPolicyAuthorizationExecutor {
  @Test
  void testAuthorizationDisabledDoesNotReadPolicy() throws Exception {
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    when(provider.getGravitinoAuthorizer()).thenReturn(new PassThroughAuthorizer());
    try (MockedStatic<GravitinoAuthorizerProvider> providers =
            mockStatic(GravitinoAuthorizerProvider.class);
        MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      Method method =
          TagOperations.class.getMethod(
              "addPolicyForTag",
              String.class,
              String.class,
              String.class,
              PolicyTagAddRequest.class);
      BindTagPolicyAuthorizationExecutor executor =
          new BindTagPolicyAuthorizationExecutor(
              "METALAKE::OWNER",
              Map.of(Entity.EntityType.METALAKE, NameIdentifier.of("lake")),
              Collections.emptyMap(),
              Optional.empty(),
              method.getParameters(),
              new Object[] {"lake", "tag", "policy", null},
              true);
      assertTrue(executor.execute(new AuthorizationRequestContext()));
      environments.verifyNoInteractions();
    }
  }

  @Test
  void testSelectsAuthorizationForPolicyType() throws Exception {
    Method method =
        TagOperations.class.getMethod(
            "addPolicyForTag", String.class, String.class, String.class, PolicyTagAddRequest.class);
    assertSelectsAuthorizationForPolicyType(
        method,
        new Object[] {"lake", "tag", "policy", mock(PolicyTagAddRequest.class)},
        true,
        (executor, dispatcher) -> {
          when(dispatcher.getPolicy("lake", "policy"))
              .thenThrow(new IllegalStateException("policy read failed"));
          assertThrows(
              IllegalStateException.class,
              () -> executor.execute(new AuthorizationRequestContext()));
        });
  }

  @Test
  void testUnbindUsesOrdinaryAssociationForEitherPolicyType() throws Exception {
    Method method =
        TagOperations.class.getMethod(
            "removePolicyFromTag", String.class, String.class, String.class);
    assertSelectsAuthorizationForPolicyType(
        method, new Object[] {"lake", "tag", "policy"}, false, (executor, dispatcher) -> {});
  }

  private static void assertSelectsAuthorizationForPolicyType(
      Method method,
      Object[] args,
      boolean binding,
      ThrowingBiConsumer<BindTagPolicyAuthorizationExecutor, PolicyDispatcher> extraChecks)
      throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    PolicyEntity policy = mock(PolicyEntity.class);
    when(env.policyDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getPolicy("lake", "policy")).thenReturn(policy);
    BindTagPolicyAuthorizationExecutor executor =
        new BindTagPolicyAuthorizationExecutor(
            "TAG::OWNER",
            Map.of(
                Entity.EntityType.METALAKE,
                NameIdentifier.of("lake"),
                Entity.EntityType.POLICY,
                NameIdentifier.of("lake", "policy")),
            Collections.emptyMap(),
            Optional.empty(),
            method.getParameters(),
            args,
            binding);
    executor.authorizationDisabled = false;
    AuthorizationExpressionEvaluator access = mock(AuthorizationExpressionEvaluator.class);
    AuthorizationExpressionEvaluator governance = mock(AuthorizationExpressionEvaluator.class);
    executor.accessPolicyEvaluator = access;
    executor.authorizationExpressionEvaluator = governance;
    when(access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    when(governance.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      when(policy.policyType()).thenReturn(Policy.BuiltInType.ACCESS_CONTROL);
      assertEquals(!binding, executor.execute(new AuthorizationRequestContext()));
      if (binding) {
        when(access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
        when(governance.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
        assertEquals(false, executor.execute(new AuthorizationRequestContext()));
        when(governance.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
        assertTrue(executor.execute(new AuthorizationRequestContext()));
      }
      when(policy.policyType()).thenReturn(Policy.BuiltInType.CUSTOM);
      assertTrue(executor.execute(new AuthorizationRequestContext()));
      extraChecks.accept(executor, dispatcher);
    }
  }

  @FunctionalInterface
  private interface ThrowingBiConsumer<A, B> {
    void accept(A first, B second) throws Exception;
  }
}
