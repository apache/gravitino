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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.rest.PolicyOperations;
import org.junit.jupiter.api.Test;

/** Tests type-specific policy creation authorization. */
public class TestCreatePolicyAuthorizationExecutor {
  @Test
  void testAccessPolicyRequiresBothChecks() throws Exception {
    Method method =
        PolicyOperations.class.getMethod("createPolicy", String.class, PolicyCreateRequest.class);
    PolicyCreateRequest request = mock(PolicyCreateRequest.class);
    CreatePolicyAuthorizationExecutor executor =
        new CreatePolicyAuthorizationExecutor(
            "METALAKE::CREATE_POLICY",
            Map.of(Entity.EntityType.METALAKE, NameIdentifier.of("lake")),
            Map.of(),
            Optional.empty(),
            method.getParameters(),
            new Object[] {"lake", request});
    AuthorizationExpressionEvaluator ordinary = mock(AuthorizationExpressionEvaluator.class);
    AuthorizationExpressionEvaluator grants = mock(AuthorizationExpressionEvaluator.class);
    executor.authorizationExpressionEvaluator = ordinary;
    executor.grantEvaluator = grants;
    when(request.getPolicyType())
        .thenReturn(Policy.BuiltInType.ACCESS_CONTROL.policyType().toUpperCase());
    when(ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    when(grants.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    assertFalse(executor.execute(new AuthorizationRequestContext()));
    when(grants.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    assertTrue(executor.execute(new AuthorizationRequestContext()));
    when(ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    assertFalse(executor.execute(new AuthorizationRequestContext()));
    when(ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    when(grants.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    when(request.getPolicyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION.policyType());
    assertTrue(executor.execute(new AuthorizationRequestContext()));
    when(request.getPolicyType()).thenReturn(null);
    assertTrue(executor.execute(new AuthorizationRequestContext()));
  }
}
