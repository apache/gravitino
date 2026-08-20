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
package org.apache.gravitino.server.web.rest.authorization;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.APPLY_TAG_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_POLICY_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TAG_AUTHORIZATION_EXPRESSION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import ognl.OgnlException;
import org.junit.jupiter.api.Test;

public class TestTagPolicyAuthorizationExpression {

  @Test
  public void testApplyTagImpliesViewUnlessViewIsExplicitlyDenied() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(LOAD_TAG_AUTHORIZATION_EXPRESSION);

    assertTrue(evaluator.getResult(ImmutableSet.of("TAG::VIEW_TAG")));
    assertTrue(evaluator.getResult(ImmutableSet.of("TAG::APPLY_TAG")));
    assertFalse(evaluator.getResult(ImmutableSet.of("TAG::APPLY_TAG", "TAG::DENY_VIEW_TAG")));
    assertTrue(evaluator.getResult(ImmutableSet.of("TAG::VIEW_TAG", "TAG::DENY_APPLY_TAG")));
  }

  @Test
  public void testViewTagDoesNotGrantApplyTag() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(APPLY_TAG_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of("TAG::VIEW_TAG")));
    assertTrue(evaluator.getResult(ImmutableSet.of("TAG::APPLY_TAG")));
  }

  @Test
  public void testApplyPolicyImpliesViewUnlessViewIsExplicitlyDenied() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(LOAD_POLICY_AUTHORIZATION_EXPRESSION);

    assertTrue(evaluator.getResult(ImmutableSet.of("POLICY::VIEW_POLICY")));
    assertTrue(evaluator.getResult(ImmutableSet.of("POLICY::APPLY_POLICY")));
    assertFalse(
        evaluator.getResult(ImmutableSet.of("POLICY::APPLY_POLICY", "POLICY::DENY_VIEW_POLICY")));
    assertTrue(
        evaluator.getResult(ImmutableSet.of("POLICY::VIEW_POLICY", "POLICY::DENY_APPLY_POLICY")));
  }
}
