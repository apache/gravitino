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

package org.apache.gravitino.server.web.rest.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import ognl.OgnlException;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.junit.jupiter.api.Test;

/**
 * Verifies the Semantic Model authorization expressions against the rules in the Semantic Model
 * design: creation needs CREATE_SEMANTIC_MODEL under USE_CATALOG and USE_SCHEMA, list and load
 * accept SELECT_SEMANTIC_MODEL or MODIFY_SEMANTIC_MODEL, alter needs MODIFY_SEMANTIC_MODEL, and
 * drop needs ownership rather than any privilege.
 */
public class TestSemanticModelAuthorizationExpression {

  @Test
  public void testCreateSemanticModel() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.CREATE_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // A schema owner still needs USE_CATALOG to reach the schema.
    assertFalse(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // Everyone else needs the full USE_CATALOG + USE_SCHEMA + CREATE_SEMANTIC_MODEL chain.
    assertFalse(evaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_SEMANTIC_MODEL")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_SEMANTIC_MODEL", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_SEMANTIC_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_SEMANTIC_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));

    // A deny anywhere in the hierarchy defeats an allow granted lower down.
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_SEMANTIC_MODEL",
                "CATALOG::DENY_CREATE_SEMANTIC_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testLoadSemanticModel() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // Owning the Semantic Model is enough once the parents are reachable.
    assertFalse(evaluator.getResult(ImmutableSet.of("SEMANTIC_MODEL::OWNER")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::OWNER", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));

    // Either SELECT or MODIFY loads the definition.
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::SELECT_SEMANTIC_MODEL",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::MODIFY_SEMANTIC_MODEL", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));

    // Unrelated privileges do not grant visibility.
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_SEMANTIC_MODEL", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_SEMANTIC_MODEL",
                "SEMANTIC_MODEL::DENY_SELECT_SEMANTIC_MODEL",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA")));
  }

  @Test
  public void testFilterSemanticModel() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.FILTER_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SEMANTIC_MODEL::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_SEMANTIC_MODEL")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SEMANTIC_MODEL::MODIFY_SEMANTIC_MODEL")));
    assertFalse(evaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_SEMANTIC_MODEL")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_SEMANTIC_MODEL", "SCHEMA::DENY_SELECT_SEMANTIC_MODEL")));
  }

  @Test
  public void testAlterSemanticModel() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::OWNER", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::MODIFY_SEMANTIC_MODEL",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA")));

    // SELECT_SEMANTIC_MODEL is read-only and must not permit an alter.
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::SELECT_SEMANTIC_MODEL",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA")));
  }

  @Test
  public void testDropSemanticModel() throws OgnlException {
    MockAuthorizationExpressionEvaluator evaluator =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.DROP_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);

    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::OWNER", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));

    // Unlike alter, MODIFY_SEMANTIC_MODEL does not authorize a drop.
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "SEMANTIC_MODEL::MODIFY_SEMANTIC_MODEL",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA")));
  }
}
