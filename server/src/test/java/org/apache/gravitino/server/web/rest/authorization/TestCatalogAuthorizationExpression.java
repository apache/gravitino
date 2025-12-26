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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import ognl.OgnlException;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.rest.CatalogOperations;
import org.junit.jupiter.api.Test;

public class TestCatalogAuthorizationExpression {

  @Test
  public void testCreateCatalog() throws NoSuchMethodException, OgnlException {
    Method method =
        CatalogOperations.class.getMethod(
            "createCatalog", String.class, CatalogCreateRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CREATE_CATALOG")));
  }

  @Test
  public void testLoadCatalog() throws NoSuchMethodException, OgnlException {
    Method method = CatalogOperations.class.getMethod("loadCatalog", String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_SCHEMA")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CREATE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("METALAKE::DENY_USE_CATALOG", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testListCatalog() throws NoSuchFieldException, IllegalAccessException, OgnlException {
    Field loadTableAuthorizationExpressionField =
        AuthorizationExpressionConstants.class.getDeclaredField(
            "loadCatalogAuthorizationExpression");
    loadTableAuthorizationExpressionField.setAccessible(true);
    String loadTableAuthExpression = (String) loadTableAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(loadTableAuthExpression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_SCHEMA")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CREATE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("METALAKE::DENY_USE_CATALOG", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testAlterCatalog() throws NoSuchMethodException, OgnlException {
    Method method =
        CatalogOperations.class.getMethod(
            "alterCatalog", String.class, String.class, CatalogUpdatesRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_SCHEMA")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropCatalog() throws NoSuchMethodException, OgnlException {
    Method method =
        CatalogOperations.class.getMethod("dropCatalog", String.class, String.class, boolean.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_SCHEMA")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::USE_CATALOG")));
  }
}
