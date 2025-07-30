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
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.rest.TableOperations;
import org.junit.jupiter.api.Test;

public class TestTableAuthorizationExpression {

  @Test
  public void testCreateTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "createTable", String.class, String.class, String.class, TableCreateRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::CREATE_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_TABLE",
                "CATALOG::DENY_CREATE_TABLE",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_CREATE_TABLE",
                "CATALOG::CREATE_TABLE",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testListTable() throws IllegalAccessException, OgnlException, NoSuchFieldException {
    Field loadTableAuthorizationExpressionField =
        TableOperations.class.getDeclaredField("loadTableAuthorizationExpression");
    loadTableAuthorizationExpressionField.setAccessible(true);
    String loadTableAuthExpression = (String) loadTableAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(loadTableAuthExpression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::SELECT_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE",
                "CATALOG::DENY_SELECT_TABLE",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testLoadTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "loadTable", String.class, String.class, String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::SELECT_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE",
                "CATALOG::DENY_SELECT_TABLE",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testAlterTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "alterTable",
            String.class,
            String.class,
            String.class,
            String.class,
            TableUpdatesRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::SELECT_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::MODIFY_TABLE", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::MODIFY_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::MODIFY_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::MODIFY_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::MODIFY_TABLE", "CATALOG::DENY_MODIFY_TABLE",
                "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_MODIFY_TABLE", "CATALOG::MODIFY_TABLE",
                "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testDropTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "dropTable", String.class, String.class, String.class, String.class, boolean.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::SELECT_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::MODIFY_TABLE", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::MODIFY_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::MODIFY_TABLE", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::MODIFY_TABLE", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("TABLE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("TABLE::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("TABLE::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "TABLE::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "TABLE::OWNER",
                "SCHEMA::DENY_USE_SCHEMA",
                "CATALOG::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }
}
