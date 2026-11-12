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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import ognl.OgnlException;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.rest.PartitionOperations;
import org.apache.gravitino.server.web.rest.StatisticOperations;
import org.apache.gravitino.server.web.rest.TableOperations;
import org.junit.jupiter.api.Test;

public class TestTableAuthorizationExpression {

  @Test
  void testGranularTablePrivileges() throws Exception {
    MockAuthorizationExpressionEvaluator alter =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.ALTER_TABLE_AUTHORIZATION_EXPRESSION);
    MockAuthorizationExpressionEvaluator write =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.WRITE_TABLE_AUTHORIZATION_EXPRESSION);
    MockAuthorizationExpressionEvaluator read =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.READ_TABLE_AUTHORIZATION_EXPRESSION);
    MockAuthorizationExpressionEvaluator load =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION);
    MockAuthorizationExpressionEvaluator filter =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION);
    for (String scope : List.of("METALAKE", "CATALOG", "SCHEMA", "TABLE")) {
      for (String privilege : List.of("ALTER", "WRITE_TABLE")) {
        Set<String> grants =
            new HashSet<>(
                Set.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA", scope + "::" + privilege));
        assertTrue(load.getResult(grants));
        assertTrue(filter.getResult(grants));
        assertFalse(read.getResult(grants));
        assertEquals(privilege.equals("ALTER"), alter.getResult(grants));
        assertEquals(privilege.equals("WRITE_TABLE"), write.getResult(grants));
        for (String denyScope : List.of("METALAKE", "CATALOG", "SCHEMA", "TABLE")) {
          Set<String> denied = new HashSet<>(grants);
          denied.add(denyScope + "::DENY_" + privilege);
          assertFalse(alter.getResult(denied));
          assertFalse(write.getResult(denied));
          assertFalse(load.getResult(denied));
          assertFalse(filter.getResult(denied));
        }
        grants.remove("SCHEMA::USE_SCHEMA");
        assertFalse(alter.getResult(grants));
        assertFalse(write.getResult(grants));
      }
    }
    Set<String> legacy =
        Set.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA", "TABLE::MODIFY_TABLE");
    assertTrue(alter.getResult(legacy));
    assertTrue(write.getResult(legacy));
    assertTrue(read.getResult(legacy));
    assertFalse(
        new MockAuthorizationExpressionEvaluator(
                AuthorizationExpressionConstants.DROP_TABLE_AUTHORIZATION_EXPRESSION)
            .getResult(
                Set.of(
                    "CATALOG::USE_CATALOG",
                    "SCHEMA::USE_SCHEMA",
                    "TABLE::ALTER",
                    "TABLE::WRITE_TABLE")));
  }

  @Test
  void testRestMutationsUseGranularPrivileges() throws Exception {
    int checked = 0;
    for (Class<?> operations :
        List.of(TableOperations.class, PartitionOperations.class, StatisticOperations.class)) {
      for (Method method : operations.getDeclaredMethods()) {
        AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
        if (annotation == null) {
          continue;
        }
        boolean alters =
            method.getName().equals("alterTable")
                || method.getName().equals("addPartitions")
                || method.getName().equals("dropPartition");
        boolean writesStatistics =
            operations == StatisticOperations.class
                && (method.getName().startsWith("update") || method.getName().startsWith("drop"));
        if (!alters && !writesStatistics) {
          continue;
        }
        checked++;
        MockAuthorizationExpressionEvaluator evaluator =
            new MockAuthorizationExpressionEvaluator(annotation.expression());
        Set<String> alter = Set.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA", "TABLE::ALTER");
        Set<String> write =
            Set.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA", "TABLE::WRITE_TABLE");
        assertEquals(alters, evaluator.getResult(alter), method.getName());
        assertEquals(writesStatistics, evaluator.getResult(write), method.getName());
      }
    }
    assertEquals(7, checked);
  }

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
        mockEvaluator.getResult(ImmutableSet.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
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
  public void testListTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod("listTables", String.class, String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testListTableFilter()
      throws IllegalAccessException, OgnlException, NoSuchFieldException {
    Field listTableLikeAuthorizationExpressionField =
        AuthorizationExpressionConstants.class.getDeclaredField(
            "LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION");
    listTableLikeAuthorizationExpressionField.setAccessible(true);
    String expression = (String) listTableLikeAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("TABLE::OWNER")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::PROBE_TABLE_LIKE",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_PROBE_TABLE_LIKE")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::PROBE_TABLE_LIKE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::MODIFY_TABLE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TABLE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
  }

  @Test
  public void testLoadTable() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "loadTable", String.class, String.class, String.class, String.class, String.class);
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
  public void testLoadTableExistenceProbe() throws NoSuchMethodException, OgnlException {
    Method method =
        TableOperations.class.getMethod(
            "loadTable", String.class, String.class, String.class, String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.allowCheckExistence();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("TABLE::OWNER")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::PROBE_TABLE_LIKE",
                "CATALOG::USE_CATALOG",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_PROBE_TABLE_LIKE")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::PROBE_TABLE_LIKE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_TABLE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::MODIFY_TABLE", "CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));
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
