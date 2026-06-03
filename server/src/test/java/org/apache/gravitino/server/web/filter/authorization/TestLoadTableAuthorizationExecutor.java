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

package org.apache.gravitino.server.web.filter.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.junit.jupiter.api.Test;

public class TestLoadTableAuthorizationExecutor {
  private static final String PRIMARY_EXPRESSION =
      AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;
  private static final String SECONDARY_EXPRESSION =
      AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION;

  @Test
  public void testUsesSecondaryExpressionForRequiredModifyPrivilege() throws Exception {
    LoadTableAuthorizationExecutor executor =
        createExecutor(
            "SELECT_TABLE, MODIFY_TABLE", ExpressionCondition.REQUIRED_MODIFY_PRIVILEGES);

    assertEquals(SECONDARY_EXPRESSION, expression(executor));
  }

  @Test
  public void testUsesPrimaryExpressionWhenConditionNever() throws Exception {
    LoadTableAuthorizationExecutor executor =
        createExecutor("MODIFY_TABLE", ExpressionCondition.NEVER);

    assertEquals(PRIMARY_EXPRESSION, expression(executor));
  }

  @Test
  public void testUsesPrimaryExpressionWithoutModifyPrivilege() throws Exception {
    LoadTableAuthorizationExecutor executor =
        createExecutor("SELECT_TABLE", ExpressionCondition.REQUIRED_MODIFY_PRIVILEGES);

    assertEquals(PRIMARY_EXPRESSION, expression(executor));
  }

  private static LoadTableAuthorizationExecutor createExecutor(
      String privileges, ExpressionCondition condition) throws Exception {
    Method method = TestOperations.class.getMethod("loadTable", String.class);
    return new LoadTableAuthorizationExecutor(
        method.getParameters(),
        new Object[] {privileges},
        PRIMARY_EXPRESSION,
        Collections.<Entity.EntityType, NameIdentifier>emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        SECONDARY_EXPRESSION,
        condition);
  }

  private static String expression(LoadTableAuthorizationExecutor executor) throws Exception {
    Field expressionField = CommonAuthorizerExecutor.class.getDeclaredField("expression");
    expressionField.setAccessible(true);
    return (String) expressionField.get(executor);
  }

  @SuppressWarnings("unused")
  public static class TestOperations {
    public void loadTable(
        @AuthorizationRequest(type = AuthorizationRequest.RequestType.LOAD_TABLE)
            String privileges) {}
  }
}
