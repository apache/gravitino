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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectionTestAuthorizationExecutor {
  private static final String PRIMARY_EXPRESSION =
      AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION;
  private static final String SECONDARY_EXPRESSION =
      AuthorizationExpressionConstants
          .TEST_CATALOG_CONNECTION_WITH_CHANGES_AUTHORIZATION_EXPRESSION;

  @Test
  public void testUsesPrimaryExpressionWithoutRequestBody() throws Exception {
    CatalogConnectionTestAuthorizationExecutor executor =
        createExecutor(null, ExpressionCondition.HAS_PROPOSED_CHANGES);

    assertEquals(PRIMARY_EXPRESSION, executor.expression);
  }

  @Test
  public void testUsesPrimaryExpressionForEmptyChanges() throws Exception {
    CatalogConnectionTestAuthorizationExecutor executor =
        createExecutor(
            new CatalogUpdatesRequest(Collections.emptyList()),
            ExpressionCondition.HAS_PROPOSED_CHANGES);

    assertEquals(PRIMARY_EXPRESSION, executor.expression);
  }

  @Test
  public void testUsesSecondaryExpressionForProposedChanges() throws Exception {
    CatalogConnectionTestAuthorizationExecutor executor =
        createExecutor(proposedChanges(), ExpressionCondition.HAS_PROPOSED_CHANGES);

    assertEquals(SECONDARY_EXPRESSION, executor.expression);
  }

  @Test
  public void testUsesPrimaryExpressionWhenConditionNever() throws Exception {
    CatalogConnectionTestAuthorizationExecutor executor =
        createExecutor(proposedChanges(), ExpressionCondition.NEVER);

    assertEquals(PRIMARY_EXPRESSION, executor.expression);
  }

  @Test
  public void testUsesPrimaryExpressionForNullChanges() throws Exception {
    CatalogConnectionTestAuthorizationExecutor executor =
        createExecutor(new CatalogUpdatesRequest(), ExpressionCondition.HAS_PROPOSED_CHANGES);

    assertEquals(PRIMARY_EXPRESSION, executor.expression);
  }

  @Test
  public void testFactoryCreatesExecutorForTestCatalogConnection() throws Exception {
    Method method = TestOperations.class.getMethod("testConnection", CatalogUpdatesRequest.class);
    AuthorizationExecutor executor =
        AuthorizeExecutorFactory.create(
            PRIMARY_EXPRESSION,
            AuthorizationRequest.RequestType.TEST_CATALOG_CONNECTION,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Optional.empty(),
            method.getParameters(),
            new Object[] {proposedChanges()},
            SECONDARY_EXPRESSION,
            ExpressionCondition.HAS_PROPOSED_CHANGES,
            "");

    assertTrue(executor instanceof CatalogConnectionTestAuthorizationExecutor);
    assertEquals(
        SECONDARY_EXPRESSION, ((CatalogConnectionTestAuthorizationExecutor) executor).expression);
  }

  private static CatalogUpdatesRequest proposedChanges() {
    return new CatalogUpdatesRequest(
        ImmutableList.of(new CatalogUpdateRequest.SetCatalogPropertyRequest("key", "value")));
  }

  private static CatalogConnectionTestAuthorizationExecutor createExecutor(
      CatalogUpdatesRequest request, ExpressionCondition condition) throws Exception {
    Method method = TestOperations.class.getMethod("testConnection", CatalogUpdatesRequest.class);
    return new CatalogConnectionTestAuthorizationExecutor(
        method.getParameters(),
        new Object[] {request},
        PRIMARY_EXPRESSION,
        Collections.<Entity.EntityType, NameIdentifier>emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        SECONDARY_EXPRESSION,
        condition);
  }

  public static class TestOperations {
    public void testConnection(
        @AuthorizationRequest(type = AuthorizationRequest.RequestType.TEST_CATALOG_CONNECTION)
            CatalogUpdatesRequest request) {}
  }
}
