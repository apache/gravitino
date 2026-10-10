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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import ognl.OgnlException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.ViewCreateRequest;
import org.apache.gravitino.dto.requests.ViewUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.rest.ViewOperations;
import org.junit.jupiter.api.Test;

/** Tests authorization expressions and metadata parameters on generic view REST endpoints. */
public class TestViewAuthorizationExpression {

  @Test
  public void testListViewAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        ViewOperations.class.getMethod("listViews", String.class, String.class, String.class);
    assertAuthorizationAnnotation(
        method,
        AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
        MetadataObject.Type.SCHEMA);
    assertMetadataTypes(
        method, Entity.EntityType.METALAKE, Entity.EntityType.CATALOG, Entity.EntityType.SCHEMA);

    MockAuthorizationExpressionEvaluator gateway = evaluator(method);
    assertFalse(gateway.getResult(ImmutableSet.of()));
    assertTrue(gateway.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(gateway.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(gateway.getResult(ImmutableSet.of("SCHEMA::USE_SCHEMA")));
    assertTrue(gateway.getResult(ImmutableSet.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA")));

    MockAuthorizationExpressionEvaluator filter =
        new MockAuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.FILTER_VIEW_AUTHORIZATION_EXPRESSION);
    assertFalse(filter.getResult(ImmutableSet.of()));
    assertTrue(filter.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertTrue(filter.getResult(ImmutableSet.of("SCHEMA::SELECT_VIEW")));
    assertFalse(filter.getResult(ImmutableSet.of("SCHEMA::CREATE_VIEW")));
    assertFalse(
        filter.getResult(ImmutableSet.of("METALAKE::SELECT_VIEW", "CATALOG::DENY_SELECT_VIEW")));
  }

  @Test
  public void testCreateViewAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        ViewOperations.class.getMethod(
            "createView", String.class, String.class, String.class, ViewCreateRequest.class);
    assertAuthorizationAnnotation(
        method,
        AuthorizationExpressionConstants.CREATE_VIEW_AUTHORIZATION_EXPRESSION,
        MetadataObject.Type.SCHEMA);
    assertMetadataTypes(
        method,
        Entity.EntityType.METALAKE,
        Entity.EntityType.CATALOG,
        Entity.EntityType.SCHEMA,
        null);

    MockAuthorizationExpressionEvaluator evaluator = evaluator(method);
    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(evaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertFalse(evaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testLoadViewAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        ViewOperations.class.getMethod(
            "loadView", String.class, String.class, String.class, String.class);
    assertAuthorizationAnnotation(
        method,
        AuthorizationExpressionConstants.LOAD_VIEW_AUTHORIZATION_EXPRESSION,
        MetadataObject.Type.VIEW);
    assertViewMetadataTypes(method);

    MockAuthorizationExpressionEvaluator evaluator = evaluator(method);
    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(evaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_VIEW",
                "CATALOG::DENY_SELECT_VIEW",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testAlterViewAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        ViewOperations.class.getMethod(
            "alterView",
            String.class,
            String.class,
            String.class,
            String.class,
            ViewUpdatesRequest.class);
    assertOwnerMutationAuthorization(method);
    assertMetadataTypes(
        method,
        Entity.EntityType.METALAKE,
        Entity.EntityType.CATALOG,
        Entity.EntityType.SCHEMA,
        Entity.EntityType.VIEW,
        null);
  }

  @Test
  public void testDropViewAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        ViewOperations.class.getMethod(
            "dropView", String.class, String.class, String.class, String.class);
    assertOwnerMutationAuthorization(method);
    assertViewMetadataTypes(method);
  }

  private void assertOwnerMutationAuthorization(Method method) throws OgnlException {
    assertAuthorizationAnnotation(
        method,
        AuthorizationExpressionConstants.VIEW_OWNER_AUTHORIZATION_EXPRESSION,
        MetadataObject.Type.VIEW);
    MockAuthorizationExpressionEvaluator evaluator = evaluator(method);
    assertFalse(evaluator.getResult(ImmutableSet.of()));
    assertTrue(evaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(evaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertTrue(
        evaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        evaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  private MockAuthorizationExpressionEvaluator evaluator(Method method) {
    return new MockAuthorizationExpressionEvaluator(
        method.getAnnotation(AuthorizationExpression.class).expression());
  }

  private void assertAuthorizationAnnotation(
      Method method, String expression, MetadataObject.Type metadataType) {
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    assertNotNull(annotation);
    assertEquals(expression, annotation.expression());
    assertEquals(metadataType, annotation.accessMetadataType());
  }

  private void assertViewMetadataTypes(Method method) {
    assertMetadataTypes(
        method,
        Entity.EntityType.METALAKE,
        Entity.EntityType.CATALOG,
        Entity.EntityType.SCHEMA,
        Entity.EntityType.VIEW);
  }

  private void assertMetadataTypes(Method method, Entity.EntityType... expectedTypes) {
    Parameter[] parameters = method.getParameters();
    assertEquals(expectedTypes.length, parameters.length);
    for (int i = 0; i < parameters.length; i++) {
      AuthorizationMetadata annotation = parameters[i].getAnnotation(AuthorizationMetadata.class);
      if (expectedTypes[i] == null) {
        assertNull(annotation);
      } else {
        assertNotNull(annotation);
        assertEquals(expectedTypes[i], annotation.type());
      }
    }
  }
}
