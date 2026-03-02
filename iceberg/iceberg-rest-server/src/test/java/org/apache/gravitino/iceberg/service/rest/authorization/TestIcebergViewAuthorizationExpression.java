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

package org.apache.gravitino.iceberg.service.rest.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import ognl.OgnlException;
import org.apache.gravitino.iceberg.service.rest.IcebergViewOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergViewRenameOperations;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.Test;

/** Tests for {@link AuthorizationExpression} annotations on Iceberg view REST endpoints. */
public class TestIcebergViewAuthorizationExpression {

  @Test
  public void testCreateView() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewOperations.class.getMethod(
            "createView", String.class, String.class, CreateViewRequest.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Metalake/catalog owner -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // Schema owner alone -> denied (needs USE_CATALOG)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));

    // CREATE_VIEW alone -> denied (needs USE_CATALOG + USE_SCHEMA)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_VIEW")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA")));

    // CREATE_VIEW + USE_SCHEMA + USE_CATALOG -> allowed
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::CREATE_VIEW", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_VIEW", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));

    // DENY overrides CREATE_VIEW
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_VIEW",
                "CATALOG::DENY_CREATE_VIEW",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_CREATE_VIEW",
                "CATALOG::CREATE_VIEW",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testListView() throws IllegalAccessException, OgnlException, NoSuchFieldException {
    // listView uses LOAD_SCHEMA_AUTHORIZATION_EXPRESSION for the gateway check
    // The per-view filtering uses FILTER_VIEW_AUTHORIZATION_EXPRESSION
    Field filterViewField =
        AuthorizationExpressionConstants.class.getDeclaredField(
            "FILTER_VIEW_AUTHORIZATION_EXPRESSION");
    filterViewField.setAccessible(true);
    String filterViewExpression = (String) filterViewField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(filterViewExpression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Any owner level -> allowed (filter passes)
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER")));

    // SELECT_VIEW at any level -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_VIEW")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::SELECT_VIEW")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::SELECT_VIEW")));

    // DENY overrides SELECT_VIEW
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("METALAKE::SELECT_VIEW", "CATALOG::DENY_SELECT_VIEW")));
  }

  @Test
  public void testLoadView() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewOperations.class.getMethod("loadView", String.class, String.class, String.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Metalake/catalog owner -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // Schema owner alone -> denied (needs USE_CATALOG)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // VIEW::OWNER alone -> denied (needs USE_CATALOG + USE_SCHEMA)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // SELECT_VIEW without USE_CATALOG/USE_SCHEMA -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_VIEW")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA")));

    // SELECT_VIEW + USE_SCHEMA + USE_CATALOG -> allowed
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::SELECT_VIEW", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_VIEW", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));

    // DENY overrides SELECT_VIEW
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_VIEW",
                "CATALOG::DENY_SELECT_VIEW",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));

    // CREATE_VIEW grants load permission to allow catalog implementations that call viewExists()
    // during creation to work
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testReplaceView() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewOperations.class.getMethod(
            "replaceView", String.class, String.class, String.class, UpdateTableRequest.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Metalake/catalog owner -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // Schema owner + USE_CATALOG -> allowed
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // VIEW::OWNER + USE_CATALOG + USE_SCHEMA -> allowed (owner-only for alter)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // SELECT_VIEW does NOT grant alter/replace permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // CREATE_VIEW does NOT grant alter/replace permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // DENY USE_SCHEMA blocks even with VIEW::OWNER
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "VIEW::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropView() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewOperations.class.getMethod("dropView", String.class, String.class, String.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Metalake/catalog owner -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // Schema owner + USE_CATALOG -> allowed
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // VIEW::OWNER + USE_CATALOG + USE_SCHEMA -> allowed (owner-only for drop)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // SELECT_VIEW does NOT grant drop permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // CREATE_VIEW does NOT grant drop permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // DENY USE_SCHEMA blocks
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "VIEW::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "VIEW::OWNER",
                "SCHEMA::DENY_USE_SCHEMA",
                "CATALOG::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testViewExists() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewOperations.class.getMethod(
            "viewExists", String.class, String.class, String.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Owner levels -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // VIEW::OWNER + USE_CATALOG + USE_SCHEMA -> allowed
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // SELECT_VIEW + USE_CATALOG + USE_SCHEMA -> allowed
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // CREATE_VIEW + USE_CATALOG + USE_SCHEMA -> allowed (can check existence for create)
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testRenameView() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergViewRenameOperations.class.getMethod(
            "renameView", String.class, RenameTableRequest.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    String expression = annotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    // No privileges -> denied
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));

    // Metalake/catalog owner -> allowed
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));

    // Schema owner + USE_CATALOG -> allowed
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // VIEW::OWNER + USE_CATALOG + USE_SCHEMA -> allowed (owner-only for rename)
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("VIEW::OWNER")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // SELECT_VIEW does NOT grant rename permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // CREATE_VIEW does NOT grant rename permission
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testLoadViewAuthorizationExpression()
      throws NoSuchFieldException, IllegalAccessException, OgnlException {
    // Verify the LOAD_VIEW_AUTHORIZATION_EXPRESSION constant directly
    Field field =
        AuthorizationExpressionConstants.class.getDeclaredField(
            "LOAD_VIEW_AUTHORIZATION_EXPRESSION");
    field.setAccessible(true);
    String expression = (String) field.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);

    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));

    // VIEW::OWNER or SELECT_VIEW with USE_CATALOG+USE_SCHEMA
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("VIEW::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    // DENY_SELECT_VIEW blocks
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::SELECT_VIEW",
                "CATALOG::DENY_SELECT_VIEW",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }
}
