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
import org.apache.gravitino.dto.requests.ModelRegisterRequest;
import org.apache.gravitino.dto.requests.ModelUpdatesRequest;
import org.apache.gravitino.dto.requests.ModelVersionLinkRequest;
import org.apache.gravitino.dto.requests.ModelVersionUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.rest.ModelOperations;
import org.junit.jupiter.api.Test;

public class TestModelAuthorizationExpression {

  @Test
  public void testCreateModel() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "registerModel", String.class, String.class, String.class, ModelRegisterRequest.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::CREATE_MODEL", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_MODEL", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_MODEL",
                "CATALOG::DENY_CREATE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_CREATE_MODEL",
                "CATALOG::CREATE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testLoadModel() throws OgnlException, NoSuchFieldException, IllegalAccessException {
    Field loadModelAuthorizationExpressionField =
        ModelOperations.class.getDeclaredField("loadModelAuthorizationExpression");
    loadModelAuthorizationExpressionField.setAccessible(true);
    String loadModelAuthExpression = (String) loadModelAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(loadModelAuthExpression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::USE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::USE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("CATALOG::USE_MODEL", "CATALOG::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::USE_MODEL", "METALAKE::USE_SCHEMA", "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::USE_MODEL",
                "CATALOG::DENY_USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::USE_MODEL",
                "CATALOG::DENY_USE_SCHEMA",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_USE_MODEL",
                "CATALOG::USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testAlterModel() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "alterModel",
            String.class,
            String.class,
            String.class,
            String.class,
            ModelUpdatesRequest.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER",
                "MODEL::USE_MODEL",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropModel() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "deleteModel", String.class, String.class, String.class, String.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER",
                "MODEL::USE_MODEL",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::DENY_USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testLinkModelVersion() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "linkModelVersion",
            String.class,
            String.class,
            String.class,
            String.class,
            ModelVersionLinkRequest.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::CREATE_MODEL_VERSION")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::CREATE_MODEL_VERSION", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::CREATE_MODEL_VERSION", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));

    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "CATALOG::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_MODEL_VERSION",
                "CATALOG::DENY_CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::DENY_CREATE_MODEL_VERSION",
                "CATALOG::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "CATALOG::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "CATALOG::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "METALAKE::CREATE_MODEL_VERSION",
                "MODEL::USE_MODEL",
                "METALAKE::USE_SCHEMA",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testAlterModelVersion() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "alterModelVersion",
            String.class,
            String.class,
            String.class,
            String.class,
            int.class,
            ModelVersionUpdatesRequest.class);
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
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER", "METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testAlterModelVersionByAlias() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "alterModelVersionByAlias",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            ModelVersionUpdatesRequest.class);
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
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER", "METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropModelVersion() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "deleteModelVersion",
            String.class,
            String.class,
            String.class,
            String.class,
            int.class);
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
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER", "METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropModelVersionByAlias() throws NoSuchMethodException, OgnlException {
    Method method =
        ModelOperations.class.getMethod(
            "deleteModelVersionByAlias",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
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
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER", "METALAKE::USE_CATALOG", "CATALOG::DENY_USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("MODEL::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "MODEL::OWNER", "MODEL::USE_MODEL", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }
}
