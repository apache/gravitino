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
import org.apache.gravitino.dto.requests.FilesetCreateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.rest.FilesetOperations;
import org.junit.jupiter.api.Test;

public class TestFilesetAuthorizationExpression {

  @Test
  public void testCreateFileset() throws NoSuchMethodException, OgnlException {
    Method method =
        FilesetOperations.class.getMethod(
            "createFileset", String.class, String.class, String.class, FilesetCreateRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("CATALOG::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_FILESET", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_CREATE_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_CREATE_FILESET",
                "CATALOG::CREATE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_FILESET",
                "CATALOG::DENY_CREATE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testLoadFileset() throws OgnlException, NoSuchFieldException, IllegalAccessException {
    Field loadFilesetAuthorizationExpressionField =
        AuthorizationExpressionConstants.class.getDeclaredField(
            "loadFilesetAuthorizationExpression");
    loadFilesetAuthorizationExpressionField.setAccessible(true);
    String loadFilesetAuthorizationExpression =
        (String) loadFilesetAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(loadFilesetAuthorizationExpression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_FILESET", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::WRITE_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::WRITE_FILESET",
                "CATALOG::DENY_WRITE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_WRITE_FILESET",
                "CATALOG::WRITE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::READ_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::READ_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::READ_FILESET")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::READ_FILESET", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::READ_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::READ_FILESET",
                "CATALOG::DENY_READ_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_READ_FILESET",
                "CATALOG::READ_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testAlterFileset() throws NoSuchMethodException, OgnlException {
    Method method =
        FilesetOperations.class.getMethod(
            "alterFileset",
            String.class,
            String.class,
            String.class,
            String.class,
            FilesetUpdatesRequest.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_FILESET", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::WRITE_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::WRITE_FILESET",
                "CATALOG::DENY_WRITE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_WRITE_FILESET",
                "CATALOG::WRITE_FILESET",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testDropFileset() throws NoSuchMethodException, OgnlException {
    Method method =
        FilesetOperations.class.getMethod(
            "dropFileset", String.class, String.class, String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    String expression = authorizationExpressionAnnotation.expression();
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(expression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_FILESET", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::WRITE_FILESET")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::WRITE_FILESET", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::WRITE_FILESET", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER", "CATALOG::USE_CATALOG", "METALAKE::DENY_USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }
}
