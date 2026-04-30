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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import ognl.OgnlException;
import org.apache.gravitino.iceberg.service.rest.IcebergTableOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergTableRenameOperations;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.ExpressionAction;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.junit.jupiter.api.Test;

public class TestIcebergTableAuthorizationExpression {

  @Test
  public void testTableExistsAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergTableOperations.class.getMethod(
            "tableExists", String.class, String.class, String.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    assertEquals(
        ExpressionCondition.PREVIOUS_EXPRESSION_FORBIDDEN, annotation.secondaryExpressionCondition());
    assertEquals(ExpressionAction.CHECK_METADATA_OBJECT_EXISTS, annotation.secondaryExpressionAction());

    MockAuthorizationExpressionEvaluator primaryEvaluator =
        new MockAuthorizationExpressionEvaluator(annotation.expression());
    MockAuthorizationExpressionEvaluator secondaryEvaluator =
        new MockAuthorizationExpressionEvaluator(annotation.secondaryExpression());

    assertTrue(
        primaryEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::SELECT_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(primaryEvaluator.getResult(ImmutableSet.of()));
    assertTrue(
        secondaryEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        secondaryEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::SELECT_VIEW", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testRenameTableAuthorization() throws NoSuchMethodException, OgnlException {
    Method method =
        IcebergTableRenameOperations.class.getMethod(
            "renameTable", String.class, RenameTableRequest.class);
    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    assertEquals(
        ExpressionCondition.RENAMING_CROSSING_NAMESPACE, annotation.secondaryExpressionCondition());
    assertEquals(
        ExpressionCondition.RENAMING_CROSSING_NAMESPACE, annotation.thirdExpressionCondition());

    MockAuthorizationExpressionEvaluator primaryEvaluator =
        new MockAuthorizationExpressionEvaluator(annotation.expression());
    MockAuthorizationExpressionEvaluator secondaryEvaluator =
        new MockAuthorizationExpressionEvaluator(annotation.secondaryExpression());
    MockAuthorizationExpressionEvaluator thirdEvaluator =
        new MockAuthorizationExpressionEvaluator(annotation.thirdExpression());

    assertTrue(
        primaryEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::MODIFY_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        secondaryEvaluator.getResult(
            ImmutableSet.of("TABLE::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertTrue(
        thirdEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_TABLE", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
  }
}
