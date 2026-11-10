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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import ognl.OgnlException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.rest.GroupOperations;
import org.junit.jupiter.api.Test;

public class TestGroupAuthorizationExpression {

  @Test
  public void testGetGroupAuthorizationExpression() throws NoSuchMethodException, OgnlException {
    Method method = GroupOperations.class.getMethod("getGroup", String.class, String.class);
    AuthorizationExpression authorizationExpressionAnnotation =
        method.getAnnotation(AuthorizationExpression.class);
    assertNotNull(authorizationExpressionAnnotation);

    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(authorizationExpressionAnnotation.expression());
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::MANAGE_USERS")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::MANAGE_GROUPS")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("GROUP::SELF")));
  }

  @Test
  public void testGetGroupAuthorizationMetadata() throws NoSuchMethodException {
    Method method = GroupOperations.class.getMethod("getGroup", String.class, String.class);
    Parameter[] parameters = method.getParameters();

    AuthorizationMetadata metalakeMetadata =
        parameters[0].getAnnotation(AuthorizationMetadata.class);
    assertNotNull(metalakeMetadata);
    assertEquals(Entity.EntityType.METALAKE, metalakeMetadata.type());

    AuthorizationMetadata groupMetadata = parameters[1].getAnnotation(AuthorizationMetadata.class);
    assertNotNull(groupMetadata);
    assertEquals(Entity.EntityType.GROUP, groupMetadata.type());
  }
}
