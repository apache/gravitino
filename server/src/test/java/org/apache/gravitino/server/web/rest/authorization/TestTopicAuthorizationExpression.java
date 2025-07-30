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
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.requests.TopicUpdatesRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.web.rest.TopicOperations;
import org.junit.jupiter.api.Test;

public class TestTopicAuthorizationExpression {

  @Test
  public void testCreateTopic() throws NoSuchMethodException, OgnlException {
    Method method =
        TopicOperations.class.getMethod(
            "createTopic", String.class, String.class, String.class, TopicCreateRequest.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TOPIC", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TOPIC", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CREATE_TOPIC",
                "METALAKE::DENY_CREATE_TOPIC",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::DENY_CREATE_TOPIC",
                "METALAKE::CREATE_TOPIC",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG")));
  }

  @Test
  public void testLoadTopics() throws OgnlException, NoSuchFieldException, IllegalAccessException {
    Field loadTopicsAuthorizationExpressionField =
        TopicOperations.class.getDeclaredField("loadTopicsAuthorizationExpression");
    loadTopicsAuthorizationExpressionField.setAccessible(true);
    String loadTopicsAuthorizationExpression =
        (String) loadTopicsAuthorizationExpressionField.get(null);
    MockAuthorizationExpressionEvaluator mockEvaluator =
        new MockAuthorizationExpressionEvaluator(loadTopicsAuthorizationExpression);
    assertFalse(mockEvaluator.getResult(ImmutableSet.of()));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_CATALOG")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("METALAKE::OWNER")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::USE_METALAKE")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CREATE_CATALOG")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TOPIC", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::PRODUCE_TOPIC", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::CONSUME_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::CONSUME_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CONSUME_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CONSUME_TOPIC", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::CONSUME_TOPIC", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG",
                "METALAKE::DENY_USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::DENY_USE_SCHEMA",
                "CATALOG::DENY_USE_CATALOG",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testAlterFileset() throws NoSuchMethodException, OgnlException {
    Method method =
        TopicOperations.class.getMethod(
            "alterTopic",
            String.class,
            String.class,
            String.class,
            String.class,
            TopicUpdatesRequest.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TOPIC", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::PRODUCE_TOPIC", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG",
                "METALAKE::DENY_USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::DENY_USE_SCHEMA",
                "CATALOG::DENY_USE_CATALOG",
                "METALAKE::USE_CATALOG")));
  }

  @Test
  public void testDropFileset() throws NoSuchMethodException, OgnlException {
    Method method =
        TopicOperations.class.getMethod(
            "dropTopic", String.class, String.class, String.class, String.class);
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
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::CREATE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::CREATE_TOPIC", "CATALOG::CREATE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("METALAKE::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("CATALOG::PRODUCE_TOPIC")));
    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC")));
    assertFalse(
        mockEvaluator.getResult(ImmutableSet.of("SCHEMA::PRODUCE_TOPIC", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::PRODUCE_TOPIC", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));

    assertFalse(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER")));
    assertTrue(mockEvaluator.getResult(ImmutableSet.of("SCHEMA::OWNER", "CATALOG::USE_CATALOG")));
    assertTrue(
        mockEvaluator.getResult(
            ImmutableSet.of("SCHEMA::OWNER", "SCHEMA::USE_SCHEMA", "CATALOG::USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::USE_SCHEMA",
                "CATALOG::USE_CATALOG",
                "METALAKE::DENY_USE_CATALOG")));
    assertFalse(
        mockEvaluator.getResult(
            ImmutableSet.of(
                "SCHEMA::OWNER",
                "SCHEMA::DENY_USE_SCHEMA",
                "CATALOG::DENY_USE_CATALOG",
                "METALAKE::USE_CATALOG")));
  }
}
