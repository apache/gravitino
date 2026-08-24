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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Test;

public class TestAssociateTagAuthorizationExecutor {

  @Test
  public void testAuthorizesV1TagNames() throws Exception {
    TagsAssociateRequest request =
        new TagsAssociateRequest(new String[] {"pii"}, new String[] {"data_domain"});
    assertAuthorizesAllTags("associateV1", TagsAssociateRequest.class, request);
  }

  @Test
  public void testAuthorizesV2TagValues() throws Exception {
    TagValuesAssociateRequest request =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.noValue("pii")},
            new TagValue[] {TagValue.of("data_domain", "finance")});
    assertAuthorizesAllTags("associateV2", TagValuesAssociateRequest.class, request);
  }

  @Test
  public void testAuthorizesV2TagNamesWithoutValidatingValues() throws Exception {
    TagValuesAssociateRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":[{\"name\":\"data_domain\",\"value\":\" \"}]}",
                TagValuesAssociateRequest.class);
    Method method = TestOperations.class.getDeclaredMethod("associateV2", request.getClass());
    Map<Entity.EntityType, NameIdentifier> metadataContext = new HashMap<>();
    metadataContext.put(Entity.EntityType.METALAKE, NameIdentifier.of("metalake"));
    AssociateTagAuthorizationExecutor executor =
        new AssociateTagAuthorizationExecutor(
            "TAG::OWNER",
            method.getParameters(),
            new Object[] {request},
            metadataContext,
            Collections.emptyMap(),
            Optional.empty());

    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    when(evaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    executor.authorizationExpressionEvaluator = evaluator;

    assertTrue(executor.execute(context));
    verify(evaluator, times(1)).evaluate(anyMap(), anyMap(), any(), any());
  }

  private static void assertAuthorizesAllTags(
      String methodName, Class<?> requestType, Object request) throws Exception {
    Method method = TestOperations.class.getDeclaredMethod(methodName, requestType);
    Map<Entity.EntityType, NameIdentifier> metadataContext = new HashMap<>();
    metadataContext.put(Entity.EntityType.METALAKE, NameIdentifier.of("metalake"));
    AssociateTagAuthorizationExecutor executor =
        new AssociateTagAuthorizationExecutor(
            "TAG::OWNER",
            method.getParameters(),
            new Object[] {request},
            metadataContext,
            Collections.emptyMap(),
            Optional.empty());

    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    when(evaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    executor.authorizationExpressionEvaluator = evaluator;

    assertTrue(executor.execute(context));
    verify(evaluator, times(2)).evaluate(anyMap(), anyMap(), any(), any());
  }

  private static class TestOperations {
    private void associateV1(
        @AuthorizationRequest(type = AuthorizationRequest.RequestType.ASSOCIATE_TAG)
            TagsAssociateRequest request) {
      request.validate();
    }

    private void associateV2(
        @AuthorizationRequest(type = AuthorizationRequest.RequestType.ASSOCIATE_TAG)
            TagValuesAssociateRequest request) {
      request.validate();
    }
  }
}
