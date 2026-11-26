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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestAssociateTagAuthorizationExecutor {

  @Test
  void testAuthorizationDisabledDoesNotReadPolicies() throws Exception {
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    when(provider.getGravitinoAuthorizer()).thenReturn(new PassThroughAuthorizer());
    try (MockedStatic<GravitinoAuthorizerProvider> providers =
        mockStatic(GravitinoAuthorizerProvider.class)) {
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      TagsAssociateRequest request = new TagsAssociateRequest(new String[] {"access"}, null);
      Method method = TestOperations.class.getDeclaredMethod("associateV1", request.getClass());
      AssociateTagAuthorizationExecutor executor =
          new AssociateTagAuthorizationExecutor(
              "METALAKE::OWNER",
              method.getParameters(),
              new Object[] {request},
              Map.of(Entity.EntityType.METALAKE, NameIdentifier.of("metalake")),
              Collections.emptyMap(),
              Optional.empty());
      executor.accessTagClassifier =
          (metalake, tag) -> {
            throw new IllegalStateException("unexpected read");
          };
      assertTrue(executor.execute(new AuthorizationRequestContext()));
    }
  }

  @Test
  void testClassificationFailureStopsAuthorization() throws Exception {
    AssociateTagAuthorizationExecutor executor =
        enabledExecutor("associateV1", new TagsAssociateRequest(new String[] {"access"}, null));
    executor.accessTagClassifier =
        (metalake, tag) -> {
          throw new IllegalStateException("unavailable");
        };
    assertThrows(
        IllegalStateException.class, () -> executor.execute(new AuthorizationRequestContext()));
  }

  @Test
  void testAccessExpressionRequiresTagGrantAndObjectAuthority() {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator(APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION, authorizer);
    Map<Entity.EntityType, NameIdentifier> metadata =
        Map.of(
            Entity.EntityType.METALAKE,
            NameIdentifier.of("lake"),
            Entity.EntityType.TAG,
            NameIdentifierUtil.ofTag("lake", "sensitive"));
    Map<String, Object> path =
        Map.of("p_metalake", "lake", "p_type", "TABLE", "p_fullName", "c.s.t");
    when(authorizer.authorize(
            nullable(Principal.class), any(), any(), eq(Privilege.Name.APPLY_TAG), any()))
        .thenReturn(true);
    assertFalse(
        evaluator.evaluate(metadata, path, new AuthorizationRequestContext(), Optional.empty()));
    when(authorizer.hasMetadataPrivilegePermission(any(), any(), any(), any())).thenReturn(true);
    assertTrue(
        evaluator.evaluate(metadata, path, new AuthorizationRequestContext(), Optional.empty()));
    when(authorizer.deny(
            nullable(Principal.class), any(), any(), eq(Privilege.Name.APPLY_TAG), any()))
        .thenReturn(true);
    assertFalse(
        evaluator.evaluate(metadata, path, new AuthorizationRequestContext(), Optional.empty()));
    AuthorizationExpressionEvaluator disabled =
        new AuthorizationExpressionEvaluator(
            APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION, new PassThroughAuthorizer());
    assertTrue(
        disabled.evaluate(metadata, path, new AuthorizationRequestContext(), Optional.empty()));
  }

  @Test
  void testPolicyClassification() {
    PolicyEntity access = mock(PolicyEntity.class);
    PolicyEntity governance = mock(PolicyEntity.class);
    when(access.policyType()).thenReturn(Policy.BuiltInType.ACCESS_CONTROL);
    when(governance.policyType()).thenReturn(Policy.BuiltInType.CUSTOM);
    RelationalEntity<?> accessRelation = mock(RelationalEntity.class);
    RelationalEntity<?> governanceRelation = mock(RelationalEntity.class);
    doReturn(access).when(accessRelation).targetEntity();
    doReturn(governance).when(governanceRelation).targetEntity();

    assertFalse(AssociateTagAuthorizationExecutor.isAccessTag(new RelationalEntity<?>[0]));
    assertFalse(
        AssociateTagAuthorizationExecutor.isAccessTag(
            new RelationalEntity<?>[] {governanceRelation}));
    assertTrue(
        AssociateTagAuthorizationExecutor.isAccessTag(
            new RelationalEntity<?>[] {governanceRelation, accessRelation}));
    RelationalEntity<PolicyEntity> disabledWithSelector =
        new RelationalEntity<>(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            NameIdentifierUtil.ofTag("lake", "sensitive"),
            Entity.EntityType.TAG,
            access,
            PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("finance")));
    assertFalse(access.enabled());
    assertTrue(
        AssociateTagAuthorizationExecutor.isAccessTag(
            new RelationalEntity<?>[] {disabledWithSelector}));
  }

  @Test
  void testAccessTagUsesStrongerAuthorization() throws Exception {
    AssociateTagAuthorizationExecutor executor =
        enabledExecutor("associateV1", new TagsAssociateRequest(new String[] {"access"}, null));
    executor.accessTagClassifier = (metalake, tag) -> true;
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    executor.accessTagEvaluator = evaluator;
    when(evaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(executor.execute(context));
    assertEquals(
        APPLY_ACCESS_TAG_AUTHORIZATION_EXPRESSION, context.getOriginalAuthorizationExpression());
    verify(evaluator).evaluate(anyMap(), anyMap(), any(), any());
  }

  @Test
  void testMixedBatchIsRejectedWhenAccessTagIsDenied() throws Exception {
    AssociateTagAuthorizationExecutor executor =
        enabledExecutor(
            "associateV1", new TagsAssociateRequest(new String[] {"descriptive", "access"}, null));
    executor.accessTagClassifier = (metalake, tag) -> tag.equals("access");
    AuthorizationExpressionEvaluator ordinary = mock(AuthorizationExpressionEvaluator.class);
    AuthorizationExpressionEvaluator access = mock(AuthorizationExpressionEvaluator.class);
    executor.authorizationExpressionEvaluator = ordinary;
    executor.accessTagEvaluator = access;
    when(ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    when(access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    assertFalse(executor.execute(new AuthorizationRequestContext()));
    verify(ordinary).evaluate(anyMap(), anyMap(), any(), any());
    verify(access).evaluate(anyMap(), anyMap(), any(), any());
  }

  @Test
  void testDisabledPolicyWithNonmatchingSelectorStillProtectsValue() throws Exception {
    PolicyEntity access = mock(PolicyEntity.class);
    when(access.policyType()).thenReturn(Policy.BuiltInType.ACCESS_CONTROL);
    RelationalEntity<PolicyEntity> relation =
        new RelationalEntity<>(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            NameIdentifierUtil.ofTag("metalake", "sensitive"),
            Entity.EntityType.TAG,
            access,
            PolicyAssociationSelectorSerde.serialize(TagValueSelector.of("finance")));
    AssociateTagAuthorizationExecutor executor =
        enabledExecutor(
            "associateV2",
            new TagValuesAssociateRequest(new TagValue[] {TagValue.of("sensitive", "risk")}, null));
    executor.accessTagClassifier =
        (metalake, tag) ->
            AssociateTagAuthorizationExecutor.isAccessTag(new RelationalEntity<?>[] {relation});
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    executor.accessTagEvaluator = evaluator;
    when(evaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    assertFalse(access.enabled());
    assertFalse(executor.execute(new AuthorizationRequestContext()));
    verify(evaluator).evaluate(anyMap(), anyMap(), any(), any());
  }

  @Test
  public void testAuthorizesV1TagNames() throws Exception {
    TagsAssociateRequest request =
        new TagsAssociateRequest(new String[] {"pii"}, new String[] {"data_domain"});
    assertAuthorizesAllTags("associateV1", request);
  }

  @Test
  public void testAuthorizesV2TagValues() throws Exception {
    TagValuesAssociateRequest request =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.noValue("pii")},
            new TagValue[] {TagValue.of("data_domain", "finance")});
    assertAuthorizesAllTags("associateV2", request);
  }

  @Test
  void testInvalidV2ValuesSkipClassification() throws Exception {
    TagValuesAssociateRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":[{\"name\":\"data_domain\",\"value\":\" \"}]}",
                TagValuesAssociateRequest.class);
    assertAuthorizesAllTags("associateV2", request, 1);
  }

  private static void assertAuthorizesAllTags(String methodName, Object request) throws Exception {
    assertAuthorizesAllTags(methodName, request, 2);
  }

  private static void assertAuthorizesAllTags(
      String methodName, Object request, int expectedEvaluations) throws Exception {
    AssociateTagAuthorizationExecutor executor = enabledExecutor(methodName, request);
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    when(evaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
    executor.authorizationExpressionEvaluator = evaluator;
    executor.accessTagClassifier = (metalake, tag) -> false;

    assertTrue(executor.execute(new AuthorizationRequestContext()));
    verify(evaluator, times(expectedEvaluations)).evaluate(anyMap(), anyMap(), any(), any());
  }

  private static AssociateTagAuthorizationExecutor enabledExecutor(
      String methodName, Object request) throws Exception {
    Method method = TestOperations.class.getDeclaredMethod(methodName, request.getClass());
    AssociateTagAuthorizationExecutor executor =
        new AssociateTagAuthorizationExecutor(
            "TAG::OWNER",
            method.getParameters(),
            new Object[] {request},
            Map.of(Entity.EntityType.METALAKE, NameIdentifier.of("metalake")),
            Collections.emptyMap(),
            Optional.empty());
    executor.authorizationDisabled = false;
    return executor;
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
