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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.requests.PolicySetRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AccessControlContent;
import org.apache.gravitino.policy.HiddenPolicyTag;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestAlterPolicyAuthorizationExecutor {
  private static final Privilege.Name SELECT = Privilege.Name.SELECT_TABLE;
  private static final Privilege.Name MODIFY = Privilege.Name.MODIFY_TABLE;

  @Test
  void testWideningComparesSets() {
    AccessControlContent old = content(List.of(SELECT, MODIFY), List.of("a", "b"));
    assertFalse(
        AlterPolicyAuthorizationExecutor.isWidening(
            old, content(List.of(MODIFY, SELECT, SELECT), List.of("b", "a", "a"))));
    assertFalse(
        AlterPolicyAuthorizationExecutor.isWidening(old, content(List.of(SELECT), List.of("a"))));
    assertTrue(
        AlterPolicyAuthorizationExecutor.isWidening(
            old, content(List.of(SELECT, MODIFY), List.of("a", "b", "c"))));
    assertTrue(
        AlterPolicyAuthorizationExecutor.isWidening(
            content(List.of(SELECT), List.of("a")),
            content(List.of(SELECT, MODIFY), List.of("a"))));
    assertTrue(
        AlterPolicyAuthorizationExecutor.isWidening(
            old, content(List.of(SELECT), List.of("a", "c"))));
  }

  @Test
  void testVisibleAndHiddenBindings() throws Exception {
    try (Scenario s = new Scenario()) {
      PolicyUpdatesRequest request =
          updates(update(content(List.of(SELECT, MODIFY), List.of("a"))));
      s.policy(false, Policy.BuiltInType.ACCESS_CONTROL);
      String hidden = HiddenPolicyTag.name(12);
      RelationalEntity<?> hiddenRelation = relation(hidden);
      RelationalEntity<?> visibleRelation = relation("visible");
      MetadataObject object = mock(MetadataObject.class);
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[0]);
      assertTrue(s.executor(request).execute(new AuthorizationRequestContext()));
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[] {hiddenRelation});
      when(s.tags.listMetadataObjectsForTag("lake", hidden)).thenReturn(new MetadataObject[0]);
      assertTrue(s.executor(request).execute(new AuthorizationRequestContext()));
      when(s.tags.listMetadataObjectsForTag("lake", hidden))
          .thenReturn(new MetadataObject[] {object});
      assertFalse(s.executor(request).execute(new AuthorizationRequestContext()));
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[] {visibleRelation});
      assertFalse(s.executor(request).execute(new AuthorizationRequestContext()));
      assertFalse(
          s.executor(updates(update(content(List.of(SELECT), List.of("a", "b")))))
              .execute(new AuthorizationRequestContext()));
      when(s.access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
      assertTrue(s.executor(request).execute(new AuthorizationRequestContext()));
    }
  }

  @Test
  void testEnableUsesBoundAccessPolicyClassification() throws Exception {
    try (Scenario s = new Scenario()) {
      PolicySetRequest enable = new PolicySetRequest(true);
      s.policy(false, Policy.BuiltInType.ACCESS_CONTROL);
      String hidden = HiddenPolicyTag.name(12);
      RelationalEntity<?> hiddenRelation = relation(hidden);
      RelationalEntity<?> visibleRelation = relation("visible");
      MetadataObject object = mock(MetadataObject.class);
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[] {hiddenRelation});
      when(s.tags.listMetadataObjectsForTag("lake", hidden)).thenReturn(new MetadataObject[0]);
      assertTrue(s.executor(enable).execute(new AuthorizationRequestContext()));
      when(s.tags.listMetadataObjectsForTag("lake", hidden))
          .thenReturn(new MetadataObject[] {object});
      assertFalse(s.executor(enable).execute(new AuthorizationRequestContext()));
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[] {visibleRelation});
      assertFalse(s.executor(enable).execute(new AuthorizationRequestContext()));
      when(s.access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
      assertTrue(s.executor(enable).execute(new AuthorizationRequestContext()));
      when(s.ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
      assertFalse(s.executor(enable).execute(new AuthorizationRequestContext()));
    }
  }

  @Test
  void testOrdinaryPolicyStateRequestsSkipBindingLookup() throws Exception {
    try (Scenario s = new Scenario()) {
      s.policy(false, Policy.BuiltInType.ACCESS_CONTROL);
      assertTrue(
          s.executor(new PolicySetRequest(false)).execute(new AuthorizationRequestContext()));
      s.policy(true, Policy.BuiltInType.ACCESS_CONTROL);
      assertTrue(s.executor(new PolicySetRequest(true)).execute(new AuthorizationRequestContext()));
      s.policy(false, Policy.BuiltInType.ICEBERG_COMPACTION);
      assertTrue(s.executor(new PolicySetRequest(true)).execute(new AuthorizationRequestContext()));
      verify(s.policies, never()).listTagAssociationsForPolicy(any(), any());
    }
  }

  @Test
  void testOnlyFinalContentControlsAdditionalAuthority() throws Exception {
    try (Scenario s = new Scenario()) {
      s.policy(true, Policy.BuiltInType.ACCESS_CONTROL);
      RelationalEntity<?> visibleRelation = relation("visible");
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenReturn(new RelationalEntity<?>[] {visibleRelation});
      PolicyUpdateRequest wider = update(content(List.of(SELECT, MODIFY), List.of("a", "b")));
      PolicyUpdateRequest original = update(content(List.of(SELECT), List.of("a")));
      assertTrue(s.executor(updates(wider, original)).execute(new AuthorizationRequestContext()));
      assertFalse(s.executor(updates(original, wider)).execute(new AuthorizationRequestContext()));
      assertFalse(
          s.executor(updates(new PolicyUpdateRequest.RenamePolicyRequest("renamed"), wider))
              .execute(new AuthorizationRequestContext()));
      verify(s.policies, never()).alterPolicy(any(), any(), any());
    }
  }

  @Test
  void testInvalidUpdatesRemainForResourceValidation() throws Exception {
    try (Scenario s = new Scenario()) {
      s.policy(true, Policy.BuiltInType.ACCESS_CONTROL);
      PolicyUpdatesRequest invalid = updates(update(content(List.of(), List.of("a"))));
      assertTrue(s.executor(invalid).execute(new AuthorizationRequestContext()));
      assertThrows(IllegalArgumentException.class, invalid::validate);
      PolicyUpdateRequest typeMismatch =
          new PolicyUpdateRequest.UpdatePolicyContentRequest(
              "custom",
              DTOConverters.toDTO(
                  PolicyContents.custom(Map.of(), Set.of(MetadataObject.Type.TABLE), Map.of())));
      assertTrue(s.executor(updates(typeMismatch)).execute(new AuthorizationRequestContext()));
      verify(s.policies, never()).listTagAssociationsForPolicy(any(), any());
    }
  }

  @Test
  void testDisabledAuthorizationAvoidsClassification() throws Exception {
    try (Scenario s = new Scenario()) {
      AlterPolicyAuthorizationExecutor executor =
          s.executor(updates(update(content(List.of(SELECT, MODIFY), List.of("a")))));
      executor.authorizationDisabled = true;
      assertTrue(executor.execute(new AuthorizationRequestContext()));
      verifyNoInteractions(s.policies);
    }
  }

  @Test
  void testOtherEditsAndGovernanceRetainOriginalAuthority() throws Exception {
    try (Scenario s = new Scenario()) {
      s.policy(true, Policy.BuiltInType.ACCESS_CONTROL);
      assertTrue(
          s.executor(updates(new PolicyUpdateRequest.RenamePolicyRequest("renamed")))
              .execute(new AuthorizationRequestContext()));
      verifyNoInteractions(s.policies);
      s.policy(true, Policy.BuiltInType.CUSTOM);
      PolicyUpdateRequest governance =
          new PolicyUpdateRequest.UpdatePolicyContentRequest(
              "custom",
              DTOConverters.toDTO(
                  PolicyContents.custom(Map.of(), Set.of(MetadataObject.Type.TABLE), Map.of())));
      assertTrue(s.executor(updates(governance)).execute(new AuthorizationRequestContext()));
    }
  }

  @Test
  void testReadFailureAndOriginalAuthorizationFailure() throws Exception {
    try (Scenario s = new Scenario()) {
      PolicyUpdatesRequest request =
          updates(update(content(List.of(SELECT, MODIFY), List.of("a"))));
      s.policy(true, Policy.BuiltInType.ACCESS_CONTROL);
      when(s.policies.listTagAssociationsForPolicy("lake", "policy"))
          .thenThrow(new IllegalStateException("read failed"));
      assertThrows(
          IllegalStateException.class,
          () -> s.executor(request).execute(new AuthorizationRequestContext()));
      String hidden = HiddenPolicyTag.name(12);
      doReturn(new RelationalEntity<?>[] {relation(hidden)})
          .when(s.policies)
          .listTagAssociationsForPolicy("lake", "policy");
      when(s.tags.listMetadataObjectsForTag("lake", hidden))
          .thenThrow(new IllegalStateException("tag read failed"));
      assertThrows(
          IllegalStateException.class,
          () -> s.executor(request).execute(new AuthorizationRequestContext()));
      when(s.ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
      assertFalse(s.executor(request).execute(new AuthorizationRequestContext()));
    }
  }

  private static AccessControlContent content(List<Privilege.Name> privileges, List<String> roles) {
    return (AccessControlContent) PolicyContents.accessControl(privileges, roles);
  }

  private static PolicyUpdateRequest update(AccessControlContent content) {
    return new PolicyUpdateRequest.UpdatePolicyContentRequest(
        Policy.BuiltInType.ACCESS_CONTROL.policyType(), DTOConverters.toDTO(content));
  }

  private static PolicyUpdatesRequest updates(PolicyUpdateRequest... changes) {
    return new PolicyUpdatesRequest(List.of(changes));
  }

  private static RelationalEntity<?> relation(String tagName) {
    RelationalEntity<?> relation = mock(RelationalEntity.class);
    TagEntity tag = mock(TagEntity.class);
    when(tag.name()).thenReturn(tagName);
    doReturn(tag).when(relation).targetEntity();
    return relation;
  }

  private static class TestOperations {
    private void alter(@AuthorizationRequest PolicyUpdatesRequest request) {
      request.validate();
    }

    private void set(@AuthorizationRequest PolicySetRequest request) {
      request.validate();
    }
  }

  private static class Scenario implements AutoCloseable {
    private final MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class);
    private final GravitinoEnv env = mock(GravitinoEnv.class);
    private final PolicyDispatcher policies = mock(PolicyDispatcher.class);
    private final TagDispatcher tags = mock(TagDispatcher.class);
    private final PolicyEntity policy = mock(PolicyEntity.class);
    private final AuthorizationExpressionEvaluator ordinary =
        mock(AuthorizationExpressionEvaluator.class);
    private final AuthorizationExpressionEvaluator access =
        mock(AuthorizationExpressionEvaluator.class);

    private Scenario() {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      when(env.policyDispatcher()).thenReturn(policies);
      when(env.tagDispatcher()).thenReturn(tags);
      when(policies.getPolicy("lake", "policy")).thenReturn(policy);
      when(ordinary.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(true);
      when(access.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(false);
    }

    private void policy(boolean enabled, Policy.BuiltInType type) {
      when(policy.enabled()).thenReturn(enabled);
      when(policy.policyType()).thenReturn(type);
      when(policy.content()).thenReturn(content(List.of(SELECT), List.of("a")));
    }

    private AlterPolicyAuthorizationExecutor executor(PolicyUpdatesRequest request)
        throws Exception {
      return executor(
          TestOperations.class.getDeclaredMethod("alter", PolicyUpdatesRequest.class), request);
    }

    private AlterPolicyAuthorizationExecutor executor(PolicySetRequest request) throws Exception {
      return executor(
          TestOperations.class.getDeclaredMethod("set", PolicySetRequest.class), request);
    }

    private AlterPolicyAuthorizationExecutor executor(Method method, Object request) {
      AlterPolicyAuthorizationExecutor executor =
          new AlterPolicyAuthorizationExecutor(
              "METALAKE::OWNER || POLICY::OWNER",
              Map.of(
                  Entity.EntityType.METALAKE,
                  NameIdentifier.of("lake"),
                  Entity.EntityType.POLICY,
                  NameIdentifier.of("lake", "policy")),
              Collections.emptyMap(),
              Optional.empty(),
              method.getParameters(),
              new Object[] {request});
      executor.authorizationDisabled = false;
      executor.authorizationExpressionEvaluator = ordinary;
      executor.accessPolicyEvaluator = access;
      return executor;
    }

    @Override
    public void close() {
      environments.close();
    }
  }
}
