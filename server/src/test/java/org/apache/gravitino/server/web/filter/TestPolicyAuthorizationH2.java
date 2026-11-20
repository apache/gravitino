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
package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.dto.policy.PolicyAssociationSelectorDTO;
import org.apache.gravitino.dto.requests.PolicySetRequest;
import org.apache.gravitino.dto.requests.PolicyTagAddRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.HiddenPolicyTag;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyEditAuthorization;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.policy.hook.PolicyLifecycleHook;
import org.apache.gravitino.policy.hook.PolicyLifecycleHooks;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.server.web.rest.PolicyOperations;
import org.apache.gravitino.server.web.rest.TagOperations;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogListener;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.LiveEndpointService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

/** Verifies denied policy changes against persisted H2 metadata. */
@Tag("server-policy-h2-test")
public class TestPolicyAuthorizationH2 extends TestJDBCBackend {
  @TestTemplate
  void testDeniedMixedUpdatePreservesStoredPolicyAndBindings() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    String renamed = "renamed";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent initial = selectContent();
    PolicyContent wider = selectModifyContent();
    PolicyEntity policy =
        putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, initial);

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      overrides.setPolicyAndTagDispatchers(policies, tags).set("eventBus", mock(EventBus.class));
      GravitinoAuthorizer authorizer = security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request =
          new PolicyUpdatesRequest(
              List.of(new PolicyUpdateRequest.RenamePolicyRequest(renamed), contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> operations.alterPolicy(metalake, policyName, request));
      Map<Integer, Long> versions = listPolicyVersions(policy.id());
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertTrue(bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(403, denied.getStatus());
      PolicyEntity unchanged = policies.getPolicy(metalake, policyName);
      assertEquals(policyName, unchanged.name());
      assertEquals(initial, unchanged.content());
      assertEquals(false, unchanged.enabled());
      assertEquals(bindings, bindingNames(policies, metalake, policyName));
      assertEquals(versions, listPolicyVersions(policy.id()));
      assertThrows(NoSuchPolicyException.class, () -> policies.getPolicy(metalake, renamed));

      doReturn(true).when(authorizer).isOwner(any(), any(), any(), any());
      Response allowed = intercept(method, invocation);
      assertEquals(200, allowed.getStatus());
      PolicyEntity updated = policies.getPolicy(metalake, renamed);
      assertEquals(wider, updated.content());
      assertEquals(false, updated.enabled());
      assertEquals(bindings, bindingNames(policies, metalake, renamed));
    }
  }

  @TestTemplate
  void testConcurrentNarrowingInvalidatesAuthorization() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent initial = selectContent();
    PolicyContent wider = selectModifyContent();
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, wider);

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      overrides.setPolicyAndTagDispatchers(policies, tags).set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () ->
                        policies.alterPolicy(
                            metalake,
                            policyName,
                            PolicyChange.updateContent(
                                Policy.BuiltInType.ACCESS_CONTROL.policyType(), initial)));
                assertEquals(initial, policies.getPolicy(metalake, policyName).content());
                return operations.alterPolicy(metalake, policyName, request);
              });
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertTrue(bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(initial, policies.getPolicy(metalake, policyName).content());
      assertEquals(409, denied.getStatus(), "stale authorization must not permit widening");
    }
  }

  @TestTemplate
  void testConcurrentBindingInvalidatesAuthorization() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent initial = selectContent();
    PolicyContent wider = selectModifyContent();
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, initial);

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags).set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () ->
                        tags.addPolicyForTag(
                            metalake, "visible", policyName, AllValuesSelector.get()));
                assertEquals(initial, policies.getPolicy(metalake, policyName).content());
                return operations.alterPolicy(metalake, policyName, request);
              });
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertTrue(!bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(initial, policies.getPolicy(metalake, policyName).content());
      assertEquals(409, denied.getStatus(), "stale authorization must not permit widening");
    }
  }

  @TestTemplate
  void testConcurrentBindInvalidatesEnableAuthorization() throws Throwable {
    assertEnableRace(false);
  }

  @TestTemplate
  void testConcurrentDisableInvalidatesEnableNoOp() throws Throwable {
    assertEnableRace(true);
  }

  @TestTemplate
  void testReplacedPolicyInvalidatesGovernanceBindAuthorization() throws Throwable {
    String metalake = "bind_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(
        store,
        metalake,
        policyName,
        Policy.BuiltInType.ICEBERG_COMPACTION,
        PolicyContents.icebergDataCompaction());
    PolicyEntity replacement =
        newPolicy(metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags);
      security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.POLICY);
      PolicyTagAddRequest request =
          new PolicyTagAddRequest(
              PolicyAssociationSelectorDTO.fromSelector(AllValuesSelector.get()));
      TagOperations operations = withHttpRequest(new TagOperations(tags));
      Method method = addPolicyForTagMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "visible", policyName, request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () -> {
                      policies.deletePolicy(metalake, policyName);
                      store.put(replacement, false);
                    });
                return operations.addPolicyForTag(metalake, "visible", policyName, request);
              });
      Response response = intercept(method, invocation);
      assertEquals(409, response.getStatus());
      assertTrue(!bindingNames(policies, metalake, policyName).contains("visible"));
      assertEquals(
          Policy.BuiltInType.ACCESS_CONTROL, policies.getPolicy(metalake, policyName).policyType());
    }
  }

  @TestTemplate
  void testReplacedPolicyInvalidatesGovernanceUnbindAuthorization() throws Throwable {
    String metalake = "unbind_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(
        store,
        metalake,
        policyName,
        Policy.BuiltInType.ICEBERG_COMPACTION,
        PolicyContents.icebergDataCompaction());
    PolicyEntity replacement =
        newPolicy(metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags);
      security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.POLICY);
      tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      TagOperations operations = withHttpRequest(new TagOperations(tags));
      Method method = removePolicyFromTagMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "visible", policyName},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () -> {
                      policies.deletePolicy(metalake, policyName);
                      store.put(replacement, false);
                      tags.addPolicyForTag(
                          metalake, "visible", policyName, AllValuesSelector.get());
                    });
                return operations.removePolicyFromTag(metalake, "visible", policyName);
              });
      Response response = intercept(method, invocation);
      assertEquals(409, response.getStatus());
      assertTrue(bindingNames(policies, metalake, policyName).contains("visible"));
      assertEquals(
          Policy.BuiltInType.ACCESS_CONTROL, policies.getPolicy(metalake, policyName).policyType());
    }
  }

  @TestTemplate
  void testUnboundWideningStillAllowed() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent wider = selectModifyContent();
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags).set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> operations.alterPolicy(metalake, policyName, request));
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertEquals(1, bindings.size());
      assertTrue(!bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(wider, policies.getPolicy(metalake, policyName).content());
      assertEquals(200, denied.getStatus());
    }
  }

  @TestTemplate
  void testHiddenTagAssignmentMakesPolicyBound() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent initial = selectContent();
    PolicyContent wider = selectModifyContent();
    PolicyEntity policy =
        putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, initial);

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("internalCatalogDispatcher", existingCatalogs());
      tags.associateTagsForMetadataObject(
          metalake,
          MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG),
          new String[] {HiddenPolicyTag.name(policy.id())},
          new String[0]);
      overrides.set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> operations.alterPolicy(metalake, policyName, request));
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertTrue(!bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(initial, policies.getPolicy(metalake, policyName).content());
      assertEquals(403, denied.getStatus());
      assertEquals(403, setEnabled(operations, metalake, policyName, true).getStatus());
      assertFalse(policies.getPolicy(metalake, policyName).enabled());
    }
  }

  @TestTemplate
  void testConcurrentHiddenTagAssignmentInvalidatesWidening() throws Throwable {
    String metalake = "policy_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);

    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent initial = selectContent();
    PolicyContent wider = selectModifyContent();
    PolicyEntity policy =
        putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, initial);

    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("internalCatalogDispatcher", existingCatalogs())
          .set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);

      PolicyUpdatesRequest request = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = alterPolicyMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () ->
                        tags.associateTagsForMetadataObject(
                            metalake,
                            MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG),
                            new String[] {HiddenPolicyTag.name(policy.id())},
                            new String[0]));
                return operations.alterPolicy(metalake, policyName, request);
              });
      Set<String> bindings = bindingNames(policies, metalake, policyName);
      assertTrue(!bindings.contains("visible"));

      Response denied = intercept(method, invocation);
      assertEquals(initial, policies.getPolicy(metalake, policyName).content());
      assertEquals(409, denied.getStatus());
    }
  }

  @TestTemplate
  void testUnboundEditBeforeBindSerializes() throws Throwable {
    String metalake = "unbound_edit_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyContent wider = selectModifyContent();
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    CountDownLatch inTransaction = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch bindingStarted = new CountDownLatch(1);
    PolicyLifecycleHook hook =
        new PolicyLifecycleHook() {
          @Override
          public void beforeUpdate(PolicyEntity ignored) {
            inTransaction.countDown();
            try {
              if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to release update");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }
        };
    ExecutorService writers = Executors.newFixedThreadPool(2);
    PolicyLifecycleHooks.register(hook);
    try {
      // H2 locks the metalake exclusively; omit that lock to exercise shared-lock backends.
      Future<?> edit =
          writers.submit(
              () -> {
                try (MockedStatic<PrincipalUtils> auth = mockStatic(PrincipalUtils.class);
                    MockedStatic<LiveEndpointService> endpoints =
                        mockStatic(LiveEndpointService.class);
                    PolicyEditAuthorization scope = PolicyEditAuthorization.open()) {
                  endpoints
                      .when(() -> LiveEndpointService.lockLiveEndpoint(any(), any(), any()))
                      .thenAnswer(
                          i ->
                              i.getArgument(1) == Entity.EntityType.METALAKE
                                  ? null
                                  : i.callRealMethod());
                  auth.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
                  auth.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
                  PolicyEditAuthorization.record(policies.getPolicy(metalake, policyName));
                  PolicyEditAuthorization.requireUnbound();
                  policies.alterPolicy(
                      metalake,
                      policyName,
                      PolicyChange.updateContent(
                          Policy.BuiltInType.ACCESS_CONTROL.policyType(), wider));
                }
              });
      assertTrue(inTransaction.await(10, TimeUnit.SECONDS));
      Future<?> bind =
          writers.submit(
              () -> {
                try (MockedStatic<PrincipalUtils> auth = mockStatic(PrincipalUtils.class);
                    MockedStatic<LiveEndpointService> endpoints =
                        mockStatic(LiveEndpointService.class)) {
                  endpoints
                      .when(() -> LiveEndpointService.lockLiveEndpoint(any(), any(), any()))
                      .thenAnswer(
                          i ->
                              i.getArgument(1) == Entity.EntityType.METALAKE
                                  ? null
                                  : i.callRealMethod());
                  auth.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
                  auth.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
                  bindingStarted.countDown();
                  tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
                }
              });
      assertTrue(bindingStarted.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> bind.get(300, TimeUnit.MILLISECONDS));
      release.countDown();
      edit.get(10, TimeUnit.SECONDS);
      bind.get(10, TimeUnit.SECONDS);
      assertEquals(wider, policies.getPolicy(metalake, policyName).content());
      assertTrue(bindingNames(policies, metalake, policyName).contains("visible"));
    } finally {
      release.countDown();
      writers.shutdownNow();
      PolicyLifecycleHooks.unregister(hook);
    }
  }

  @TestTemplate
  void testActualJcasbinPolicyAuthorization() throws Throwable {
    String metalake = "real_policy_auth_h2";
    String foreignMetalake = "foreign_policy_auth_h2";
    createAndInsertMakeLake(metalake);
    createAndInsertMakeLake(foreignMetalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);
    createAndInsertTagEntity("fresh", "", metalake);
    createAndInsertTagEntity("label", "", metalake);
    RelationalEntityStore store =
        new RelationalEntityStore() {
          @Override
          public void registerEntityChangeLogListener(EntityChangeLogListener listener) {}

          @Override
          public void unregisterEntityChangeLogListener(EntityChangeLogListener listener) {}
        };
    Config config = new Config(false) {};
    config.set(Configs.ENABLE_AUTHORIZATION, true);
    config.set(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS, 3600L);
    wireStore(store, config);
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    PolicyEntity policy =
        putPolicy(store, metalake, "policy", Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    putPolicy(
        store,
        foreignMetalake,
        "foreign_policy",
        Policy.BuiltInType.ACCESS_CONTROL,
        policy.content());
    putPolicy(
        store,
        metalake,
        "governance",
        Policy.BuiltInType.ICEBERG_COMPACTION,
        PolicyContents.icebergDataCompaction());
    long roleId =
        putRole(
            store,
            metalake,
            "role",
            List.of(
                SecurableObjects.ofTag("visible", List.of(Privileges.ApplyTag.allow())),
                SecurableObjects.ofMetalake(metalake, List.of(Privileges.CreatePolicy.allow())),
                SecurableObjects.ofCatalog(
                    "catalog",
                    List.of(Privileges.ManageGrants.allow(), Privileges.UseCatalog.allow()))));
    long governanceRoleId =
        putRole(
            store,
            metalake,
            "governanceRole",
            List.of(SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow()))));
    long viewerRoleId =
        putRole(
            store,
            metalake,
            "labelViewer",
            List.of(SecurableObjects.ofTag("label", List.of(Privileges.ViewTag.allow()))));
    putUser(store, metalake, "allowed", List.of(roleId), List.of("role"));
    putUser(store, metalake, "denied", List.of(), List.of());
    putUser(store, metalake, "metalakeOwner", List.of(), List.of());
    long creatorRoleId =
        putRole(
            store,
            metalake,
            "createPolicy",
            List.of(
                SecurableObjects.ofMetalake(metalake, List.of(Privileges.CreatePolicy.allow()))));
    long tagSideId =
        putRole(
            store,
            metalake,
            "tagSide",
            List.of(
                SecurableObjects.ofTag("fresh", List.of(Privileges.ApplyTag.allow())),
                SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow()))));
    long policySideId =
        putRole(
            store,
            metalake,
            "policySide",
            List.of(SecurableObjects.ofPolicy("policy", List.of(Privileges.ApplyPolicy.allow()))));
    long policyDenyId =
        putRole(
            store,
            metalake,
            "policyDeny",
            List.of(SecurableObjects.ofPolicy("policy", List.of(Privileges.ApplyPolicy.deny()))));
    putUser(store, metalake, "creator", List.of(creatorRoleId), List.of("createPolicy"));
    putUser(store, metalake, "tagSide", List.of(tagSideId), List.of("tagSide"));
    putUser(store, metalake, "policySide", List.of(policySideId), List.of("policySide"));
    putUser(
        store,
        metalake,
        "association",
        List.of(tagSideId, policySideId),
        List.of("tagSide", "policySide"));
    putUser(
        store,
        metalake,
        "associationDenied",
        List.of(tagSideId, policySideId, policyDenyId),
        List.of("tagSide", "policySide", "policyDeny"));
    putUser(store, metalake, "governor", List.of(governanceRoleId), List.of("governanceRole"));
    putUser(store, metalake, "viewer", List.of(viewerRoleId), List.of("labelViewer"));
    NameIdentifier policyId = NameIdentifierUtil.ofPolicy(metalake, "policy");
    NameIdentifier freshId = NameIdentifierUtil.ofTag(metalake, "fresh");
    setOwner(metalake, NameIdentifier.of(metalake), Entity.EntityType.METALAKE, "metalakeOwner");
    setOwner(metalake, policyId, Entity.EntityType.POLICY, "allowed");
    setOwner(metalake, freshId, Entity.EntityType.TAG, "allowed");
    setOwner(
        metalake, NameIdentifierUtil.ofTag(metalake, "label"), Entity.EntityType.TAG, "governor");
    setOwner(
        metalake,
        NameIdentifierUtil.ofPolicy(metalake, "governance"),
        Entity.EntityType.POLICY,
        "governor");
    GravitinoEnv env = GravitinoEnv.getInstance();
    try (EnvOverride overrides =
            new EnvOverride(
                "config",
                "entityStore",
                "gravitinoAuthorizer",
                "policyDispatcher",
                "tagDispatcher",
                "internalTagDispatcher",
                "internalCatalogDispatcher",
                "eventBus");
        JcasbinAuthorizer authorizer = new JcasbinAuthorizer();
        MockedStatic<PrincipalUtils> principals = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providers =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      overrides
          .set("config", config)
          .set("entityStore", store)
          .setPolicyAndTagDispatchers(policies, tags)
          .set("eventBus", mock(EventBus.class))
          .set("internalCatalogDispatcher", existingCatalogs());
      AtomicReference<UserPrincipal> current = new AtomicReference<>(new UserPrincipal("allowed"));
      principals.when(PrincipalUtils::getCurrentPrincipal).thenAnswer(i -> current.get());
      principals.when(PrincipalUtils::getCurrentUserName).thenAnswer(i -> current.get().getName());
      principals.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
      tags.addPolicyForTag(metalake, "visible", "policy", AllValuesSelector.get());
      authorizer.initialize();
      env.setGravitinoAuthorizer(authorizer);
      assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      TagOperations legacy = withHttpRequest(new TagOperations(tags));
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      PolicyTagAddRequest bind =
          new PolicyTagAddRequest(
              PolicyAssociationSelectorDTO.fromSelector(AllValuesSelector.get()));
      Method bindMethod = addPolicyForTagMethod();
      MethodInvocation bindInvocation =
          invocation(
              bindMethod,
              new Object[] {metalake, "fresh", "policy", bind},
              i -> legacy.addPolicyForTag(metalake, "fresh", "policy", bind));
      MethodInvocation foreignBind =
          invocation(bindMethod, new Object[] {metalake, "fresh", "foreign_policy", bind});
      current.set(new UserPrincipal("creator"));
      assertEquals(403, intercept(bindMethod, foreignBind).getStatus());
      assertFalse(bindingNames(policies, foreignMetalake, "foreign_policy").contains("fresh"));
      PolicyTagAddRequest invalidSelector = new PolicyTagAddRequest();
      MethodInvocation invalidBind =
          invocation(
              bindMethod,
              new Object[] {metalake, "fresh", "policy", invalidSelector},
              i -> legacy.addPolicyForTag(metalake, "fresh", "policy", invalidSelector));
      current.set(new UserPrincipal("allowed"));
      assertEquals(400, intercept(bindMethod, invalidBind).getStatus());
      assertFalse(bindingNames(policies, metalake, "policy").contains("fresh"));
      current.set(new UserPrincipal("denied"));
      assertEquals(403, intercept(bindMethod, bindInvocation).getStatus());
      assertEquals(
          403,
          intercept(
                  bindMethod,
                  invocation(bindMethod, new Object[] {metalake, "fresh", "missing", bind}))
              .getStatus());
      current.set(new UserPrincipal("metalakeOwner"));
      assertEquals(
          404,
          intercept(
                  bindMethod,
                  invocation(bindMethod, new Object[] {metalake, "fresh", "missing", bind}))
              .getStatus());
      assertFalse(bindingNames(policies, metalake, "policy").contains("fresh"));
      current.set(new UserPrincipal("creator"));
      assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
      assertEquals(200, intercept(bindMethod, bindInvocation).getStatus());
      assertTrue(bindingNames(policies, metalake, "policy").contains("fresh"));
      Method unbindMethod = removePolicyFromTagMethod();
      MethodInvocation unbindInvocation =
          invocation(
              unbindMethod,
              new Object[] {metalake, "fresh", "policy"},
              i -> legacy.removePolicyFromTag(metalake, "fresh", "policy"));
      for (String user : List.of("creator", "tagSide", "policySide", "associationDenied")) {
        current.set(new UserPrincipal(user));
        assertEquals(403, intercept(unbindMethod, unbindInvocation).getStatus(), user);
        assertTrue(bindingNames(policies, metalake, "policy").contains("fresh"));
      }
      current.set(new UserPrincipal("tagSide"));
      assertEquals(
          403,
          intercept(
                  unbindMethod,
                  invocation(unbindMethod, new Object[] {metalake, "fresh", "missing"}))
              .getStatus());
      current.set(new UserPrincipal("metalakeOwner"));
      assertEquals(
          404,
          intercept(
                  unbindMethod,
                  invocation(unbindMethod, new Object[] {metalake, "fresh", "missing"}))
              .getStatus());
      assertTrue(bindingNames(policies, metalake, "policy").contains("fresh"));
      current.set(new UserPrincipal("association"));
      TagsAssociateRequest addFresh =
          new TagsAssociateRequest(new String[] {"fresh"}, new String[0]);
      assertEquals(403, associateCatalogTags(operations, metalake, addFresh).getStatus());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "fresh").length);
      assertEquals(204, intercept(unbindMethod, unbindInvocation).getStatus());
      assertFalse(bindingNames(policies, metalake, "policy").contains("fresh"));
      assertEquals(403, intercept(bindMethod, bindInvocation).getStatus());
      assertFalse(bindingNames(policies, metalake, "policy").contains("fresh"));
      assertEquals(200, associateCatalogTags(operations, metalake, addFresh).getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "fresh").length);
      current.set(new UserPrincipal("creator"));
      assertEquals(200, intercept(bindMethod, bindInvocation).getStatus());
      assertTrue(bindingNames(policies, metalake, "policy").contains("fresh"));
      current.set(new UserPrincipal("metalakeOwner"));
      assertEquals(204, intercept(unbindMethod, unbindInvocation).getStatus());
      assertFalse(bindingNames(policies, metalake, "policy").contains("fresh"));
      PolicyContent initial = policy.content();
      PolicyContent wider = selectModifyContent();
      PolicyUpdatesRequest update = new PolicyUpdatesRequest(List.of(contentUpdate(wider)));
      PolicyOperations policyOps = withHttpRequest(new PolicyOperations(policies));
      Method editMethod = alterPolicyMethod();
      MethodInvocation editInvocation =
          invocation(
              editMethod,
              new Object[] {metalake, "policy", update},
              i -> policyOps.alterPolicy(metalake, "policy", update));
      current.set(new UserPrincipal("creator"));
      assertEquals(403, intercept(editMethod, editInvocation).getStatus());
      assertEquals(initial, policies.getPolicy(metalake, "policy").content());
      current.set(new UserPrincipal("allowed").withActiveRoles(ActiveRoles.none()));
      assertEquals(403, intercept(editMethod, editInvocation).getStatus());
      assertEquals(initial, policies.getPolicy(metalake, "policy").content());
      current.set(new UserPrincipal("allowed"));
      assertEquals(200, intercept(editMethod, editInvocation).getStatus());
      assertEquals(wider, policies.getPolicy(metalake, "policy").content());

      assertEquals(200, setEnabled(policyOps, metalake, "policy", false).getStatus());
      assertFalse(policies.getPolicy(metalake, "policy").enabled());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("allowed").withActiveRoles(ActiveRoles.none()));
      assertEquals(403, setEnabled(policyOps, metalake, "policy", true).getStatus());
      assertFalse(policies.getPolicy(metalake, "policy").enabled());
      current.set(new UserPrincipal("creator"));
      assertEquals(403, setEnabled(policyOps, metalake, "policy", true).getStatus());
      assertFalse(policies.getPolicy(metalake, "policy").enabled());
      current.set(new UserPrincipal("allowed"));
      assertEquals(200, setEnabled(policyOps, metalake, "policy", true).getStatus());
      assertTrue(policies.getPolicy(metalake, "policy").enabled());
      current.set(new UserPrincipal("allowed").withActiveRoles(ActiveRoles.none()));
      PolicyEntity enabled = policies.getPolicy(metalake, "policy");
      assertEquals(200, setEnabled(policyOps, metalake, "policy", true).getStatus());
      assertEquals(enabled.auditInfo(), policies.getPolicy(metalake, "policy").auditInfo());
      assertEquals(200, setEnabled(policyOps, metalake, "policy", false).getStatus());
      assertFalse(policies.getPolicy(metalake, "policy").enabled());
      current.set(new UserPrincipal("metalakeOwner"));
      assertEquals(200, setEnabled(policyOps, metalake, "policy", true).getStatus());
      assertTrue(policies.getPolicy(metalake, "policy").enabled());

      current.set(new UserPrincipal("allowed").withActiveRoles(ActiveRoles.none()));
      putPolicy(store, metalake, "unbound", Policy.BuiltInType.ACCESS_CONTROL, selectContent());
      setOwner(
          metalake,
          NameIdentifierUtil.ofPolicy(metalake, "unbound"),
          Entity.EntityType.POLICY,
          "allowed");
      assertEquals(200, setEnabled(policyOps, metalake, "unbound", false).getStatus());
      assertEquals(200, setEnabled(policyOps, metalake, "unbound", true).getStatus());
      assertTrue(policies.getPolicy(metalake, "unbound").enabled());

      current.set(new UserPrincipal("governor"));
      assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
      assertFalse(
          authorizer.authorize(
              current.get(),
              metalake,
              MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE),
              Privilege.Name.CREATE_POLICY,
              new AuthorizationRequestContext()));
      MethodInvocation governanceBind =
          invocation(
              bindMethod,
              new Object[] {metalake, "label", "governance", bind},
              i -> legacy.addPolicyForTag(metalake, "label", "governance", bind));
      assertEquals(200, intercept(bindMethod, governanceBind).getStatus());
      assertTrue(bindingNames(policies, metalake, "governance").contains("label"));
      assertEquals(200, setEnabled(policyOps, metalake, "governance", false).getStatus());
      assertEquals(200, setEnabled(policyOps, metalake, "governance", true).getStatus());
      assertTrue(policies.getPolicy(metalake, "governance").enabled());

      PolicyUpdatesRequest governanceUpdate =
          new PolicyUpdatesRequest(
              List.of(new PolicyUpdateRequest.UpdatePolicyCommentRequest("updated governance")));
      MethodInvocation governanceEdit =
          invocation(
              editMethod,
              new Object[] {metalake, "governance", governanceUpdate},
              i -> policyOps.alterPolicy(metalake, "governance", governanceUpdate));
      assertEquals(200, intercept(editMethod, governanceEdit).getStatus());
      assertEquals("updated governance", policies.getPolicy(metalake, "governance").comment());
      assertEquals(
          200,
          associateCatalogTags(
                  operations,
                  metalake,
                  new TagsAssociateRequest(new String[] {"label"}, new String[0]))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "label").length);

      Method listMethod =
          TagOperations.class.getMethod(
              "listPoliciesForTag", String.class, String.class, boolean.class);
      MethodInvocation listInvocation =
          invocation(
              listMethod,
              new Object[] {metalake, "label", false},
              i -> legacy.listPoliciesForTag(metalake, "label", false));
      Response ownerList = intercept(listMethod, listInvocation);
      assertEquals(200, ownerList.getStatus());
      assertTrue(
          Arrays.asList(((NameListResponse) ownerList.getEntity()).getNames())
              .contains("governance"));
      current.set(new UserPrincipal("viewer"));
      assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
      Response viewerList = intercept(listMethod, listInvocation);
      assertEquals(200, viewerList.getStatus());
      assertEquals(0, ((NameListResponse) viewerList.getEntity()).getNames().length);

      current.set(new UserPrincipal("governor"));
      MethodInvocation governanceUnbind =
          invocation(
              unbindMethod,
              new Object[] {metalake, "label", "governance"},
              i -> legacy.removePolicyFromTag(metalake, "label", "governance"));
      assertEquals(204, intercept(unbindMethod, governanceUnbind).getStatus());
      assertFalse(bindingNames(policies, metalake, "governance").contains("label"));
      assertEquals(200, intercept(bindMethod, governanceBind).getStatus());
    }
  }

  private void assertEnableRace(boolean alreadyEnabled) throws Throwable {
    String metalake = "enable_race_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags).set("eventBus", mock(EventBus.class));
      security.mockAuthorizer(MetadataObject.Type.POLICY);
      putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
      if (alreadyEnabled) {
        policies.enablePolicy(metalake, policyName);
        tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      }
      assertEquals(alreadyEnabled, policies.getPolicy(metalake, policyName).enabled());

      PolicyOperations operations = withHttpRequest(new PolicyOperations(policies));
      Method method = setPolicyMethod();
      PolicySetRequest request = new PolicySetRequest(true);
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, policyName, request},
              ignored -> {
                if (alreadyEnabled) {
                  runAsSecondWriter(principal, () -> policies.disablePolicy(metalake, policyName));
                } else {
                  runAsSecondWriter(
                      principal,
                      () ->
                          tags.addPolicyForTag(
                              metalake, "visible", policyName, AllValuesSelector.get()));
                }
                return operations.setPolicy(metalake, policyName, request);
              });
      assertEquals(409, intercept(method, invocation).getStatus());
      assertFalse(policies.getPolicy(metalake, policyName).enabled());
      assertTrue(bindingNames(policies, metalake, policyName).contains("visible"));
    }
  }

  private RelationalEntityStore newStore() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    wireStore(store, new Config(false) {});
    return store;
  }

  private void wireStore(RelationalEntityStore store, Config config) throws IllegalAccessException {
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
  }

  private static PolicyContent selectContent() {
    return PolicyContents.accessControl(List.of(Privilege.Name.SELECT_TABLE), List.of("reader"));
  }

  private static PolicyContent selectModifyContent() {
    return PolicyContents.accessControl(
        List.of(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE), List.of("reader"));
  }

  private static PolicyUpdateRequest contentUpdate(PolicyContent content) {
    return new PolicyUpdateRequest.UpdatePolicyContentRequest(
        Policy.BuiltInType.ACCESS_CONTROL.policyType(), DTOConverters.toDTO(content));
  }

  private static PolicyEntity newPolicy(
      String metalake, String name, Policy.BuiltInType type, PolicyContent content) {
    return PolicyEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withNamespace(Namespace.of(metalake))
        .withName(name)
        .withPolicyType(type)
        .withEnabled(false)
        .withContent(content)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static PolicyEntity putPolicy(
      RelationalEntityStore store,
      String metalake,
      String name,
      Policy.BuiltInType type,
      PolicyContent content)
      throws IOException {
    PolicyEntity policy = newPolicy(metalake, name, type, content);
    store.put(policy, false);
    return policy;
  }

  private static long putRole(
      RelationalEntityStore store, String metalake, String name, List<SecurableObject> objects)
      throws IOException {
    long id = RandomIdGenerator.INSTANCE.nextId();
    store.put(
        RoleEntity.builder()
            .withId(id)
            .withName(name)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
            .withProperties(null)
            .withSecurableObjects(objects)
            .withAuditInfo(AUDIT_INFO)
            .build(),
        false);
    return id;
  }

  private static void putUser(
      RelationalEntityStore store,
      String metalake,
      String name,
      List<Long> roleIds,
      List<String> roleNames)
      throws IOException {
    store.put(
        UserEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(AuthorizationUtils.ofUserNamespace(metalake))
            .withRoleIds(roleIds)
            .withRoleNames(roleNames)
            .withAuditInfo(AUDIT_INFO)
            .build(),
        false);
  }

  private static void setOwner(
      String metalake, NameIdentifier entity, Entity.EntityType type, String user) {
    OwnerMetaService.getInstance()
        .setOwner(entity, type, NameIdentifierUtil.ofUser(metalake, user), Entity.EntityType.USER);
  }

  private static CatalogDispatcher existingCatalogs() {
    CatalogDispatcher catalogs = mock(CatalogDispatcher.class);
    when(catalogs.catalogExists(any())).thenReturn(true);
    return catalogs;
  }

  private static Answer<Boolean> ownerOf(MetadataObject.Type... ownedTypes) {
    List<MetadataObject.Type> owned = List.of(ownedTypes);
    return invocation -> owned.contains(((MetadataObject) invocation.getArgument(2)).type());
  }

  private static <T> T withHttpRequest(T resource) throws IllegalAccessException {
    FieldUtils.writeField(resource, "httpRequest", mock(HttpServletRequest.class), true);
    return resource;
  }

  private static Method setPolicyMethod() throws NoSuchMethodException {
    return PolicyOperations.class.getMethod(
        "setPolicy", String.class, String.class, PolicySetRequest.class);
  }

  private static Response setEnabled(
      PolicyOperations operations, String metalake, String policy, boolean enable)
      throws Throwable {
    Method method = setPolicyMethod();
    PolicySetRequest request = new PolicySetRequest(enable);
    return intercept(
        method,
        invocation(
            method,
            new Object[] {metalake, policy, request},
            ignored -> operations.setPolicy(metalake, policy, request)));
  }

  private static Method alterPolicyMethod() throws NoSuchMethodException {
    return PolicyOperations.class.getMethod(
        "alterPolicy", String.class, String.class, PolicyUpdatesRequest.class);
  }

  private static Method associateTagsMethod() throws NoSuchMethodException {
    return MetadataObjectTagOperations.class.getMethod(
        "associateTagsForObject",
        String.class,
        String.class,
        String.class,
        TagsAssociateRequest.class);
  }

  private static Method addPolicyForTagMethod() throws NoSuchMethodException {
    return TagOperations.class.getMethod(
        "addPolicyForTag", String.class, String.class, String.class, PolicyTagAddRequest.class);
  }

  private static Method removePolicyFromTagMethod() throws NoSuchMethodException {
    return TagOperations.class.getMethod(
        "removePolicyFromTag", String.class, String.class, String.class);
  }

  private static MethodInvocation invocation(Method method, Object[] arguments) {
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(arguments);
    return invocation;
  }

  private static MethodInvocation invocation(Method method, Object[] arguments, Answer<?> proceed)
      throws Throwable {
    MethodInvocation invocation = invocation(method, arguments);
    when(invocation.proceed()).thenAnswer(proceed);
    return invocation;
  }

  private static Response intercept(Method method, MethodInvocation invocation) throws Throwable {
    return (Response)
        new GravitinoInterceptionService().getMethodInterceptors(method).get(0).invoke(invocation);
  }

  private static Response associateCatalogTags(
      MetadataObjectTagOperations operations, String metalake, TagsAssociateRequest request)
      throws Throwable {
    Method method = associateTagsMethod();
    return intercept(
        method,
        invocation(
            method,
            new Object[] {metalake, "catalog", "catalog", request},
            ignored -> operations.associateTagsForObject(metalake, "catalog", "catalog", request)));
  }

  private static void runAsSecondWriter(UserPrincipal principal, ThrowingRunnable action)
      throws Exception {
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      writer
          .submit(
              () -> {
                try (MockedStatic<PrincipalUtils> writerPrincipal =
                    mockStatic(PrincipalUtils.class)) {
                  writerPrincipal.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
                  writerPrincipal
                      .when(PrincipalUtils::getCurrentUserName)
                      .thenReturn(principal.getName());
                  action.run();
                }
                return null;
              })
          .get(10, TimeUnit.SECONDS);
    } finally {
      writer.shutdownNow();
    }
  }

  private Set<String> bindingNames(PolicyManager manager, String metalake, String policy) {
    return Arrays.stream(manager.listTagAssociationsForPolicy(metalake, policy))
        .map(relation -> relation.targetEntity().name())
        .collect(Collectors.toSet());
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Overrides {@link GravitinoEnv} fields and restores their original values on close. */
  private static final class EnvOverride implements AutoCloseable {
    private final GravitinoEnv env = GravitinoEnv.getInstance();
    private final Map<String, Object> originals = new LinkedHashMap<>();

    private EnvOverride(String... preservedFields) throws IllegalAccessException {
      for (String field : preservedFields) {
        preserve(field);
      }
    }

    private EnvOverride set(String field, Object value) throws IllegalAccessException {
      preserve(field);
      FieldUtils.writeField(env, field, value, true);
      return this;
    }

    private EnvOverride setPolicyAndTagDispatchers(PolicyManager policies, TagManager tags)
        throws IllegalAccessException {
      return set("policyDispatcher", policies)
          .set("tagDispatcher", tags)
          .set("internalTagDispatcher", tags);
    }

    @Override
    public void close() throws IllegalAccessException {
      for (Map.Entry<String, Object> original : originals.entrySet()) {
        FieldUtils.writeField(env, original.getKey(), original.getValue(), true);
      }
    }

    private void preserve(String field) throws IllegalAccessException {
      if (!originals.containsKey(field)) {
        originals.put(field, FieldUtils.readField(env, field, true));
      }
    }
  }

  /**
   * Statically mocks the current principal, the authorizer provider, and the current-user check.
   */
  private static final class MockedSecurity implements AutoCloseable {
    private final MockedStatic<PrincipalUtils> principals = mockStatic(PrincipalUtils.class);
    private final MockedStatic<GravitinoAuthorizerProvider> providers =
        mockStatic(GravitinoAuthorizerProvider.class);
    private final MockedStatic<AuthorizationUtils> auth = mockStatic(AuthorizationUtils.class);

    private MockedSecurity(UserPrincipal principal) {
      principals.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      principals.when(PrincipalUtils::getCurrentUserName).thenReturn(principal.getName());
      principals.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
    }

    private GravitinoAuthorizer mockAuthorizer(MetadataObject.Type... ownedTypes) {
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      when(authorizer.isOwner(any(), any(), any(), any())).thenAnswer(ownerOf(ownedTypes));
      auth.when(() -> AuthorizationUtils.checkCurrentUser(any(), any(), any()))
          .thenAnswer(invocation -> null);
      return authorizer;
    }

    @Override
    public void close() {
      auth.close();
      providers.close();
      principals.close();
    }
  }
}
