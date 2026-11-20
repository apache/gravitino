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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserGroup;
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
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.dto.requests.TagValuesAssociateRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.HiddenPolicyTag;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.server.web.rest.TagOperations;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogListener;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.tag.TagAssignmentAuthorization;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

/** Verifies tag assignment authorization against persisted H2 metadata. */
@Tag("server-tag-assignment-h2-test")
public class TestTagAssignmentAuthorizationH2 extends TestJDBCBackend {
  @TestTemplate
  void testCreatedAccessTagInvalidatesMissingTagAuthorization() throws Throwable {
    String metalake = "missing_tag_auth_h2";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(store, metalake, "policy", Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("internalCatalogDispatcher", existingCatalogs());
      security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.CATALOG);
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"created"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Response response =
          invokeRealTagRoute(
              operations,
              "associateTagsForObject",
              metalake,
              "catalog",
              "catalog",
              request,
              () ->
                  runAsSecondWriter(
                      principal,
                      () -> {
                        createAndInsertTagEntity("created", "", metalake);
                        tags.addPolicyForTag(
                            metalake, "created", "policy", AllValuesSelector.get());
                      }));
      assertEquals(409, response.getStatus());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "created").length);
    }
  }

  @TestTemplate
  void testPaddedTagNameKeepsMixedRequestBehavior() throws Throwable {
    Config config = GravitinoEnv.getInstance().config();
    try (Connection connection =
            DriverManager.getConnection(
                config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL),
                config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER),
                config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD));
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER TABLE tag_meta ALTER COLUMN tag_name CHAR(5) NOT NULL");
      try {
        String metalake = "pad_test";
        createAndInsertMakeLake(metalake);
        createAndInsertCatalog(metalake, "catalog");
        createAndInsertTagEntity("label", "", metalake);
        createAndInsertTagEntity("plain", "", metalake);
        RelationalEntityStore store = newStore();
        PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
        TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
        UserPrincipal principal = new UserPrincipal("tester");
        try (EnvOverride overrides = new EnvOverride();
            MockedSecurity security = new MockedSecurity(principal)) {
          overrides
              .setPolicyAndTagDispatchers(policies, tags)
              .set("internalCatalogDispatcher", existingCatalogs());
          security.mockAuthorizer(MetadataObject.Type.METALAKE);
          TagsAssociateRequest request =
              new TagsAssociateRequest(new String[] {"plain"}, new String[] {"label "});
          MetadataObjectTagOperations operations =
              withHttpRequest(new MetadataObjectTagOperations(tags));
          assertEquals(
              200,
              invokeRealTagRoute(operations, "associateTagsForObject", metalake, request)
                  .getStatus());
          assertEquals(1, tags.listMetadataObjectsForTag(metalake, "plain").length);
          assertEquals(
              200,
              invokeRealTagRoute(
                      operations,
                      "associateTagsForObject",
                      metalake,
                      new TagsAssociateRequest(new String[0], new String[] {"plain"}))
                  .getStatus());
          Response replaced =
              invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "catalog",
                  "catalog",
                  request,
                  () ->
                      runAsSecondWriter(
                          principal,
                          () -> {
                            assertTrue(tags.deleteTag(metalake, "label"));
                            createAndInsertTagEntity("label", "", metalake);
                          }));
          assertEquals(409, replaced.getStatus());
          assertEquals(0, tags.listMetadataObjectsForTag(metalake, "plain").length);
        }
      } finally {
        statement.execute("ALTER TABLE tag_meta ALTER COLUMN tag_name VARCHAR(128) NOT NULL");
      }
    }
  }

  @TestTemplate
  void testConcurrentBindingInvalidatesDescriptiveAssignment() throws Throwable {
    String metalake = "tag_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("internalCatalogDispatcher", existingCatalogs());
      security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.CATALOG);
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "catalog", "catalog", request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () ->
                        tags.addPolicyForTag(
                            metalake, "visible", policyName, AllValuesSelector.get()));
                return operations.associateTagsForObject(metalake, "catalog", "catalog", request);
              });
      Response response = intercept(method, invocation);
      assertEquals(409, response.getStatus());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertTrue(hasVisibleBinding(policies, metalake, policyName));
    }
  }

  @TestTemplate
  void testRecreatedAccessTagCannotReusePriorGrant() throws Throwable {
    String metalake = "tag_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    AtomicBoolean oldTagGranted = new AtomicBoolean(true);
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags);
      tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      overrides.set("internalCatalogDispatcher", existingCatalogs());
      GravitinoAuthorizer authorizer =
          security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.CATALOG);
      when(authorizer.authorize(any(), any(), any(), any(), any()))
          .thenAnswer(
              inv ->
                  oldTagGranted.get()
                      && ((MetadataObject) inv.getArgument(2)).type() == MetadataObject.Type.TAG
                      && inv.getArgument(3) == Privilege.Name.APPLY_TAG);
      when(authorizer.hasMetadataPrivilegePermission(any(), any(), any(), any())).thenReturn(true);
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "catalog", "catalog", request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () -> {
                      assertTrue(tags.deleteTag(metalake, "visible"));
                      tags.createTag(metalake, "visible", "replacement", Map.of());
                      tags.addPolicyForTag(
                          metalake, "visible", policyName, AllValuesSelector.get());
                      oldTagGranted.set(false);
                    });
                return operations.associateTagsForObject(metalake, "catalog", "catalog", request);
              });
      Response response = intercept(method, invocation);
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertFalse(oldTagGranted.get());
      assertEquals(409, response.getStatus(), "replacement must not reuse the old tag grant");
      assertTrue(hasVisibleBinding(policies, metalake, policyName));
    }
  }

  @TestTemplate
  void testDeletedAuthorizedTagDoesNotPartiallyAssignBatch() throws Throwable {
    String metalake = "tag_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);
    createAndInsertTagEntity("other", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("internalCatalogDispatcher", existingCatalogs());
      security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.CATALOG);
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"visible", "other"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "catalog", "catalog", request},
              ignored -> {
                runAsSecondWriter(principal, () -> assertTrue(tags.deleteTag(metalake, "visible")));
                return operations.associateTagsForObject(metalake, "catalog", "catalog", request);
              });
      Response response = intercept(method, invocation);
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "other").length);
      assertEquals(
          409,
          response.getStatus(),
          "a disappeared authorized tag must not partially mutate the batch");
    }
  }

  @TestTemplate
  void testRecreatedCatalogCannotReusePriorGrant() throws Throwable {
    String metalake = "tag_auth_h2";
    String policyName = "policy";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    putPolicy(store, metalake, policyName, Policy.BuiltInType.ACCESS_CONTROL, selectContent());
    UserPrincipal principal = new UserPrincipal("tester");
    AtomicBoolean oldObjectGranted = new AtomicBoolean(true);
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides.setPolicyAndTagDispatchers(policies, tags);
      tags.addPolicyForTag(metalake, "visible", policyName, AllValuesSelector.get());
      overrides.set("internalCatalogDispatcher", existingCatalogs());
      GravitinoAuthorizer authorizer =
          security.mockAuthorizer(MetadataObject.Type.TAG, MetadataObject.Type.CATALOG);
      when(authorizer.authorize(any(), any(), any(), any(), any()))
          .thenAnswer(
              inv ->
                  ((MetadataObject) inv.getArgument(2)).type() == MetadataObject.Type.TAG
                      && inv.getArgument(3) == Privilege.Name.APPLY_TAG);
      when(authorizer.hasMetadataPrivilegePermission(any(), any(), any(), any()))
          .thenAnswer(inv -> oldObjectGranted.get());
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      MethodInvocation invocation =
          invocation(
              method,
              new Object[] {metalake, "catalog", "catalog", request},
              ignored -> {
                runAsSecondWriter(
                    principal,
                    () -> {
                      assertTrue(
                          store.delete(
                              NameIdentifier.of(metalake, "catalog"), Entity.EntityType.CATALOG));
                      createAndInsertCatalog(metalake, "catalog");
                      oldObjectGranted.set(false);
                    });
                return operations.associateTagsForObject(metalake, "catalog", "catalog", request);
              });
      Response response = intercept(method, invocation);
      assertFalse(oldObjectGranted.get());
      assertEquals(409, response.getStatus(), "replacement must not reuse the old object grant");
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertTrue(hasVisibleBinding(policies, metalake, policyName));
    }
  }

  @TestTemplate
  void testActualJcasbinTagAssignment() throws Throwable {
    String metalake = "real_auth_h2";
    String foreignMetalake = "foreign_auth_h2";
    createAndInsertMakeLake(metalake);
    createAndInsertMakeLake(foreignMetalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertSchema(metalake, "catalog", "schema");
    createAndInsertTableEntity(Namespace.of(metalake, "catalog", "schema"), "table");
    backend.insert(
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("coltable")
            .withNamespace(Namespace.of(metalake, "catalog", "schema"))
            .withColumns(
                List.of(
                    ColumnEntity.builder()
                        .withId(RandomIdGenerator.INSTANCE.nextId())
                        .withName("c1")
                        .withPosition(0)
                        .withComment("c")
                        .withDataType(Types.IntegerType.get())
                        .withNullable(true)
                        .withAutoIncrement(false)
                        .withAuditInfo(AUDIT_INFO)
                        .build()))
            .withAuditInfo(AUDIT_INFO)
            .build(),
        false);
    createAndInsertSchema(metalake, "catalog", "outer:inner");
    createAndInsertTableEntity(Namespace.of(metalake, "catalog", "outer:inner"), "nested_table");
    createAndInsertSchema(metalake, "catalog", "MiXeD");
    createAndInsertTableEntity(Namespace.of(metalake, "catalog", "MiXeD"), "MiXeDTbl");
    createAndInsertTagEntity("visible", "", metalake);
    createAndInsertTagEntity("label", "", metalake);
    createAndInsertTagEntity("foreign_tag", "", foreignMetalake);
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
    long roleId =
        putRole(
            store,
            metalake,
            "role",
            List.of(
                SecurableObjects.ofTag("visible", List.of(Privileges.ApplyTag.allow())),
                SecurableObjects.ofCatalog(
                    "catalog",
                    List.of(Privileges.ManageGrants.allow(), Privileges.UseCatalog.allow())),
                SecurableObjects.ofSchema(
                    SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow())),
                    "schema",
                    List.of(Privileges.UseSchema.allow())),
                SecurableObjects.ofSchema(
                    SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow())),
                    "outer",
                    List.of(Privileges.UseSchema.allow())),
                SecurableObjects.ofSchema(
                    SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow())),
                    "MiXeD",
                    List.of(Privileges.UseSchema.allow()))));
    long denyRoleId =
        putRole(
            store,
            metalake,
            "denyTag",
            List.of(SecurableObjects.ofTag("visible", List.of(Privileges.ApplyTag.deny()))));
    long tagOnlyRoleId =
        putRole(
            store,
            metalake,
            "tagOnly",
            List.of(
                SecurableObjects.ofTag("visible", List.of(Privileges.ApplyTag.allow())),
                SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow())),
                SecurableObjects.ofSchema(
                    SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow())),
                    "schema",
                    List.of(Privileges.UseSchema.allow()))));
    long governanceRoleId =
        putRole(
            store,
            metalake,
            "governanceRole",
            List.of(SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow()))));
    SecurableObject labelCatalog =
        SecurableObjects.ofCatalog("catalog", List.of(Privileges.UseCatalog.allow()));
    SecurableObject labelSchema =
        SecurableObjects.ofSchema(labelCatalog, "schema", List.of(Privileges.UseSchema.allow()));
    long labelRoleId =
        putRole(
            store,
            metalake,
            "labelColumn",
            List.of(
                labelCatalog,
                labelSchema,
                SecurableObjects.ofTable(
                    labelSchema, "coltable", List.of(Privileges.SelectTable.allow())),
                SecurableObjects.ofTag("label", List.of(Privileges.ApplyTag.allow()))));
    putUser(store, metalake, "allowed", List.of(roleId), List.of("role"));
    putUser(store, metalake, "denied", List.of(), List.of());
    putUser(store, metalake, "tagOnly", List.of(tagOnlyRoleId), List.of("tagOnly"));
    putUser(store, metalake, "metalakeOwner", List.of(), List.of());
    putUser(store, metalake, "groupUser", List.of(), List.of());
    putUser(store, metalake, "deniedTag", List.of(roleId, denyRoleId), List.of("role", "denyTag"));
    putGroup(store, metalake, "taggers", List.of(roleId), List.of("role"));
    putUser(store, metalake, "governor", List.of(governanceRoleId), List.of("governanceRole"));
    putUser(store, metalake, "labelColumn", List.of(labelRoleId), List.of("labelColumn"));
    setOwner(metalake, NameIdentifier.of(metalake), Entity.EntityType.METALAKE, "metalakeOwner");
    setOwner(
        metalake, NameIdentifierUtil.ofTag(metalake, "label"), Entity.EntityType.TAG, "governor");
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
                "internalSchemaDispatcher",
                "internalTableDispatcher",
                "catalogManager",
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
      SchemaDispatcher schemas = mock(SchemaDispatcher.class);
      when(schemas.schemaExists(any())).thenReturn(true);
      overrides.set("internalSchemaDispatcher", schemas);
      TableDispatcher tables = mock(TableDispatcher.class);
      when(tables.tableExists(any())).thenReturn(true);
      overrides.set("internalTableDispatcher", tables);
      BaseCatalog<?> catalog = mock(BaseCatalog.class);
      when(catalog.capability()).thenReturn(Capability.DEFAULT);
      overrides.set("catalogManager", catalogManagerFor(catalog));
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
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      TagsAssociateRequest add = new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      Response allow =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "catalog", "catalog", add},
                  i -> operations.associateTagsForObject(metalake, "catalog", "catalog", add)));
      assertEquals(200, allow.getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("denied"));
      assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
      TagsAssociateRequest remove =
          new TagsAssociateRequest(new String[0], new String[] {"visible"});
      Response deny =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "catalog", "catalog", remove},
                  i -> operations.associateTagsForObject(metalake, "catalog", "catalog", remove)));
      assertEquals(403, deny.getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      TagOperations legacy = withHttpRequest(new TagOperations(tags));
      Object[] resources = {operations, operations, legacy};
      String[] methods = {
        "associateTagsForObject", "associateTagValuesForObject", "associateTagsForObject"
      };
      for (int route = 0; route < resources.length; route++) {
        Object resource = resources[route];
        String name = methods[route];
        Object routeAdd = visibleTagRequest(route == 1, true);
        Object routeRemove = visibleTagRequest(route == 1, false);
        current.set(new UserPrincipal("allowed"));
        assertEquals(200, invokeRealTagRoute(resource, name, metalake, routeRemove).getStatus());
        assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
        assertEquals(200, invokeRealTagRoute(resource, name, metalake, routeAdd).getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(
            new UserPrincipal("groupUser", List.of(new UserGroup(Optional.empty(), "taggers"))));
        assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
        assertEquals(200, invokeRealTagRoute(resource, name, metalake, routeRemove).getStatus());
        assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
        assertEquals(200, invokeRealTagRoute(resource, name, metalake, routeAdd).getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(new UserPrincipal("deniedTag"));
        assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
        assertEquals(403, invokeRealTagRoute(resource, name, metalake, routeRemove).getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(new UserPrincipal("denied"));
        assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
        assertEquals(403, invokeRealTagRoute(resource, name, metalake, routeRemove).getStatus());
        Object missing =
            route == 1
                ? new TagValuesAssociateRequest(
                    new TagValue[] {TagValue.noValue("missing")}, new TagValue[0])
                : new TagsAssociateRequest(new String[] {"missing"}, new String[0]);
        assertEquals(403, invokeRealTagRoute(resource, name, metalake, missing).getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(new UserPrincipal("allowed").withActiveRoles(ActiveRoles.none()));
        assertTrue(authorizer.isMetalakeUser(metalake, new AuthorizationRequestContext()));
        assertEquals(403, invokeRealTagRoute(resource, name, metalake, routeRemove).getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      }
      current.set(new UserPrincipal("allowed"));
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  add)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("denied"));
      assertEquals(
          403,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  remove)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("tagOnly"));
      assertEquals(
          403,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  remove)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("deniedTag"));
      assertEquals(
          403,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  remove)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("allowed"));
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  remove)
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("labelColumn"));
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  new TagsAssociateRequest(new String[] {"label"}, new String[0]))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "label").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  new TagsAssociateRequest(new String[0], new String[] {"label"}))
              .getStatus());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "label").length);
      for (int route = 0; route < resources.length; route++) {
        Object resource = resources[route];
        String name = methods[route];
        Object routeAdd = visibleTagRequest(route == 1, true);
        Object routeRemove = visibleTagRequest(route == 1, false);
        current.set(new UserPrincipal("allowed"));
        assertEquals(
            200,
            invokeRealTagRoute(resource, name, metalake, "table", "catalog.schema.table", routeAdd)
                .getStatus());
        assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(new UserPrincipal("deniedTag"));
        assertEquals(
            403,
            invokeRealTagRoute(
                    resource, name, metalake, "table", "catalog.schema.table", routeRemove)
                .getStatus());
        assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
        current.set(new UserPrincipal("allowed"));
        assertEquals(
            200,
            invokeRealTagRoute(
                    resource, name, metalake, "table", "catalog.schema.table", routeRemove)
                .getStatus());
        assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
        Response inherited =
            operations.getTagForObject(metalake, "table", "catalog.schema.table", "visible");
        assertEquals(200, inherited.getStatus());
        assertTrue(((TagResponse) inherited.getEntity()).getTag().inherited().orElse(false));
      }
      current.set(new UserPrincipal("allowed"));
      TagsAssociateRequest nestedAdd =
          new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      TagsAssociateRequest nestedRemove =
          new TagsAssociateRequest(new String[0], new String[] {"visible"});
      Capability quoteAware =
          new Capability() {
            @Override
            public CapabilityResult caseSensitiveOnName(Scope scope) {
              return CapabilityResult.unsupported("normalization depends on quoting");
            }

            @Override
            public String normalizeName(Scope scope, String name) {
              if (name.startsWith("\"") && name.endsWith("\"") && name.length() >= 2) {
                return name.substring(1, name.length() - 1);
              }
              return name;
            }
          };
      when(catalog.capability()).thenReturn(quoteAware);
      ThrowingRunnable mixedCaseAncestorsChecked =
          () -> {
            Map<NameIdentifier, Optional<NamespacedEntityId>> ancestors =
                TagAssignmentAuthorization.checkedSchemaAncestors();
            assertEquals(
                List.of(NameIdentifier.of(metalake, "catalog", "MiXeD")),
                List.copyOf(ancestors.keySet()));
            assertTrue(ancestors.values().stream().allMatch(Optional::isPresent));
          };
      String mixedCaseTable = "catalog.\"MiXeD\".\"MiXeDTbl\"";
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "table",
                  mixedCaseTable,
                  nestedAdd,
                  mixedCaseAncestorsChecked)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "table",
                  mixedCaseTable,
                  nestedRemove,
                  mixedCaseAncestorsChecked)
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      when(catalog.capability()).thenReturn(Capability.DEFAULT);
      ThrowingRunnable nestedAncestorsChecked =
          () -> {
            Map<NameIdentifier, Optional<NamespacedEntityId>> ancestors =
                TagAssignmentAuthorization.checkedSchemaAncestors();
            assertEquals(
                List.of(
                    NameIdentifier.of(metalake, "catalog", "outer"),
                    NameIdentifier.of(metalake, "catalog", "outer:inner")),
                List.copyOf(ancestors.keySet()));
            assertTrue(ancestors.values().stream().allMatch(Optional::isPresent));
          };
      String nestedTable = "catalog.outer:inner.nested_table";
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "table",
                  nestedTable,
                  nestedAdd,
                  nestedAncestorsChecked)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "table",
                  nestedTable,
                  nestedRemove,
                  nestedAncestorsChecked)
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "schema",
                  "catalog.outer:inner",
                  nestedAdd)
              .getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "schema",
                  "catalog.outer:inner",
                  nestedRemove)
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("allowed"));
      assertEquals(
          400,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[] {"visible", ""}, new String[0]))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("metalakeOwner"));
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[] {"label"}, new String[] {"missing"}))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "label").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[0], new String[] {"label"}))
              .getStatus());
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  add)
              .getStatus());
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  "column",
                  "catalog.schema.coltable.c1",
                  remove)
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      current.set(new UserPrincipal("allowed"));
      assertEquals(
          404,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(
                      new String[] {HiddenPolicyTag.name(policy.id())}, new String[0]))
              .getStatus());
      assertEquals(
          403,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[] {"foreign_tag"}, new String[0]))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(0, tags.listMetadataObjectsForTag(foreignMetalake, "foreign_tag").length);
      PassThroughAuthorizer disabledAuthorizer = new PassThroughAuthorizer();
      config.set(Configs.ENABLE_AUTHORIZATION, false);
      env.setGravitinoAuthorizer(disabledAuthorizer);
      when(provider.getGravitinoAuthorizer()).thenReturn(disabledAuthorizer);
      current.set(new UserPrincipal("denied"));
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[0], new String[] {"visible"}))
              .getStatus());
      assertEquals(0, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[] {"visible"}, new String[0]))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[0], new String[] {"label"}))
              .getStatus());
      assertEquals(
          200,
          invokeRealTagRoute(
                  operations,
                  "associateTagsForObject",
                  metalake,
                  new TagsAssociateRequest(new String[] {"label"}, new String[] {"missing"}))
              .getStatus());
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "label").length);
    }
  }

  @TestTemplate
  void testColdExternalTableCanBeTaggedByMetalakeOwner() throws Throwable {
    String metalake = "cold_table_auth_h2";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "catalog");
    createAndInsertSchema(metalake, "catalog", "schema");
    createAndInsertTagEntity("visible", "", metalake);
    RelationalEntityStore store = newStore();
    PolicyManager policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    UserPrincipal principal = new UserPrincipal("tester");
    try (EnvOverride overrides = new EnvOverride();
        MockedSecurity security = new MockedSecurity(principal)) {
      overrides
          .setPolicyAndTagDispatchers(policies, tags)
          .set("entityStore", store)
          .set("eventBus", mock(EventBus.class));
      BaseCatalog<?> catalog = mock(BaseCatalog.class);
      when(catalog.capability()).thenReturn(Capability.DEFAULT);
      overrides.set("catalogManager", catalogManagerFor(catalog));
      TableDispatcher tables = mock(TableDispatcher.class);
      when(tables.tableExists(any()))
          .thenAnswer(
              invocation -> {
                NameIdentifier name = invocation.getArgument(0);
                String schema = name.namespace().level(2);
                if ("new_schema".equals(schema)) {
                  createAndInsertSchema(metalake, "catalog", schema);
                }
                createAndInsertTableEntity(Namespace.of(metalake, "catalog", schema), name.name());
                return true;
              });
      overrides.set("internalTableDispatcher", tables);
      GravitinoAuthorizer authorizer = security.mockAuthorizer(MetadataObject.Type.METALAKE);
      TagsAssociateRequest request =
          new TagsAssociateRequest(new String[] {"visible"}, new String[0]);
      MetadataObjectTagOperations operations =
          withHttpRequest(new MetadataObjectTagOperations(tags));
      Method method = associateTagsMethod();
      Response response =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "table", "catalog.schema.table", request},
                  ignored ->
                      operations.associateTagsForObject(
                          metalake, "table", "catalog.schema.table", request)));
      assertTrue(
          store.exists(
              NameIdentifier.of(metalake, "catalog", "schema", "table"), Entity.EntityType.TABLE));
      assertEquals(
          200,
          response.getStatus(),
          "a cold external table should be taggable by the metalake owner");
      assertEquals(1, tags.listMetadataObjectsForTag(metalake, "visible").length);
      doAnswer(ownerOf(MetadataObject.Type.CATALOG, MetadataObject.Type.TAG))
          .when(authorizer)
          .isOwner(any(), any(), any(), any());
      Response catalogOwnerResponse =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "table", "catalog.schema.table2", request},
                  ignored ->
                      operations.associateTagsForObject(
                          metalake, "table", "catalog.schema.table2", request)));
      assertEquals(200, catalogOwnerResponse.getStatus());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      putPolicy(store, metalake, "access", Policy.BuiltInType.ACCESS_CONTROL, selectContent());
      tags.addPolicyForTag(metalake, "visible", "access", AllValuesSelector.get());
      assertEquals(2, tags.listMetadataObjectsForTag(metalake, "visible").length);
      when(authorizer.authorize(any(), any(), any(), eq(Privilege.Name.APPLY_TAG), any()))
          .thenReturn(true);
      when(authorizer.hasMetadataPrivilegePermission(any(), any(), any(), any())).thenReturn(true);
      Response accessTagResponse =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "table", "catalog.schema.table3", request},
                  ignored ->
                      operations.associateTagsForObject(
                          metalake, "table", "catalog.schema.table3", request)));
      assertEquals(200, accessTagResponse.getStatus());
      assertEquals(3, tags.listMetadataObjectsForTag(metalake, "visible").length);
      Response coldSchemaResponse =
          intercept(
              method,
              invocation(
                  method,
                  new Object[] {metalake, "table", "catalog.new_schema.table4", request},
                  ignored ->
                      operations.associateTagsForObject(
                          metalake, "table", "catalog.new_schema.table4", request)));
      assertEquals(200, coldSchemaResponse.getStatus());
      assertEquals(4, tags.listMetadataObjectsForTag(metalake, "visible").length);
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

  private static void putGroup(
      RelationalEntityStore store,
      String metalake,
      String name,
      List<Long> roleIds,
      List<String> roleNames)
      throws IOException {
    store.put(
        GroupEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(AuthorizationUtils.ofGroupNamespace(metalake))
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

  private static CatalogManager catalogManagerFor(BaseCatalog<?> catalog) throws Exception {
    CatalogManager catalogManager = mock(CatalogManager.class);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogManager)
        .doWithCatalog(any(), any());
    return catalogManager;
  }

  private static Answer<Boolean> ownerOf(MetadataObject.Type... ownedTypes) {
    List<MetadataObject.Type> owned = List.of(ownedTypes);
    return invocation -> owned.contains(((MetadataObject) invocation.getArgument(2)).type());
  }

  private static <T> T withHttpRequest(T resource) throws IllegalAccessException {
    FieldUtils.writeField(resource, "httpRequest", mock(HttpServletRequest.class), true);
    return resource;
  }

  private static Method associateTagsMethod() throws NoSuchMethodException {
    return MetadataObjectTagOperations.class.getMethod(
        "associateTagsForObject",
        String.class,
        String.class,
        String.class,
        TagsAssociateRequest.class);
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

  private static Object visibleTagRequest(boolean tagValues, boolean add) {
    if (tagValues) {
      TagValue[] visible = {TagValue.noValue("visible")};
      return add
          ? new TagValuesAssociateRequest(visible, new TagValue[0])
          : new TagValuesAssociateRequest(new TagValue[0], visible);
    }
    String[] visible = {"visible"};
    return add
        ? new TagsAssociateRequest(visible, new String[0])
        : new TagsAssociateRequest(new String[0], visible);
  }

  private static Response invokeRealTagRoute(
      Object resource, String name, String metalake, Object request) throws Throwable {
    return invokeRealTagRoute(resource, name, metalake, "catalog", "catalog", request);
  }

  private static Response invokeRealTagRoute(
      Object resource, String name, String metalake, String type, String fullName, Object request)
      throws Throwable {
    return invokeRealTagRoute(resource, name, metalake, type, fullName, request, () -> {});
  }

  private static Response invokeRealTagRoute(
      Object resource,
      String name,
      String metalake,
      String type,
      String fullName,
      Object request,
      ThrowingRunnable beforeProceed)
      throws Throwable {
    Method method =
        resource
            .getClass()
            .getMethod(name, String.class, String.class, String.class, request.getClass());
    MethodInvocation invocation =
        invocation(
            method,
            new Object[] {metalake, type, fullName, request},
            i -> {
              beforeProceed.run();
              return method.invoke(resource, metalake, type, fullName, request);
            });
    return intercept(method, invocation);
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

  private boolean hasVisibleBinding(PolicyManager manager, String metalake, String policy) {
    return Arrays.stream(manager.listTagAssociationsForPolicy(metalake, policy))
        .anyMatch(relation -> relation.targetEntity().name().equals("visible"));
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
