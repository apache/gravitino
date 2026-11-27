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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.authorization.Privilege.Name.MODIFY_SEMANTIC_MODEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.CoreBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Authorizes a Semantic Model that has only drafts through the real H2 store and JCasbin.
 *
 * <p>Grants are written against the model identity, so enforcement must resolve the same identity
 * rather than the active revision, which a never-published model does not have.
 */
@CoreBackend.H2
public class TestSemanticModelDraftAuthorizationH2 {
  private static final String METALAKE = "semantic_draft_metalake";
  private static final String CATALOG = "semantic_draft_catalog";
  private static final String SCHEMA = "semantic_draft_schema";
  private static final String MODEL = "sales";
  private static final String EDITOR = "semantic_draft_editor";
  private static final String DENIED = "semantic_draft_denied";
  private static final MetadataObject MODEL_OBJECT =
      MetadataObjects.of(List.of(CATALOG, SCHEMA, MODEL), MetadataObject.Type.SEMANTIC_MODEL);
  private static final MetadataObject SCHEMA_OBJECT =
      MetadataObjects.of(List.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA);
  private static final AuditInfo AUDIT =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build();

  @TempDir Path directory;

  private Object previousConfig;
  private Object previousStore;
  private Object previousIdGenerator;
  private Object previousMetrics;
  private Object previousCatalogManager;
  private EntityIdResolver previousIdResolver;
  private RelationalEntityStore store;
  private JcasbinAuthorizer authorizer;

  @BeforeEach
  void setUp() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    previousConfig = FieldUtils.readField(env, "config", true);
    previousStore = FieldUtils.readField(env, "entityStore", true);
    previousIdGenerator = FieldUtils.readField(env, "idGenerator", true);
    previousMetrics = FieldUtils.readField(env, "metricsSystem", true);
    previousCatalogManager = FieldUtils.readField(env, "catalogManager", true);
    previousIdResolver =
        (EntityIdResolver)
            FieldUtils.readStaticField(EntityIdService.class, "entityIdResolver", true);
    assertNull(
        FieldUtils.readStaticField(SqlSessionFactoryHelper.class, "sqlSessionFactory", true),
        "The isolated H2 fixture must not reuse another test's MyBatis factory");

    Config config = new Config(false) {};
    String dbPath = directory.resolve("semantic").toAbsolutePath().toString();
    config.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH, dbPath);
    config.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        "jdbc:h2:file:" + dbPath + ";DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=FALSE");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "test");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "test");
    config.set(Configs.CACHE_ENABLED, false);
    config.set(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS, 3600L);
    config.set(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS, 3600L);
    FieldUtils.writeField(env, "config", config, true);
    FieldUtils.writeField(env, "idGenerator", RandomIdGenerator.INSTANCE, true);
    FieldUtils.writeField(env, "metricsSystem", null, true);
    FieldUtils.writeField(env, "catalogManager", caseSensitiveCatalogs(), true);

    store = new RelationalEntityStore();
    store.initialize(config);
    FieldUtils.writeField(env, "entityStore", store, true);
    seedEntities();
    authorizer = new JcasbinAuthorizer();
    authorizer.initialize();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (authorizer != null) {
        authorizer.close();
      }
    } finally {
      try {
        if (store != null) {
          store.close();
        }
      } finally {
        GravitinoEnv env = GravitinoEnv.getInstance();
        FieldUtils.writeField(env, "config", previousConfig, true);
        FieldUtils.writeField(env, "entityStore", previousStore, true);
        FieldUtils.writeField(env, "idGenerator", previousIdGenerator, true);
        FieldUtils.writeField(env, "metricsSystem", previousMetrics, true);
        FieldUtils.writeField(env, "catalogManager", previousCatalogManager, true);
        EntityIdService.initialize(previousIdResolver);
      }
    }
  }

  @Test
  void testModelPrivilegesApplyBeforeFirstPublication() {
    assertModelHasOnlyDrafts();
    assertTrue(
        authorizer.authorize(
            principal(EDITOR),
            METALAKE,
            MODEL_OBJECT,
            MODIFY_SEMANTIC_MODEL,
            new AuthorizationRequestContext()),
        "a model-level grant must let its editor read and edit the first draft");
  }

  @Test
  void testModelDenyOverridesSchemaAllowBeforeFirstPublication() {
    assertModelHasOnlyDrafts();
    assertTrue(
        authorizer.authorize(
            principal(DENIED),
            METALAKE,
            SCHEMA_OBJECT,
            MODIFY_SEMANTIC_MODEL,
            new AuthorizationRequestContext()));
    assertTrue(
        authorizer.deny(
            principal(DENIED),
            METALAKE,
            MODEL_OBJECT,
            MODIFY_SEMANTIC_MODEL,
            new AuthorizationRequestContext()),
        "a model-level deny must still override the schema-level allow");
  }

  private void assertModelHasOnlyDrafts() {
    ManagedSemanticModelOperations operations = deferredOperations();
    NameIdentifier model = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, MODEL);
    assertTrue(operations.semanticModelExists(model));
    // Consumers do not see the model until it is published.
    assertThrows(NoSuchSemanticModelException.class, () -> operations.loadSemanticModel(model));
  }

  private void seedEntities() throws IOException {
    store.put(
        BaseMetalake.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(METALAKE)
            .withComment("")
            .withProperties(null)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(SCHEMA)
            .withNamespace(Namespace.of(METALAKE, CATALOG))
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    TableEntity orders =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("orders")
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withAuditInfo(AUDIT)
            .build();
    store.put(orders, false);
    SemanticModel draft =
        deferredOperations()
            .createSemanticModel(
                NameIdentifier.of(METALAKE, CATALOG, SCHEMA, MODEL),
                null,
                SemanticModelDefinition.builder()
                    .withDatasets(
                        new Dataset[] {
                          Dataset.builder()
                              .withName("orders")
                              .withSource(NameIdentifier.of(CATALOG, SCHEMA, "orders"))
                              .build()
                        })
                    .build(),
                Map.of(),
                SemanticModelSourceUpdate.replace(
                    List.of(new SemanticModelSource("orders", orders.type(), orders.id()))));
    assertEquals(SemanticModel.Status.DRAFT, draft.status());

    SecurableObject catalog = SecurableObjects.ofCatalog(CATALOG, List.of());
    SecurableObject schema = SecurableObjects.ofSchema(catalog, SCHEMA, List.of());
    addUser(
        EDITOR,
        List.of(
            SecurableObjects.ofSemanticModel(
                schema, MODEL, List.of(Privileges.ModifySemanticModel.allow()))));
    addUser(
        DENIED,
        List.of(
            SecurableObjects.ofSchema(
                catalog, SCHEMA, List.of(Privileges.ModifySemanticModel.allow())),
            SecurableObjects.ofSemanticModel(
                schema, MODEL, List.of(Privileges.ModifySemanticModel.deny()))));
  }

  private void addUser(String user, List<SecurableObject> objects) throws IOException {
    long roleId = RandomIdGenerator.INSTANCE.nextId();
    String role = user + "_role";
    store.put(
        RoleEntity.builder()
            .withId(roleId)
            .withName(role)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .withSecurableObjects(objects)
            .build(),
        false);
    store.put(
        UserEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(user)
            .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
            .withRoleIds(List.of(roleId))
            .withRoleNames(List.of(role))
            .withAuditInfo(AUDIT)
            .build(),
        false);
  }

  private ManagedSemanticModelOperations deferredOperations() {
    return new ManagedSemanticModelOperations(
        store, RandomIdGenerator.INSTANCE, (ident, definition) -> {}, ident -> false);
  }

  private static Principal principal(String user) {
    return () -> user;
  }

  private static CatalogManager caseSensitiveCatalogs() throws Exception {
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(new Capability() {});
    CatalogManager catalogs = mock(CatalogManager.class);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    return catalogs;
  }
}
