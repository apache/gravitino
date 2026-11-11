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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cache.CachedEntityIdResolver;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Verifies function grant names against parent renames committed by a separate node. */
public class TestFunctionPrivilegeCoherence extends TestJDBCBackend {

  private static final String METALAKE = "function_coherence_ml";
  private static final String CATALOG = "function_catalog";
  private static final String SCHEMA = "function_schema";

  private CatalogEntity catalog;
  private SchemaEntity schema;
  private FunctionEntity function;
  private RoleEntity role;

  /** Creates a function and an ID-bound grant before the reader cache is warmed. */
  @BeforeEach
  public void prepare() throws Exception {
    createAndInsertMakeLake(METALAKE);
    catalog = createAndInsertCatalog(METALAKE, CATALOG);
    schema = createAndInsertSchema(METALAKE, CATALOG, SCHEMA);
    function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE, CATALOG, SCHEMA),
            "function",
            AUDIT_INFO);
    backend.insert(function, false);
    role =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(METALAKE),
            "function_role",
            AUDIT_INFO,
            List.of(
                SecurableObjects.parse(
                    CATALOG + "." + SCHEMA + ".function",
                    MetadataObject.Type.FUNCTION,
                    List.of(Privileges.ExecuteFunction.allow()))),
            Map.of());
    backend.insert(role, false);
  }

  /** Peer renames must be visible to repeated role reads in a READ_COMMITTED session. */
  @TestTemplate
  public void testPeerParentRenamesRefreshFunctionSecurableNames() throws Exception {
    checkPeerParentRenames(false);
  }

  /** Listing full roles also refreshes parent names without a preceding get-role call. */
  @TestTemplate
  public void testPeerParentRenamesRefreshListedFunctionSecurableNames() throws Exception {
    checkPeerParentRenames(true);
  }

  /** Queries outside full-role authorization reads still reuse the MyBatis session cache. */
  @TestTemplate
  public void testUnrelatedQueriesRetainSessionCaching() {
    try (SqlSession session =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
      assertEquals(LocalCacheScope.SESSION, session.getConfiguration().getLocalCacheScope());
      CatalogMetaMapper mapper = session.getMapper(CatalogMetaMapper.class);
      CatalogPO first = mapper.selectCatalogMetaById(catalog.id());
      assertEquals(CATALOG, first.getCatalogName());
      assertSame(first, mapper.selectCatalogMetaById(catalog.id()));
      session.clearCache();
      assertNotSame(first, mapper.selectCatalogMetaById(catalog.id()));
    }
  }

  private void checkPeerParentRenames(boolean listRoles) throws Exception {
    withReaderCache(
        () -> {
          SessionUtils.beginTransaction();
          try {
            assertFunctionGrantName(CATALOG + "." + SCHEMA + ".function", listRoles);
            onPeer(
                () ->
                    backend.update(
                        catalog.nameIdentifier(),
                        Entity.EntityType.CATALOG,
                        entity ->
                            CatalogEntity.builder()
                                .withId(catalog.id())
                                .withName("renamed_catalog")
                                .withNamespace(catalog.namespace())
                                .withType(catalog.getType())
                                .withProvider(catalog.getProvider())
                                .withComment(catalog.getComment())
                                .withProperties(catalog.getProperties())
                                .withAuditInfo(AUDIT_INFO)
                                .build()));
            assertFunctionGrantName("renamed_catalog." + SCHEMA + ".function", listRoles);
            onPeer(
                () ->
                    backend.update(
                        NameIdentifier.of(METALAKE, "renamed_catalog", SCHEMA),
                        Entity.EntityType.SCHEMA,
                        entity ->
                            SchemaEntity.builder()
                                .withId(schema.id())
                                .withName("renamed_schema")
                                .withNamespace(Namespace.of(METALAKE, "renamed_catalog"))
                                .withProperties(schema.properties())
                                .withComment(schema.comment())
                                .withAuditInfo(AUDIT_INFO)
                                .build()));
            assertFunctionGrantName("renamed_catalog.renamed_schema.function", listRoles);
            FunctionEntity loaded =
                FunctionMetaService.getInstance()
                    .getFunctionByIdentifier(
                        NameIdentifier.of(
                            METALAKE, "renamed_catalog", "renamed_schema", "function"));
            assertEquals(function.id(), loaded.id());
          } finally {
            SessionUtils.rollbackTransaction();
          }
        });
  }

  private void assertFunctionGrantName(String expected, boolean listRoles) throws Exception {
    RoleEntity loaded;
    if (listRoles) {
      List<RoleEntity> roles =
          RoleMetaService.getInstance()
              .listRolesByMetadataObject(
                  NameIdentifier.parse(METALAKE + "." + expected),
                  Entity.EntityType.FUNCTION,
                  true);
      assertEquals(1, roles.size());
      loaded = roles.get(0);
    } else {
      loaded = backend.get(role.nameIdentifier(), Entity.EntityType.ROLE);
    }
    assertEquals(1, loaded.securableObjects().size());
    assertEquals(expected, loaded.securableObjects().get(0).fullName());
    assertEquals(
        List.of(Privileges.ExecuteFunction.allow()), loaded.securableObjects().get(0).privileges());
  }

  private void withReaderCache(CheckedRunnable action) throws Exception {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    cache.put(catalog);
    cache.put(schema);
    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    Mockito.when(env.cacheEnabled()).thenReturn(true);
    try (MockedStatic<GravitinoEnv> environments = Mockito.mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      EntityIdService.initialize(
          new CachedEntityIdResolver(cache, new RelationalEntityStoreIdResolver()));
      action.run();
      assertTrue(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
      assertTrue(cache.contains(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
      assertFalse(cache.contains(role.nameIdentifier(), Entity.EntityType.ROLE));
    } finally {
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
      cache.clear();
    }
  }

  private <T> T onPeer(Callable<T> action) throws Exception {
    ExecutorService peer = Executors.newSingleThreadExecutor();
    try {
      return peer.submit(
              () -> {
                // A peer has its own resolver and SQL session, not the reader's stale cache.
                RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
                try (MockedStatic<EntityIdService> ids =
                    Mockito.mockStatic(EntityIdService.class)) {
                  ids.when(() -> EntityIdService.getEntityId(Mockito.any(), Mockito.any()))
                      .thenAnswer(
                          invocation ->
                              resolver.getEntityId(
                                  invocation.getArgument(0), invocation.getArgument(1)));
                  ids.when(() -> EntityIdService.getEntityIds(Mockito.any(), Mockito.any()))
                      .thenAnswer(
                          invocation ->
                              resolver.getEntityIds(
                                  invocation.getArgument(0), invocation.getArgument(1)));
                  return action.call();
                }
              })
          .get(20, TimeUnit.SECONDS);
    } finally {
      peer.shutdownNow();
      assertTrue(peer.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @FunctionalInterface
  private interface CheckedRunnable {
    /** Runs one scenario that may perform database operations. */
    void run() throws Exception;
  }
}
