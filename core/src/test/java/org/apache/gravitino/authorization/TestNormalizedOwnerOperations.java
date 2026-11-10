/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestNormalizedOwnerOperations extends TestJDBCBackend {
  private static final String METALAKE = "normalized_owner";
  private CatalogManager catalogs;
  private BaseCatalog catalog;
  private AuthorizationPlugin plugin;
  private GravitinoAuthorizer authorizer;
  private OwnerManager manager;
  private OwnerDispatcher dispatcher;
  private RelationalEntityStore store;
  private Object originalCatalogs;
  private Object originalAuthorizer;
  private Object originalLockManager;

  @BeforeEach
  void setUp() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    originalCatalogs = FieldUtils.readField(env, "catalogManager", true);
    originalAuthorizer = FieldUtils.readField(env, "gravitinoAuthorizer", true);
    originalLockManager = FieldUtils.readField(env, "lockManager", true);
    FieldUtils.writeField(env, "lockManager", new LockManager(new Config(false) {}), true);
    store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(new Config(false) {}), true);
    catalogs = mock(CatalogManager.class);
    catalog = mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogs, catalog);
    when(catalog.capability()).thenReturn(foldingCapability());
    plugin = mock(AuthorizationPlugin.class);
    when(catalog.getAuthorizationPlugin()).thenReturn(plugin);
    authorizer = mock(GravitinoAuthorizer.class);
    FieldUtils.writeField(env, "catalogManager", catalogs, true);
    FieldUtils.writeField(env, "gravitinoAuthorizer", authorizer, true);
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "analytics");
    createAndInsertTableEntity(Namespace.of(METALAKE, "cat", "analytics"), "orders");
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE),
            PrincipalUtils.getCurrentUserName(),
            AUDIT_INFO);
    backend.insert(user, false);
    manager = new OwnerManager(store);
    dispatcher = new OwnerNormalizeDispatcher(manager, catalogs, store);
  }

  @AfterEach
  void tearDown() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    FieldUtils.writeField(env, "catalogManager", originalCatalogs, true);
    FieldUtils.writeField(env, "gravitinoAuthorizer", originalAuthorizer, true);
    FieldUtils.writeField(env, "lockManager", originalLockManager, true);
  }

  @TestTemplate
  void testEquivalentSchemaAndTableNamesShareOwnerAndNotifications() {
    for (MetadataObject.Type type :
        List.of(MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE)) {
      String suffix = type == MetadataObject.Type.TABLE ? ".orders" : "";
      MetadataObject stored = MetadataObjects.parse("cat.analytics" + suffix, type);
      MetadataObject requested =
          MetadataObjects.parse("cat.ANALYTICS" + suffix.toUpperCase(Locale.ROOT), type);
      dispatcher.setOwner(
          METALAKE, requested, PrincipalUtils.getCurrentUserName(), Owner.Type.USER);
      assertEquals(
          PrincipalUtils.getCurrentUserName(),
          dispatcher.getOwner(METALAKE, requested).orElseThrow().name());
      assertEquals(
          PrincipalUtils.getCurrentUserName(),
          manager.getOwner(METALAKE, stored).orElseThrow().name());
      verify(plugin).onOwnerSet(eq(stored), isNull(), any(Owner.class));
      verify(authorizer)
          .handleMetadataOwnerChange(
              eq(METALAKE),
              isNull(),
              eq(MetadataObjectUtil.toEntityIdent(METALAKE, stored)),
              eq(Entity.EntityType.valueOf(type.name())));
    }
  }

  @TestTemplate
  void testEquivalentBatchNamesDoNotCreateDuplicateRelations() {
    MetadataObject lower = MetadataObjects.parse("cat.analytics.orders", MetadataObject.Type.TABLE);
    MetadataObject upper = MetadataObjects.parse("cat.ANALYTICS.ORDERS", MetadataObject.Type.TABLE);
    dispatcher.setOwners(
        METALAKE, List.of(lower, upper), PrincipalUtils.getCurrentUserName(), Owner.Type.USER);
    assertEquals(
        PrincipalUtils.getCurrentUserName(),
        manager.getOwner(METALAKE, lower).orElseThrow().name());
    verify(plugin).onOwnerSet(eq(lower), isNull(), any(Owner.class));
  }

  @TestTemplate
  void testCaseSensitiveCatalogStillRejectsDifferentNames() {
    when(catalog.capability()).thenReturn(new Capability() {});
    MetadataObject upper = MetadataObjects.parse("cat.ANALYTICS.ORDERS", MetadataObject.Type.TABLE);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            dispatcher.setOwner(
                METALAKE, upper, PrincipalUtils.getCurrentUserName(), Owner.Type.USER));
    assertFalse(
        manager
            .getOwner(
                METALAKE, MetadataObjects.parse("cat.analytics.orders", MetadataObject.Type.TABLE))
            .isPresent());
  }

  private static Capability foldingCapability() {
    return new Capability() {
      @Override
      public CapabilityResult caseSensitiveOnName(Scope scope) {
        return scope == Scope.SCHEMA || scope == Scope.TABLE
            ? CapabilityResult.unsupported("case insensitive")
            : CapabilityResult.SUPPORTED;
      }
    };
  }
}
