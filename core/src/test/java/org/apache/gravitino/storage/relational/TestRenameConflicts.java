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
package org.apache.gravitino.storage.relational;

import static org.apache.gravitino.TestCatalog.PROPERTY_KEY1;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY2;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY5_PREFIX;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

/** Verifies rename conflict translation against real metadata database constraints. */
public class TestRenameConflicts extends TestJDBCBackend {
  private Config config;
  private RelationalEntityStore store;
  private Object previousConfig;
  private Object previousLockManager;
  private LockManager lockManager;
  private EntityChangeLogPoller poller;

  @BeforeEach
  void setUpManagers() throws IllegalAccessException {
    config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousLockManager = FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);
    if (lockManager == null) {
      lockManager = new LockManager(config);
    }
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", lockManager, true);
    store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    poller = new EntityChangeLogPoller(3600);
    FieldUtils.writeField(store, "entityChangeLogPoller", poller, true);
  }

  @AfterEach
  void restoreEnvironment() throws Exception {
    poller.close();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", previousLockManager, true);
  }

  @TestTemplate
  void testRenameMetalakeToExistingName() throws Exception {
    BaseMetalake source = createAndInsertMakeLake("source");
    BaseMetalake target = createAndInsertMakeLake("target");
    MetalakeManager manager = new MetalakeManager(store, RandomIdGenerator.INSTANCE);
    for (MetalakeChange[] changes :
        new MetalakeChange[][] {
          {MetalakeChange.rename("target")},
          {MetalakeChange.rename("intermediate"), MetalakeChange.rename("target")}
        }) {
      MetalakeAlreadyExistsException exception =
          Assertions.assertThrows(
              MetalakeAlreadyExistsException.class,
              () -> manager.alterMetalake(source.nameIdentifier(), changes));
      Assertions.assertEquals("Metalake target already exists", exception.getMessage());
      Assertions.assertInstanceOf(EntityAlreadyExistsException.class, exception.getCause());
      Assertions.assertEquals(source.id(), manager.loadMetalake(source.nameIdentifier()).id());
      Assertions.assertEquals(target.id(), manager.loadMetalake(target.nameIdentifier()).id());
      Assertions.assertFalse(store.exists(NameIdentifier.of("intermediate"), EntityType.METALAKE));
    }
  }

  @TestTemplate
  void testRenameCatalogToExistingName() throws Exception {
    createAndInsertMakeLake("lake");
    NameIdentifier source = NameIdentifier.of("lake", "source");
    NameIdentifier target = NameIdentifier.of("lake", "target");
    Map<String, String> properties =
        Map.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
    try (CatalogManager manager =
        new CatalogManager(config, store, RandomIdGenerator.INSTANCE, new SecretManager(config))) {
      manager.createCatalog(source, Catalog.Type.RELATIONAL, "test", "source comment", properties);
      manager.createCatalog(target, Catalog.Type.RELATIONAL, "test", "target comment", properties);
      CatalogEntity originalSource = store.get(source, EntityType.CATALOG, CatalogEntity.class);
      CatalogEntity originalTarget = store.get(target, EntityType.CATALOG, CatalogEntity.class);
      for (CatalogChange[] changes :
          new CatalogChange[][] {
            {CatalogChange.rename("target")},
            {CatalogChange.rename("intermediate"), CatalogChange.rename("target")}
          }) {
        CatalogAlreadyExistsException exception =
            Assertions.assertThrows(
                CatalogAlreadyExistsException.class, () -> manager.alterCatalog(source, changes));
        Assertions.assertEquals("Catalog " + target + " already exists", exception.getMessage());
        Assertions.assertInstanceOf(EntityAlreadyExistsException.class, exception.getCause());
        Assertions.assertEquals(
            originalSource.id(), store.get(source, EntityType.CATALOG, CatalogEntity.class).id());
        Assertions.assertEquals(
            originalTarget.id(), store.get(target, EntityType.CATALOG, CatalogEntity.class).id());
        Assertions.assertEquals("source comment", manager.loadCatalog(source).comment());
        Assertions.assertEquals("target comment", manager.loadCatalog(target).comment());
        Assertions.assertFalse(
            store.exists(NameIdentifier.of("lake", "intermediate"), EntityType.CATALOG));
      }
    }
  }

  @TestTemplate
  void testRenameManagedTableToExistingName() throws Exception {
    createParentEntities("lake", "catalog", "schema", AUDIT_INFO);
    createAndInsertSchema("lake", "catalog", "other_schema");
    TableEntity source = table(Namespace.of("lake", "catalog", "schema"), "source");
    TableEntity target = table(source.namespace(), "target");
    TableEntity crossSchemaTarget =
        table(Namespace.of("lake", "catalog", "other_schema"), "target");
    backend.insert(source, false);
    backend.insert(target, false);
    backend.insert(crossSchemaTarget, false);
    ManagedTableOperations operations = managedTableOperations();
    record RenameCase(TableEntity expectedTarget, TableChange... changes) {}
    for (RenameCase renameCase :
        new RenameCase[] {
          new RenameCase(target, TableChange.rename("target")),
          new RenameCase(target, TableChange.rename("intermediate"), TableChange.rename("target")),
          new RenameCase(crossSchemaTarget, TableChange.rename("target", "other_schema")),
          new RenameCase(
              crossSchemaTarget,
              TableChange.rename("intermediate", "other_schema"),
              TableChange.rename("target")),
          new RenameCase(
              target,
              TableChange.rename("intermediate", "other_schema"),
              TableChange.rename("intermediate", "schema"),
              TableChange.rename("target"))
        }) {
      TableAlreadyExistsException exception =
          Assertions.assertThrows(
              TableAlreadyExistsException.class,
              () -> operations.alterTable(source.nameIdentifier(), renameCase.changes()));
      Assertions.assertEquals(
          "Table " + renameCase.expectedTarget().nameIdentifier() + " already exists",
          exception.getMessage());
      Assertions.assertInstanceOf(EntityAlreadyExistsException.class, exception.getCause());
      Assertions.assertEquals(
          source.id(),
          store.get(source.nameIdentifier(), EntityType.TABLE, TableEntity.class).id());
      Assertions.assertEquals(
          target.id(),
          store.get(target.nameIdentifier(), EntityType.TABLE, TableEntity.class).id());
      Assertions.assertEquals(
          crossSchemaTarget.id(),
          store.get(crossSchemaTarget.nameIdentifier(), EntityType.TABLE, TableEntity.class).id());
      Assertions.assertFalse(
          store.exists(NameIdentifier.of(source.namespace(), "intermediate"), EntityType.TABLE));
      Assertions.assertFalse(
          store.exists(
              NameIdentifier.of(crossSchemaTarget.namespace(), "intermediate"), EntityType.TABLE));
    }
  }

  @TestTemplate
  void testRenameManagedTableKeepsEarlierSchema() throws Exception {
    createParentEntities("lake", "catalog", "schema", AUDIT_INFO);
    createAndInsertSchema("lake", "catalog", "other_schema");
    TableEntity source = table(Namespace.of("lake", "catalog", "schema"), "source");
    TableEntity originalSchemaTarget = table(source.namespace(), "target");
    backend.insert(source, false);
    backend.insert(originalSchemaTarget, false);

    managedTableOperations()
        .alterTable(
            source.nameIdentifier(),
            TableChange.rename("intermediate", "other_schema"),
            TableChange.rename("target"));

    NameIdentifier expectedTarget = NameIdentifier.of("lake", "catalog", "other_schema", "target");
    Assertions.assertEquals(
        source.id(), store.get(expectedTarget, EntityType.TABLE, TableEntity.class).id());
    Assertions.assertEquals(
        originalSchemaTarget.id(),
        store.get(originalSchemaTarget.nameIdentifier(), EntityType.TABLE, TableEntity.class).id());
    Assertions.assertFalse(store.exists(source.nameIdentifier(), EntityType.TABLE));
    Assertions.assertFalse(
        store.exists(
            NameIdentifier.of("lake", "catalog", "other_schema", "intermediate"),
            EntityType.TABLE));
  }

  @TestTemplate
  void testRenameManagedTableBackToOriginalSchema() throws Exception {
    createParentEntities("lake", "catalog", "schema", AUDIT_INFO);
    createAndInsertSchema("lake", "catalog", "other_schema");
    TableEntity source = table(Namespace.of("lake", "catalog", "schema"), "source");
    TableEntity crossSchemaTarget =
        table(Namespace.of("lake", "catalog", "other_schema"), "target");
    backend.insert(source, false);
    backend.insert(crossSchemaTarget, false);

    managedTableOperations()
        .alterTable(
            source.nameIdentifier(),
            TableChange.rename("intermediate", "other_schema"),
            TableChange.rename("intermediate", "schema"),
            TableChange.rename("target"));

    NameIdentifier expectedTarget = NameIdentifier.of(source.namespace(), "target");
    Assertions.assertEquals(
        source.id(), store.get(expectedTarget, EntityType.TABLE, TableEntity.class).id());
    Assertions.assertEquals(
        crossSchemaTarget.id(),
        store.get(crossSchemaTarget.nameIdentifier(), EntityType.TABLE, TableEntity.class).id());
    Assertions.assertFalse(store.exists(source.nameIdentifier(), EntityType.TABLE));
    Assertions.assertFalse(
        store.exists(NameIdentifier.of(source.namespace(), "intermediate"), EntityType.TABLE));
    Assertions.assertFalse(
        store.exists(
            NameIdentifier.of("lake", "catalog", "other_schema", "intermediate"),
            EntityType.TABLE));
  }

  private ManagedTableOperations managedTableOperations() {
    return new ManagedTableOperations() {
      @Override
      protected EntityStore store() {
        return store;
      }

      @Override
      protected SupportsSchemas schemas() {
        return Mockito.mock(SupportsSchemas.class);
      }

      @Override
      protected IdGenerator idGenerator() {
        return RandomIdGenerator.INSTANCE;
      }
    };
  }

  private TableEntity table(Namespace namespace, String name) {
    return TableEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(namespace)
        .withProperties(Collections.emptyMap())
        .withColumns(Collections.emptyList())
        .withPartitioning(new Transform[0])
        .withSortOrders(new SortOrder[0])
        .withIndexes(Indexes.EMPTY_INDEXES)
        .withDistribution(Distributions.NONE)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
