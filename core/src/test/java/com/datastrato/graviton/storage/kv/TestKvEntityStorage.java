/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Catalog.Type;
import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDeFactory;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.EntityStoreFactory;
import com.datastrato.graviton.Metalake;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.AlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.exceptions.NonEmptyEntityException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaEntity;
import com.datastrato.graviton.meta.SchemaVersion;
import com.datastrato.graviton.meta.TableEntity;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvEntityStorage {
  public static final String ROCKS_DB_STORE_PATH =
      "/tmp/graviton_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  @BeforeEach
  @AfterEach
  public void cleanEnv() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  public BaseMetalake createBaseMakeLake(String name, AuditInfo auditInfo) {
    return new BaseMetalake.Builder()
        .withId(1L)
        .withName(name)
        .withAuditInfo(auditInfo)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  public CatalogEntity createCatalog(Namespace namespace, String name, AuditInfo auditInfo) {
    return new CatalogEntity.Builder()
        .withId(1L)
        .withName(name)
        .withNamespace(namespace)
        .withType(Type.RELATIONAL)
        .withProvider("test")
        .withAuditInfo(auditInfo)
        .build();
  }

  public SchemaEntity createSchemaEntity(Namespace namespace, String name, AuditInfo auditInfo) {
    return new SchemaEntity.Builder()
        .withId(1L)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  public TableEntity createTableEntity(Namespace namespace, String name, AuditInfo auditInfo) {
    return new TableEntity.Builder()
        .withId(1L)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  @Test
  void testEntityUpdate() throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);

    Assertions.assertEquals(ROCKS_DB_STORE_PATH, config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));

      BaseMetalake metalake = createBaseMakeLake("metalake", auditInfo);
      CatalogEntity catalog = createCatalog(Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy = createCatalog(Namespace.of("metalake"), "catalogCopy", auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      TableEntity table1 =
          createTableEntity(Namespace.of("metalake", "catalog", "schema1"), "table1", auditInfo);

      SchemaEntity schema2 =
          createSchemaEntity(Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      TableEntity table1InSchema2 =
          createTableEntity(Namespace.of("metalake", "catalog", "schema2"), "table1", auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);

      // Try to check an update option is what we expected
      store.update(
          metalake.nameIdentifier(),
          BaseMetalake.class,
          EntityType.METALAKE,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator1")
                    .withCreateTime(Instant.now())
                    .build();
            return createBaseMakeLake("metalakeChanged", auditInfo1);
          });

      // Check metalake entity and sub-entities are already changed.
      BaseMetalake updatedMetalake =
          store.get(NameIdentifier.of("metalakeChanged"), EntityType.METALAKE, BaseMetalake.class);
      Assertions.assertEquals("creator1", updatedMetalake.auditInfo().creator());

      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () -> store.get(NameIdentifier.of("metalake"), EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog"),
              EntityType.CATALOG,
              CatalogEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema1"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema1", "table1"),
              EntityType.TABLE,
              TableEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema2"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema1", "table1"),
              EntityType.TABLE,
              TableEntity.class));

      // Check catalog entities and sub-entities are already changed.
      store.update(
          NameIdentifier.of("metalakeChanged", "catalog"),
          CatalogEntity.class,
          EntityType.CATALOG,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator2")
                    .withCreateTime(Instant.now())
                    .build();
            return createCatalog(Namespace.of("metalakeChanged"), "catalogChanged", auditInfo1);
          });
      CatalogEntity updatedCatalog =
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged"),
              EntityType.CATALOG,
              CatalogEntity.class);
      Assertions.assertEquals("creator2", updatedCatalog.auditInfo().creator());
      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () ->
              store.get(
                  NameIdentifier.of("metalakeChanged", "catalog"),
                  EntityType.CATALOG,
                  CatalogEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "table1"),
              EntityType.TABLE,
              TableEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              TableEntity.class));

      // Check schema entities and sub-entities are already changed.
      store.update(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
          SchemaEntity.class,
          EntityType.SCHEMA,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator3")
                    .withCreateTime(Instant.now())
                    .build();
            return createSchemaEntity(
                Namespace.of("metalakeChanged", "catalogChanged"), "schemaChanged", auditInfo1);
          });

      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () ->
              store.get(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
                  EntityType.SCHEMA,
                  SchemaEntity.class));
      SchemaEntity updatedSchema =
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              EntityType.SCHEMA,
              SchemaEntity.class);
      Assertions.assertEquals("creator3", updatedSchema.auditInfo().creator());

      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
              EntityType.TABLE,
              TableEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
              EntityType.SCHEMA,
              SchemaEntity.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              TableEntity.class));

      // Check table entities
      store.update(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
          TableEntity.class,
          EntityType.TABLE,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator4")
                    .withCreateTime(Instant.now())
                    .build();
            return createTableEntity(
                Namespace.of("metalakeChanged", "catalogChanged", "schemaChanged"),
                "tableChanged",
                auditInfo1);
          });

      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () ->
              store.get(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "table1"),
                  EntityType.TABLE,
                  TableEntity.class));
      TableEntity updatedTable =
          store.get(
              NameIdentifier.of(
                  "metalakeChanged", "catalogChanged", "schemaChanged", "tableChanged"),
              EntityType.TABLE,
              TableEntity.class);
      Assertions.assertEquals("creator4", updatedTable.auditInfo().creator());

      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              TableEntity.class));

      store.delete(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
          EntityType.TABLE);
      // Update a deleted entities
      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () ->
              store.update(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
                  TableEntity.class,
                  EntityType.TABLE,
                  (e) -> e));
      // The updated entities already existed, should throw exception
      Assertions.assertThrowsExactly(
          AlreadyExistsException.class,
          () ->
              store.update(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
                  SchemaEntity.class,
                  EntityType.SCHEMA,
                  e -> {
                    AuditInfo auditInfo1 =
                        new AuditInfo.Builder()
                            .withCreator("creator5")
                            .withCreateTime(Instant.now())
                            .build();
                    return createSchemaEntity(
                        Namespace.of("metalakeChanged", "catalogChanged"),
                        "schemaChanged",
                        auditInfo1);
                  }));
      // Update operations do not conatin any changes in name
      store.update(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
          SchemaEntity.class,
          EntityType.SCHEMA,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator6")
                    .withCreateTime(Instant.now())
                    .build();
            return createSchemaEntity(
                Namespace.of("metalakeChanged", "catalogChanged"), "schema2", auditInfo1);
          });
      Assertions.assertEquals(
          "creator6",
          store
              .get(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
                  EntityType.SCHEMA,
                  SchemaEntity.class)
              .auditInfo()
              .creator());
    }
  }

  @Test
  void testEntityDelete() throws IOException {
    // TODO
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn("/tmp/graviton");

    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));

      BaseMetalake metalake = createBaseMakeLake("metalake", auditInfo);
      CatalogEntity catalog = createCatalog(Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy = createCatalog(Namespace.of("metalake"), "catalogCopy", auditInfo);

      SchemaEntity schema1 =
          createSchemaEntity(Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      TableEntity table1 =
          createTableEntity(Namespace.of("metalake", "catalog", "schema1"), "table1", auditInfo);

      SchemaEntity schema2 =
          createSchemaEntity(Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      TableEntity table1InSchema2 =
          createTableEntity(Namespace.of("metalake", "catalog", "schema2"), "table1", auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);

      // Now try to get
      Assertions.assertEquals(
          metalake, store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertEquals(
          catalog, store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));
      Assertions.assertEquals(
          catalogCopy,
          store.get(catalogCopy.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));
      Assertions.assertEquals(
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class));
      Assertions.assertEquals(
          schema2, store.get(schema2.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class));
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, TableEntity.class));
      Assertions.assertEquals(
          table1InSchema2,
          store.get(table1InSchema2.nameIdentifier(), EntityType.TABLE, TableEntity.class));

      // Delete the table 'metalake.catalog.schema2.table1'
      Assertions.assertTrue(store.delete(table1InSchema2.nameIdentifier(), EntityType.TABLE));
      Assertions.assertFalse(store.exists(table1InSchema2.nameIdentifier(), EntityType.TABLE));
      // Make sure table 'metalake.catalog.schema1.table1' still exist;
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, TableEntity.class));
      // Make sure schema 'metalake.catalog.schema2' still exist;
      Assertions.assertEquals(
          schema2, store.get(schema2.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class));
      // Re-insert table1Inschema2 and everything is OK
      store.put(table1InSchema2);
      Assertions.assertTrue(store.exists(table1InSchema2.nameIdentifier(), EntityType.TABLE));

      // Delete the schema 'metalake.catalog.schema1' but failed, because it ha sub-entities;
      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(schema1.nameIdentifier(), EntityType.SCHEMA));
      // Make sure schema 'metalake.catalog.schema1' and table 'metalake.catalog.schema1.table1'
      // has not been deleted yet;
      Assertions.assertTrue(store.exists(schema1.nameIdentifier(), EntityType.SCHEMA));
      Assertions.assertTrue(store.exists(table1.nameIdentifier(), EntityType.TABLE));

      // Delete table1 and schema1
      Assertions.assertTrue(store.delete(table1.nameIdentifier(), EntityType.TABLE));
      Assertions.assertTrue(store.delete(schema1.nameIdentifier(), EntityType.SCHEMA));
      // Make sure table1 in 'metalake.catalog.schema1' can't be access;
      Assertions.assertFalse(store.exists(table1.nameIdentifier(), EntityType.TABLE));
      Assertions.assertFalse(store.exists(schema1.nameIdentifier(), EntityType.SCHEMA));
      // Now we re-insert table1 and schema1, and everything should be OK
      store.put(schema1);
      store.put(table1);
      Assertions.assertEquals(
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class));
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, TableEntity.class));

      // Now try to delete all schemas under catalog;
      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(catalog.nameIdentifier(), EntityType.CATALOG));
      store.delete(table1.nameIdentifier(), EntityType.TABLE);
      store.delete(schema1.nameIdentifier(), EntityType.SCHEMA);
      store.delete(table1InSchema2.nameIdentifier(), EntityType.TABLE);
      store.delete(schema2.nameIdentifier(), EntityType.SCHEMA);

      store.delete(catalog.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertFalse(store.exists(catalog.nameIdentifier(), EntityType.CATALOG));

      // Now delete catalog 'catalogCopy' and metalake
      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(metalake.nameIdentifier(), EntityType.METALAKE));
      store.delete(catalogCopy.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertFalse(store.exists(catalogCopy.nameIdentifier(), EntityType.CATALOG));

      store.delete(metalake.nameIdentifier(), EntityType.METALAKE);
      Assertions.assertFalse(store.exists(metalake.nameIdentifier(), EntityType.METALAKE));

      // Store all entities again
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);

      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(schema1.nameIdentifier(), EntityType.SCHEMA));

      Assertions.assertEquals(
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, SchemaEntity.class));

      // Test cascade delete
      store.delete(schema1.nameIdentifier(), EntityType.SCHEMA, true);
      try {
        store.get(table1.nameIdentifier(), EntityType.TABLE, TableEntity.class);
      } catch (Exception e) {
        Assertions.assertTrue(e instanceof NoSuchEntityException);
        Assertions.assertTrue(e.getMessage().contains("metalake.catalog.schema1"));
      }

      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(catalog.nameIdentifier(), EntityType.CATALOG));
      store.delete(catalog.nameIdentifier(), EntityType.CATALOG, true);
      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () -> store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));

      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () -> store.get(schema2.nameIdentifier(), EntityType.SCHEMA, CatalogEntity.class));

      Assertions.assertTrue(store.delete(metalake.nameIdentifier(), EntityType.METALAKE, true));

      // catalog has already deleted, so we can't delete it again and should return false
      Assertions.assertFalse(store.delete(catalog.nameIdentifier(), EntityType.CATALOG));
      Assertions.assertFalse(store.delete(schema2.nameIdentifier(), EntityType.SCHEMA));
      Assertions.assertFalse(store.delete(metalake.nameIdentifier(), EntityType.METALAKE));
    }
  }

  @Test
  void testCreateKvEntityStore() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn("/tmp/graviton");

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));

      AuditInfo auditInfo =
          new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake = createBaseMakeLake("metalake", auditInfo);
      BaseMetalake metalakeCopy = createBaseMakeLake("metalakeCopy", auditInfo);
      CatalogEntity catalog = createCatalog(Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy = createCatalog(Namespace.of("metalake"), "catalogCopy", auditInfo);
      CatalogEntity catalogCopyAgain =
          createCatalog(Namespace.of("metalake"), "catalogCopyAgain", auditInfo);

      // First, we try to test transactional is OK
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));
      try {
        store.executeInTransaction(
            () -> {
              store.put(metalake);
              // Try to mock an exception
              double a = 1 / 0;
              store.put(catalog);
              return null;
            });
      } catch (Exception e) {
        Assertions.assertTrue(e instanceof ArithmeticException);
      }

      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));

      store.executeInTransaction(
          () -> {
            store.put(metalake);
            store.put(catalog);
            store.put(metalakeCopy);
            store.put(catalogCopy);
            store.put(catalogCopyAgain);
            return null;
          });

      Metalake retrievedMetalake =
          store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class);
      Assertions.assertEquals(metalake, retrievedMetalake);
      CatalogEntity retrievedCatalog =
          store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(catalog, retrievedCatalog);
      Metalake retrievedMetalakeCopy =
          store.get(metalakeCopy.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class);
      Assertions.assertEquals(metalakeCopy, retrievedMetalakeCopy);
      CatalogEntity retrievedCatalogCopy =
          store.get(catalogCopy.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(catalogCopy, retrievedCatalogCopy);

      // Test scan and store list interface
      List<CatalogEntity> catalogEntityList =
          store.list(catalog.namespace(), CatalogEntity.class, EntityType.CATALOG);
      Assertions.assertEquals(3, catalogEntityList.size());
      Assertions.assertTrue(catalogEntityList.contains(catalog));
      Assertions.assertTrue(catalogEntityList.contains(catalogCopy));
      Assertions.assertTrue(catalogEntityList.contains(catalogCopyAgain));

      Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
      store.delete(catalog.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));

      Assertions.assertThrows(
          EntityAlreadyExistsException.class, () -> store.put(catalogCopy, false));
      store.delete(catalogCopy.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(catalogCopy.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));

      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(metalake.nameIdentifier(), EntityType.METALAKE));
      store.delete(catalogCopyAgain.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertTrue(store.delete(metalake.nameIdentifier(), EntityType.METALAKE));
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));

      // Test update
      BaseMetalake updatedMetalake = createBaseMakeLake("updatedMetalake", auditInfo);
      store.put(metalake);
      store.update(
          metalake.nameIdentifier(), BaseMetalake.class, EntityType.METALAKE, l -> updatedMetalake);
      Assertions.assertEquals(
          updatedMetalake,
          store.get(updatedMetalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));

      // Add new updateMetaLake.
      // 'updatedMetalake2' is a new name, which will trigger id allocation
      BaseMetalake updatedMetalake2 = createBaseMakeLake("updatedMetalake2", auditInfo);
      store.put(updatedMetalake2);
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
