/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.DEFUALT_ENTITY_KV_STORE;
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
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.exceptions.NonEmptyEntityException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaVersion;
import com.datastrato.graviton.meta.rel.BaseSchema;
import com.datastrato.graviton.meta.rel.BaseTable;
import com.datastrato.graviton.rel.Column;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvEntityStorage {
  public static final String ROCKS_DB_STORE_PATH = "/tmp/graviton";

  @BeforeEach
  @AfterEach
  public void cleanEnv() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  static class MockSchemaBuilder
      extends BaseSchema.BaseSchemaBuilder<MockSchemaBuilder, BaseSchema> {
    @Override
    protected BaseSchema internalBuild() {
      BaseSchema baseSchema = new BaseSchema();
      try {
        setField(baseSchema, "id", id);
        setField(baseSchema, "catalogId", catalogId);
        setField(baseSchema, "namespace", namespace);
        setField(baseSchema, "name", name);
        setField(baseSchema, "comment", comment);
        setField(baseSchema, "properties", properties);
        setField(baseSchema, "auditInfo", auditInfo);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return baseSchema;
    }

    private void setField(BaseSchema object, String fieldName, Object value)
        throws NoSuchFieldException, IllegalAccessException {
      Field field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(object, value);
    }
  }

  static class MockTableBuilder extends BaseTable.BaseTableBuilder<MockTableBuilder, BaseTable> {

    @Override
    protected BaseTable internalBuild() {
      BaseTable baseTable = new BaseTable();
      try {
        setField(baseTable, "id", id);
        setField(baseTable, "schemaId", schemaId);
        setField(baseTable, "namespace", namespace);
        setField(baseTable, "name", name);
        setField(baseTable, "comment", comment);
        setField(baseTable, "properties", properties);
        setField(baseTable, "auditInfo", auditInfo);
        setField(baseTable, "columns", columns);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return baseTable;
    }

    private void setField(BaseTable object, String fieldName, Object value)
        throws NoSuchFieldException, IllegalAccessException {
      Field field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(object, value);
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
        .withMetalakeId(1L)
        .withAuditInfo(auditInfo)
        .build();
  }

  public BaseSchema createBaseschema(Namespace namespace, String name, AuditInfo auditInfo) {
    return new MockSchemaBuilder()
        .withId(1L)
        .withName(name)
        .withCatalogId(1L)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .withComment("a schmea")
        .build();
  }

  public BaseTable createBaseTable(Namespace namespace, String name, AuditInfo auditInfo) {
    return new MockTableBuilder()
        .withId(1L)
        .withSchemaId(1L)
        .withName(name)
        .withNameSpace(namespace)
        .withAuditInfo(auditInfo)
        .withComment("a table")
        .withColumns(
            new Column[] {
              new Column() {
                @Override
                public String name() {
                  return "test";
                }

                @Override
                public io.substrait.type.Type dataType() {
                  return TypeCreator.NULLABLE.I32;
                }

                @Override
                public String comment() {
                  return "test";
                }
              }
            })
        .build();
  }

  @Test
  void testEntityUpdate() throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
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
      BaseSchema schema1 =
          createBaseschema(Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      BaseTable table1 =
          createBaseTable(Namespace.of("metalake", "catalog", "schema1"), "table1", auditInfo);

      BaseSchema schema2 =
          createBaseschema(Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      BaseTable table1InSchema2 =
          createBaseTable(Namespace.of("metalake", "catalog", "schema2"), "table1", auditInfo);

      // Store all entities
      store.put(metalake);
      store.put(catalog);
      store.put(catalogCopy);
      store.put(schema1);
      store.put(schema2);
      store.put(table1);
      store.put(table1InSchema2);

      // Try to check an update option is what exepct
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

      // Check metalake entity and subenties are already changed.
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
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema1", "table1"),
              EntityType.TABLE,
              BaseTable.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema2"),
              EntityType.SCHEMA,
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalog", "schema1", "table1"),
              EntityType.TABLE,
              BaseTable.class));

      // Check catalog entitis and sub-entities are already changed.
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
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1", "table1"),
              EntityType.TABLE,
              BaseTable.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
              EntityType.SCHEMA,
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              BaseTable.class));

      // Check schema entitis and sub-entities are already changed.
      store.update(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
          BaseSchema.class,
          EntityType.SCHEMA,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator3")
                    .withCreateTime(Instant.now())
                    .build();
            return createBaseschema(
                Namespace.of("metalakeChanged", "catalogChanged"), "schemaChanged", auditInfo1);
          });

      Assertions.assertThrowsExactly(
          NoSuchEntityException.class,
          () ->
              store.get(
                  NameIdentifier.of("metalakeChanged", "catalogChanged", "schema1"),
                  EntityType.SCHEMA,
                  BaseSchema.class));
      BaseSchema updatedSchema =
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              EntityType.SCHEMA,
              BaseSchema.class);
      Assertions.assertEquals("creator3", updatedSchema.auditInfo().creator());

      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged"),
              EntityType.SCHEMA,
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
              EntityType.TABLE,
              BaseTable.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2"),
              EntityType.SCHEMA,
              BaseSchema.class));
      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              BaseTable.class));

      // Check table entities
      store.update(
          NameIdentifier.of("metalakeChanged", "catalogChanged", "schemaChanged", "table1"),
          BaseTable.class,
          EntityType.TABLE,
          e -> {
            AuditInfo auditInfo1 =
                new AuditInfo.Builder()
                    .withCreator("creator4")
                    .withCreateTime(Instant.now())
                    .build();
            return createBaseTable(
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
                  BaseTable.class));
      BaseTable updatedTable =
          store.get(
              NameIdentifier.of(
                  "metalakeChanged", "catalogChanged", "schemaChanged", "tableChanged"),
              EntityType.TABLE,
              BaseTable.class);
      Assertions.assertEquals("creator4", updatedTable.auditInfo().creator());

      Assertions.assertNotNull(
          store.get(
              NameIdentifier.of("metalakeChanged", "catalogChanged", "schema2", "table1"),
              EntityType.TABLE,
              BaseTable.class));
    }
  }

  @Test
  void testEntityDelete() throws IOException {
    // TODO
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
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

      BaseSchema schema1 =
          createBaseschema(Namespace.of("metalake", "catalog"), "schema1", auditInfo);
      BaseTable table1 =
          createBaseTable(Namespace.of("metalake", "catalog", "schema1"), "table1", auditInfo);

      BaseSchema schema2 =
          createBaseschema(Namespace.of("metalake", "catalog"), "schema2", auditInfo);
      BaseTable table1InSchema2 =
          createBaseTable(Namespace.of("metalake", "catalog", "schema2"), "table1", auditInfo);

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
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, BaseSchema.class));
      Assertions.assertEquals(
          schema2, store.get(schema2.nameIdentifier(), EntityType.SCHEMA, BaseSchema.class));
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, BaseTable.class));
      Assertions.assertEquals(
          table1InSchema2,
          store.get(table1InSchema2.nameIdentifier(), EntityType.TABLE, BaseTable.class));

      // Delete the table 'metalake.catalog.schema2.table1'
      store.delete(table1InSchema2.nameIdentifier(), EntityType.TABLE);
      Assertions.assertFalse(store.exists(table1InSchema2.nameIdentifier(), EntityType.TABLE));
      // Make sure table 'metalake.catalog.schema1.table1' still exist;
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, BaseTable.class));
      // Make sure schema 'metalake.catalog.schema2' still exist;
      Assertions.assertEquals(
          schema2, store.get(schema2.nameIdentifier(), EntityType.SCHEMA, BaseSchema.class));
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
      store.delete(table1.nameIdentifier(), EntityType.TABLE);
      store.delete(schema1.nameIdentifier(), EntityType.SCHEMA);
      // Make sure table1 in 'metalake.catalog.schema1' can't be access;
      Assertions.assertFalse(store.exists(table1.nameIdentifier(), EntityType.TABLE));
      Assertions.assertFalse(store.exists(schema1.nameIdentifier(), EntityType.SCHEMA));
      // Now we re-insert table1 and schema1, and everything should be OK
      store.put(schema1);
      store.put(table1);
      Assertions.assertEquals(
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, BaseSchema.class));
      Assertions.assertEquals(
          table1, store.get(table1.nameIdentifier(), EntityType.TABLE, BaseTable.class));

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
          schema1, store.get(schema1.nameIdentifier(), EntityType.SCHEMA, BaseSchema.class));

      // Test cascade delete
      store.delete(schema1.nameIdentifier(), EntityType.SCHEMA, true);
      try {
        store.get(table1.nameIdentifier(), EntityType.TABLE, BaseTable.class);
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
    }
  }

  @Test
  void testCreateKvEntityStore() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
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
