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
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaVersion;
import java.time.Instant;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityStorageBackend {
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

  @Test
  public void testCreateKvEntityStore() {
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

      BaseMetalake metalake =
          new BaseMetalake.Builder()
              .withId(1L)
              .withName("metalake")
              .withAuditInfo(auditInfo)
              .withVersion(SchemaVersion.V_0_1)
              .build();

      BaseMetalake metalakeCopy =
          new BaseMetalake.Builder()
              .withId(1L)
              .withName("metalakeCopy")
              .withAuditInfo(auditInfo)
              .withVersion(SchemaVersion.V_0_1)
              .build();

      CatalogEntity catalog =
          new CatalogEntity.Builder()
              .withId(1L)
              .withName("catalog")
              .withNamespace(Namespace.of("metalake"))
              .withType(Type.RELATIONAL)
              .withMetalakeId(1L)
              .withAuditInfo(auditInfo)
              .build();

      CatalogEntity catalogCopy =
          new CatalogEntity.Builder()
              .withId(1L)
              .withName("catalogCopy")
              .withNamespace(Namespace.of("metalake"))
              .withType(Type.RELATIONAL)
              .withMetalakeId(1L)
              .withAuditInfo(auditInfo)
              .build();
      CatalogEntity catalogCopyAgain =
          new CatalogEntity.Builder()
              .withId(1L)
              .withName("catalogCopyAgain")
              .withNamespace(Namespace.of("metalake"))
              .withType(Type.RELATIONAL)
              .withMetalakeId(1L)
              .withAuditInfo(auditInfo)
              .build();

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

      store.delete(metalake.nameIdentifier(), EntityType.METALAKE);
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));

      Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
      store.delete(catalog.nameIdentifier(), EntityType.CATALOG);
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class));

      // Test update
      BaseMetalake updatedMetalake =
          new BaseMetalake.Builder()
              .withId(metalake.getId())
              .withName("updatedMetalake")
              .withAuditInfo(auditInfo)
              .withVersion(SchemaVersion.V_0_1)
              .build();
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
      BaseMetalake updatedMetalake2 =
          new BaseMetalake.Builder()
              .withId(metalake.getId())
              .withName("updatedMetalake2")
              .withAuditInfo(auditInfo)
              .withVersion(SchemaVersion.V_0_1)
              .build();
      store.put(updatedMetalake2);
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
