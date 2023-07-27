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

      store.put(metalake.nameIdentifier(), metalake);
      store.put(catalog.nameIdentifier(), catalog);
      store.put(metalakeCopy.nameIdentifier(), metalakeCopy);
      store.put(catalogCopy.nameIdentifier(), catalogCopy);

      Metalake retrievedMetalake = store.get(metalake.nameIdentifier(), BaseMetalake.class);
      Assertions.assertEquals(metalake, retrievedMetalake);
      CatalogEntity retrievedCatalog = store.get(catalog.nameIdentifier(), CatalogEntity.class);
      Assertions.assertEquals(catalog, retrievedCatalog);
      Metalake retrievedMetalakeCopy = store.get(metalakeCopy.nameIdentifier(), BaseMetalake.class);
      Assertions.assertEquals(metalakeCopy, retrievedMetalakeCopy);
      CatalogEntity retrievedCatalogCopy =
          store.get(catalogCopy.nameIdentifier(), CatalogEntity.class);
      Assertions.assertEquals(catalogCopy, retrievedCatalogCopy);

      store.delete(metalake.nameIdentifier());
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalake.nameIdentifier(), BaseMetalake.class));

      Assertions.assertThrows(
          EntityAlreadyExistsException.class,
          () -> store.put(catalog.nameIdentifier(), catalog, false));
      store.delete(catalog.nameIdentifier());
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(catalog.nameIdentifier(), CatalogEntity.class));
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
