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

package org.apache.gravitino.storage.kv;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static org.apache.gravitino.Configs.ENTITY_KV_ROCKSDB_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_KV_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntitySerDeFactory;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.TestEntityStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Disabled("Gravitino will not support KV entity store since 0.6.0, so we disable this test.")
public class TestKvEntityStorage extends TestEntityStorage {
  @BeforeEach
  @AfterEach
  public void cleanEnv() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(KV_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  public Config getConfig() throws IOException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTITY_KV_ROCKSDB_BACKEND_PATH)).thenReturn("/tmp/gravitino");
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    return config;
  }

  @Test
  void testCreateKvEntityStore() throws IOException {
    Config config = getConfig();
    FileUtils.deleteDirectory(FileUtils.getFile("/tmp/gravitino"));

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));

      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake = createBaseMakeLake(1L, "metalake", auditInfo);
      BaseMetalake metalakeCopy = createBaseMakeLake(2L, "metalakeCopy", auditInfo);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake"), "catalog", auditInfo);
      CatalogEntity catalogCopy =
          createCatalog(2L, Namespace.of("metalake"), "catalogCopy", auditInfo);
      CatalogEntity catalogCopyAgain =
          createCatalog(3L, Namespace.of("metalake"), "catalogCopyAgain", auditInfo);

      // First, we try to test transactional is OK
      final NameIdentifier metalakeID1 = metalake.nameIdentifier();
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalakeID1, Entity.EntityType.METALAKE, BaseMetalake.class));

      store.put(metalake);
      store.put(catalog);
      store.put(metalakeCopy);
      store.put(catalogCopy);
      store.put(catalogCopyAgain);

      Metalake retrievedMetalake =
          store.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
      Assertions.assertEquals(metalake, retrievedMetalake);
      CatalogEntity retrievedCatalog =
          store.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(catalog, retrievedCatalog);
      Metalake retrievedMetalakeCopy =
          store.get(metalakeCopy.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
      Assertions.assertEquals(metalakeCopy, retrievedMetalakeCopy);
      CatalogEntity retrievedCatalogCopy =
          store.get(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(catalogCopy, retrievedCatalogCopy);

      // Test scan and store list interface
      List<CatalogEntity> catalogEntityList =
          store.list(catalog.namespace(), CatalogEntity.class, Entity.EntityType.CATALOG);
      Assertions.assertEquals(3, catalogEntityList.size());
      Assertions.assertTrue(catalogEntityList.contains(catalog));
      Assertions.assertTrue(catalogEntityList.contains(catalogCopy));
      Assertions.assertTrue(catalogEntityList.contains(catalogCopyAgain));

      Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
      store.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
      final NameIdentifier metalakeID2 = catalog.nameIdentifier();
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalakeID2, Entity.EntityType.CATALOG, CatalogEntity.class));

      Assertions.assertThrows(
          EntityAlreadyExistsException.class, () -> store.put(catalogCopy, false));
      store.delete(catalogCopy.nameIdentifier(), Entity.EntityType.CATALOG);
      final NameIdentifier metalakeID3 = catalogCopy.nameIdentifier();
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalakeID3, Entity.EntityType.CATALOG, CatalogEntity.class));

      Assertions.assertThrowsExactly(
          NonEmptyEntityException.class,
          () -> store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
      store.delete(catalogCopyAgain.nameIdentifier(), Entity.EntityType.CATALOG);
      Assertions.assertTrue(store.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
      final NameIdentifier metalakeID4 = metalake.nameIdentifier();
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalakeID4, Entity.EntityType.METALAKE, BaseMetalake.class));

      // Test update
      BaseMetalake updatedMetalake = createBaseMakeLake(1L, "updatedMetalake", auditInfo);
      store.put(metalake);
      store.update(
          metalake.nameIdentifier(),
          BaseMetalake.class,
          Entity.EntityType.METALAKE,
          l -> updatedMetalake);
      Assertions.assertEquals(
          updatedMetalake,
          store.get(
              updatedMetalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class));
      final NameIdentifier metalakeID5 = metalake.nameIdentifier();
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> store.get(metalakeID5, Entity.EntityType.METALAKE, BaseMetalake.class));

      // Add new updateMetaLake.
      // 'updatedMetalake2' is a new name, which will trigger id allocation
      BaseMetalake updatedMetalake2 = createBaseMakeLake(3L, "updatedMetalake2", auditInfo);
      store.put(updatedMetalake2);
    }
  }

  @Test
  @Disabled("KvEntityStore is not thread safe after issue #780")
  void testConcurrentIssues() throws IOException, ExecutionException, InterruptedException {
    Config config = getConfig();
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTITY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            10,
            20,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1000),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("gravitino-t-%d").build());

    CompletionService<Boolean> future = new ExecutorCompletionService<>(threadPoolExecutor);

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      Assertions.assertTrue(store instanceof KvEntityStore);
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));

      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake = createBaseMakeLake(1L, "metalake", auditInfo);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake"), "catalog", auditInfo);

      store.put(metalake);
      store.put(catalog);
      Assertions.assertNotNull(
          store.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class));

      // Delete the catalog entity, and we try to use multi-thread to delete it and make sure only
      // one thread can delete it.
      for (int i = 0; i < 10; i++) {
        future.submit(
            () ->
                store.delete(NameIdentifier.of("metalake", "catalog"), Entity.EntityType.CATALOG));
      }
      int totalSuccessNum = 0;
      for (int i = 0; i < 10; i++) {
        totalSuccessNum += future.take().get() ? 1 : 0;
      }
      Assertions.assertEquals(1, totalSuccessNum);

      // Try to use multi-thread to put the same catalog entity, and make sure only one thread can
      // put it.
      for (int i = 0; i < 20; i++) {
        future.submit(
            () -> {
              store.put(catalog); /* overwrite is false, then only one will save it successfully */
              return null;
            });
      }

      int totalFailed = 0;
      for (int i = 0; i < 20; i++) {
        try {
          future.take().get();
        } catch (Exception e) {
          Assertions.assertTrue(e.getCause() instanceof EntityAlreadyExistsException);
          totalFailed++;
        }
      }
      Assertions.assertEquals(19, totalFailed);

      // Try to use multi-thread to update the same catalog entity, and make sure only one thread
      // can update it.
      for (int i = 0; i < 10; i++) {
        future.submit(
            () -> {
              // Ten threads rename the catalog entity from 'catalog' to 'catalog1' at the same
              // time.
              store.update(
                  NameIdentifier.of("metalake", "catalog"),
                  CatalogEntity.class,
                  Entity.EntityType.CATALOG,
                  e -> {
                    AuditInfo auditInfo1 =
                        AuditInfo.builder()
                            .withCreator("creator1")
                            .withCreateTime(Instant.now())
                            .build();
                    return createCatalog(1L, Namespace.of("metalake"), "catalog1", auditInfo1);
                  });
              return null;
            });
      }

      totalFailed = 0;
      for (int i = 0; i < 10; i++) {
        try {
          future.take().get();
        } catch (Exception e) {
          // It may throw NoSuchEntityException or AlreadyExistsException
          // NoSuchEntityException: because old entity has been renamed by the other thread already,
          // we can't get the old one.
          // AlreadyExistsException: because the entity has been renamed by the other thread
          // already, we can't rename it again.
          Assertions.assertTrue(
              e.getCause() instanceof AlreadyExistsException
                  || e.getCause() instanceof NoSuchEntityException);
          totalFailed++;
        }
      }
      Assertions.assertEquals(9, totalFailed);
    }
  }
}
