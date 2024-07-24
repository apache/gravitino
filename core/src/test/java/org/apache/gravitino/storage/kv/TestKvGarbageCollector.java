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
import static org.apache.gravitino.storage.kv.TestKvEntityStorage.createBaseMakeLake;
import static org.apache.gravitino.storage.kv.TestKvEntityStorage.createCatalog;
import static org.apache.gravitino.storage.kv.TestKvEntityStorage.createFilesetEntity;
import static org.apache.gravitino.storage.kv.TestKvEntityStorage.createSchemaEntity;
import static org.apache.gravitino.storage.kv.TestKvEntityStorage.createTableEntity;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.getBinaryTransactionId;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.getTransactionId;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntitySerDeFactory;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.TransactionIdGenerator;
import org.apache.gravitino.storage.kv.KvGarbageCollector.LogHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("DefaultCharset")
@Disabled("Gravitino will not support KV entity store since 0.6.0, so we disable this test.")
class TestKvGarbageCollector {
  public Config getConfig() throws IOException {
    Config config = Mockito.mock(Config.class);
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTITY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    return config;
  }

  private KvBackend getKvBackEnd(Config config) throws IOException {
    KvBackend kvBackend = new RocksDBKvBackend();
    kvBackend.initialize(config);
    return kvBackend;
  }

  @Test
  void testScheduler() throws IOException {
    Config config = getConfig();
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L); // 20 minutes
    long dateTimelineMinute = config.get(STORE_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(10, Math.max(dateTimelineMinute / 10, 10));

    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(2 * 60 * 60 * 1000L); // 2 hours
    dateTimelineMinute = config.get(STORE_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(12, Math.max(dateTimelineMinute / 10, 10));

    Mockito.when(config.get(STORE_DELETE_AFTER_TIME))
        .thenReturn(2 * 60 * 60 * 24 * 1000L); // 2 days
    dateTimelineMinute = config.get(STORE_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(288, Math.max(dateTimelineMinute / 10, 10));
  }

  @Test
  void testCollectGarbage() throws IOException, InterruptedException {
    Config config = getConfig();
    try (KvBackend kvBackend = getKvBackEnd(config)) {
      TransactionIdGenerator transactionIdGenerator =
          new TransactionIdGeneratorImpl(kvBackend, config);
      TransactionalKvBackendImpl transactionalKvBackend =
          new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
      transactionalKvBackend.begin();
      transactionalKvBackend.put("testA".getBytes(), "v1".getBytes(), true);
      transactionalKvBackend.put("testB".getBytes(), "v1".getBytes(), true);
      transactionalKvBackend.put("testC".getBytes(), "v1".getBytes(), true);
      transactionalKvBackend.commit();
      transactionalKvBackend.closeTransaction();

      transactionalKvBackend.begin();
      transactionalKvBackend.put("testA".getBytes(), "v2".getBytes(), true);
      transactionalKvBackend.put("testB".getBytes(), "v2".getBytes(), true);
      transactionalKvBackend.commit();
      transactionalKvBackend.closeTransaction();

      transactionalKvBackend.begin();
      transactionalKvBackend.put("testA".getBytes(), "v3".getBytes(), true);
      transactionalKvBackend.delete("testC".getBytes());
      transactionalKvBackend.commit();
      transactionalKvBackend.closeTransaction();

      // Test data is OK
      transactionalKvBackend.begin();
      Assertions.assertEquals("v3", new String(transactionalKvBackend.get("testA".getBytes())));
      Assertions.assertEquals("v2", new String(transactionalKvBackend.get("testB".getBytes())));
      Assertions.assertNull(transactionalKvBackend.get("testC".getBytes()));
      List<Pair<byte[], byte[]>> allData =
          kvBackend.scan(
              new KvRange.KvRangeBuilder()
                  .start("_".getBytes())
                  .end("z".getBytes())
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());

      // 7 for real-data(3 testA, 2 testB, 2 testC), 3 commit marks can't be seen as they start with
      // 0x1E, last_timestamp can be seen as they have not been stored to the backend.
      Assertions.assertEquals(7, allData.size());

      // Set the TTL to 2 seconds before the kvGarbageCollector is created
      Mockito.doReturn(2000L).when(config).get(STORE_DELETE_AFTER_TIME);
      KvGarbageCollector kvGarbageCollector = new KvGarbageCollector(kvBackend, config, null);

      // Wait TTL time to make sure the data is expired, please see ENTITY_KV_TTL
      Thread.sleep(3000);
      kvGarbageCollector.collectAndClean();

      allData =
          kvBackend.scan(
              new KvRange.KvRangeBuilder()
                  .start("_".getBytes())
                  .end("z".getBytes())
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());
      // Except version 3 of testA and version 2 of testB will be left, all will be removed, so the
      // left key-value paris will be 2(real-data)
      Assertions.assertEquals(2, allData.size());
      Assertions.assertEquals("v3", new String(transactionalKvBackend.get("testA".getBytes())));
      Assertions.assertEquals("v2", new String(transactionalKvBackend.get("testB".getBytes())));
      Assertions.assertNull(transactionalKvBackend.get("testC".getBytes()));
    }
  }

  @Test
  void testRemoveWithGCCollector1() throws IOException, InterruptedException {
    Config config = getConfig();
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (!(store instanceof KvEntityStore)) {
        return;
      }
      KvEntityStore kvEntityStore = (KvEntityStore) store;
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 = createBaseMakeLake(1L, "metalake1", auditInfo);
      CatalogEntity catalog = createCatalog(1L, Namespace.of("metalake1"), "catalog1", auditInfo);
      SchemaEntity schemaEntity =
          createSchemaEntity(1L, Namespace.of("metalake1", "catalog1"), "schema1", auditInfo);
      TableEntity tableEntity =
          createTableEntity(
              1L, Namespace.of("metalake1", "catalog1", "schema1"), "table1", auditInfo);
      FilesetEntity filesetEntity =
          createFilesetEntity(
              1L, Namespace.of("metalake1", "catalog1", "schema1"), "fileset1", auditInfo);

      kvEntityStore.put(metalake1);
      kvEntityStore.put(catalog);
      kvEntityStore.put(schemaEntity);
      kvEntityStore.put(tableEntity);
      kvEntityStore.put(filesetEntity);
      kvEntityStore.put(
          UserEntity.builder()
              .withId(1L)
              .withAuditInfo(auditInfo)
              .withName("the same")
              .withNamespace(Namespace.of("metalake1", "catalog1", "schema1"))
              .build());
      kvEntityStore.put(
          GroupEntity.builder()
              .withId(2L)
              .withAuditInfo(auditInfo)
              .withName("the same")
              .withNamespace(Namespace.of("metalake1", "catalog1", "schema1"))
              .build());

      // now try to scan raw data from kv store
      KvBackend kvBackend = kvEntityStore.backend;
      List<Pair<byte[], byte[]>> data =
          kvBackend.scan(
              new KvRange.KvRangeBuilder()
                  .start("_".getBytes(StandardCharsets.UTF_8))
                  .end("z".getBytes(StandardCharsets.UTF_8))
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());

      Assertions.assertEquals(7, data.size());

      KvGarbageCollector kvGarbageCollector = kvEntityStore.kvGarbageCollector;
      for (Pair<byte[], byte[]> pair : data) {
        byte[] key = pair.getKey();
        LogHelper helper = kvGarbageCollector.decodeKey(key);
        Assertions.assertNotSame(LogHelper.NONE, helper);

        switch (helper.type) {
          case METALAKE:
            Assertions.assertEquals(NameIdentifier.of("metalake1"), helper.identifier);
            break;
          case CATALOG:
            Assertions.assertEquals(NameIdentifier.of("metalake1", "catalog1"), helper.identifier);
            break;
          case SCHEMA:
            Assertions.assertEquals(
                NameIdentifier.of("metalake1", "catalog1", "schema1"), helper.identifier);
            break;
          case TABLE:
            Assertions.assertEquals(
                NameIdentifier.of("metalake1", "catalog1", "schema1", "table1"), helper.identifier);
            break;
          case FILESET:
            Assertions.assertEquals(
                NameIdentifier.of("metalake1", "catalog1", "schema1", "fileset1"),
                helper.identifier);
            break;
          case USER:
            Assertions.assertEquals(
                NameIdentifier.of("metalake1", "catalog1", "schema1", "the same"),
                helper.identifier);
            break;

          case GROUP:
            Assertions.assertEquals(
                NameIdentifier.of("metalake1", "catalog1", "schema1", "the same"),
                helper.identifier);
            break;
          default:
            Assertions.fail();
        }
      }
    }
  }

  @Test
  void testRemoveWithGCCollector2() throws IOException, InterruptedException {
    Config config = getConfig();
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      if (!(store instanceof KvEntityStore)) {
        return;
      }
      KvEntityStore kvEntityStore = (KvEntityStore) store;

      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 = createBaseMakeLake(1L, "metalake1", auditInfo);
      BaseMetalake metalake2 = createBaseMakeLake(2L, "metalake2", auditInfo);
      BaseMetalake metalake3 = createBaseMakeLake(3L, "metalake3", auditInfo);

      store.put(metalake1);
      store.put(metalake2);
      store.put(metalake3);

      store.delete(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE);

      store.put(metalake1);
      store.put(metalake2);
      store.put(metalake3);

      Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(1000L);
      Thread.sleep(1500);

      kvEntityStore.kvGarbageCollector.collectAndClean();

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE, BaseMetalake.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE, BaseMetalake.class));

      // Test catalog
      CatalogEntity catalog1 = createCatalog(1L, Namespace.of("metalake1"), "catalog1", auditInfo);
      CatalogEntity catalog2 = createCatalog(2L, Namespace.of("metalake1"), "catalog2", auditInfo);

      store.put(catalog1);
      store.put(catalog2);

      store.delete(NameIdentifier.of("metalake1", "catalog1"), Entity.EntityType.CATALOG);
      store.delete(NameIdentifier.of("metalake1", "catalog2"), Entity.EntityType.CATALOG);

      store.put(catalog1);
      store.put(catalog2);

      Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(1000L);
      Thread.sleep(1500);

      kvEntityStore.kvGarbageCollector.collectAndClean();

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog1"),
                  Entity.EntityType.CATALOG,
                  CatalogEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog2"),
                  Entity.EntityType.CATALOG,
                  CatalogEntity.class));

      // Test schema
      SchemaEntity schema1 =
          createSchemaEntity(1L, Namespace.of("metalake1", "catalog2"), "schema1", auditInfo);
      SchemaEntity schema2 =
          createSchemaEntity(2L, Namespace.of("metalake1", "catalog2"), "schema2", auditInfo);

      store.put(schema1);
      store.put(schema2);

      store.delete(NameIdentifier.of("metalake1", "catalog2", "schema1"), Entity.EntityType.SCHEMA);
      store.delete(NameIdentifier.of("metalake1", "catalog2", "schema2"), Entity.EntityType.SCHEMA);

      store.put(schema1);
      store.put(schema2);

      Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(1000L);
      Thread.sleep(1500);
      kvEntityStore.kvGarbageCollector.collectAndClean();

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog2", "schema1"),
                  Entity.EntityType.SCHEMA,
                  SchemaEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog2", "schema2"),
                  Entity.EntityType.SCHEMA,
                  SchemaEntity.class));

      // Test table
      TableEntity table1 =
          createTableEntity(
              1L, Namespace.of("metalake1", "catalog2", "schema2"), "table1", auditInfo);
      TableEntity table2 =
          createTableEntity(
              2L, Namespace.of("metalake1", "catalog2", "schema2"), "table2", auditInfo);

      store.put(table1);
      store.put(table2);

      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "table1"), Entity.EntityType.TABLE);
      store.delete(
          NameIdentifier.of("metalake1", "catalog2", "schema2", "table2"), Entity.EntityType.TABLE);

      store.put(table1);
      store.put(table2);

      Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(1000L);
      Thread.sleep(1500);
      kvEntityStore.kvGarbageCollector.collectAndClean();

      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog2", "schema2", "table1"),
                  Entity.EntityType.TABLE,
                  TableEntity.class));
      Assertions.assertDoesNotThrow(
          () ->
              store.get(
                  NameIdentifier.of("metalake1", "catalog2", "schema2", "table2"),
                  Entity.EntityType.TABLE,
                  TableEntity.class));
    }
  }

  @Test
  void testIncrementalGC() throws Exception {
    Config config = getConfig();
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      if (!(store instanceof KvEntityStore)) {
        return;
      }
      KvEntityStore kvEntityStore = (KvEntityStore) store;

      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 = createBaseMakeLake(1L, "metalake1", auditInfo);
      BaseMetalake metalake2 = createBaseMakeLake(2L, "metalake2", auditInfo);
      BaseMetalake metalake3 = createBaseMakeLake(3L, "metalake3", auditInfo);

      for (int i = 0; i < 10; i++) {
        store.put(metalake1);
        store.put(metalake2);
        store.put(metalake3);

        store.delete(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE);
        store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
        store.delete(NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE);

        Thread.sleep(10);
      }

      store.put(metalake1);
      store.put(metalake2);
      store.put(metalake3);

      Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(1000L);
      Thread.sleep(1500);

      // Scan raw key-value data from storage to confirm the data is deleted
      kvEntityStore.kvGarbageCollector.collectAndClean();
      List<Pair<byte[], byte[]>> allData =
          kvEntityStore.backend.scan(
              new KvRange.KvRangeBuilder()
                  .start("_".getBytes())
                  .end("z".getBytes())
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());

      Assertions.assertEquals(3, allData.size());

      long transactionId =
          getTransactionId(
              getBinaryTransactionId(kvEntityStore.kvGarbageCollector.commitIdHasBeenCollected));
      Assertions.assertNotEquals(1, transactionId);

      for (int i = 0; i < 10; i++) {
        store.delete(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE);
        store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
        store.delete(NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE);
        store.put(metalake1);
        store.put(metalake2);
        store.put(metalake3);
        Thread.sleep(10);
      }
      store.delete(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake2"), Entity.EntityType.METALAKE);
      store.delete(NameIdentifier.of("metalake3"), Entity.EntityType.METALAKE);

      Thread.sleep(1500);
      kvEntityStore.kvGarbageCollector.collectAndClean();

      allData =
          kvEntityStore.backend.scan(
              new KvRange.KvRangeBuilder()
                  .start("_".getBytes())
                  .end("z".getBytes())
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());

      Assertions.assertTrue(allData.isEmpty());

      long transactionIdV2 =
          getTransactionId(
              getBinaryTransactionId(kvEntityStore.kvGarbageCollector.commitIdHasBeenCollected));
      Assertions.assertTrue(transactionIdV2 > transactionId);
    }
  }
}
