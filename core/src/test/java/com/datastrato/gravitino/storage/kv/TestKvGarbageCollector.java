/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.storage.kv.TestKvEntityStorage.createBaseMakeLake;
import static com.datastrato.gravitino.storage.kv.TestKvEntityStorage.createCatalog;
import static com.datastrato.gravitino.storage.kv.TestKvEntityStorage.createFilesetEntity;
import static com.datastrato.gravitino.storage.kv.TestKvEntityStorage.createSchemaEntity;
import static com.datastrato.gravitino.storage.kv.TestKvEntityStorage.createTableEntity;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.storage.kv.KvGarbageCollector.LogHelper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("DefaultCharset")
class TestKvGarbageCollector {
  public Config getConfig() throws IOException {
    Config config = Mockito.mock(Config.class);
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3L);
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
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
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L); // 20 minutes
    long dateTimeLineMinute = config.get(KV_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(10, Math.max(dateTimeLineMinute / 10, 10));

    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(2 * 60 * 60 * 1000L); // 2 hours
    dateTimeLineMinute = config.get(KV_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(12, Math.max(dateTimeLineMinute / 10, 10));

    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(2 * 60 * 60 * 24 * 1000L); // 2 days
    dateTimeLineMinute = config.get(KV_DELETE_AFTER_TIME) / 1000 / 60;
    Assertions.assertEquals(288, Math.max(dateTimeLineMinute / 10, 10));
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

      KvGarbageCollector kvGarbageCollector = new KvGarbageCollector(kvBackend, config, null);
      Mockito.doReturn(2000L).when(config).get(KV_DELETE_AFTER_TIME);

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
  void testRemoveWithGCCollector() throws IOException, InterruptedException {
    Config config = Mockito.mock(Config.class);
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (!(store instanceof KvEntityStore)) {
        return;
      }
      KvEntityStore kvEntityStore = (KvEntityStore) store;
      store.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

      BaseMetalake metalake1 = createBaseMakeLake("metalake1", auditInfo);
      CatalogEntity catalog = createCatalog(Namespace.of("metalake1"), "catalog1", auditInfo);
      SchemaEntity schemaEntity =
          createSchemaEntity(Namespace.of("metalake1", "catalog1"), "schema1", auditInfo);
      TableEntity tableEntity =
          createTableEntity(Namespace.of("metalake1", "catalog1", "schema1"), "table1", auditInfo);
      FilesetEntity filesetEntity =
          createFilesetEntity(
              Namespace.of("metalake1", "catalog1", "schema1"), "fileset1", auditInfo);

      kvEntityStore.put(metalake1);
      kvEntityStore.put(catalog);
      kvEntityStore.put(schemaEntity);
      kvEntityStore.put(tableEntity);
      kvEntityStore.put(filesetEntity);

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

      Assertions.assertEquals(5, data.size());

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
          default:
            Assertions.fail();
        }
      }
    }
  }
}
