/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestKvGarbageCollector {
  public Config getConfig() {
    Config config = Mockito.mock(Config.class);
    File file = Files.createTempDir();
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
  void testScheduler() {
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
              new KvRangeScan.KvRangeScanBuilder()
                  .start("_".getBytes())
                  .end("z".getBytes())
                  .startInclusive(false)
                  .endInclusive(false)
                  .build());

      // 7 for real-data(3 testA, 2 testB, 2 testC), 3 commit marks can't be seen as they start with
      // 0x1E, last_timestamp can be seen as they have not been stored to the backend.
      Assertions.assertEquals(7, allData.size());

      KvGarbageCollector kvGarbageCollector = new KvGarbageCollector(kvBackend, config);
      Mockito.doReturn(2000L).when(config).get(KV_DELETE_AFTER_TIME);

      // Wait TTL time to make sure the data is expired, please see ENTITY_KV_TTL
      Thread.sleep(3000);
      kvGarbageCollector.collectAndClean();

      allData =
          kvBackend.scan(
              new KvRangeScan.KvRangeScanBuilder()
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
}
