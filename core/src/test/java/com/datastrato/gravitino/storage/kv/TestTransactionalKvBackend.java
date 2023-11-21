/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestTransactionalKvBackend {

  private Config getConfig() {
    File file = Files.createTempDir();
    file.deleteOnExit();
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3L);
    return config;
  }

  private KvBackend getKvBackEnd(Config config) throws IOException {
    KvBackend kvBackend = new RocksDBKvBackend();
    kvBackend.initialize(config);
    return kvBackend;
  }

  @Test
  void testGet() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    Assertions.assertEquals("value1", new String(transactionalKvBackend.get("key1".getBytes())));
    Assertions.assertEquals("value2", new String(transactionalKvBackend.get("key2".getBytes())));

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value3".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value4".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    Assertions.assertEquals("value3", new String(transactionalKvBackend.get("key1".getBytes())));
    Assertions.assertEquals("value4", new String(transactionalKvBackend.get("key2".getBytes())));
  }

  @Test
  void testDelete() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.delete("key1".getBytes());
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNotNull(transactionalKvBackend.get("key2".getBytes()));
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.delete("key2".getBytes());
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key2".getBytes()));
  }

  @Test
  void testScan() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();

    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.put("key3".getBytes(), "value3".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    Map<String, String> map =
        new HashMap<String, String>() {
          {
            put("key1", "value1");
            put("key2", "value2");
            put("key3", "value3");
          }
        };

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    List<Pair<byte[], byte[]>> pairs =
        transactionalKvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start("k".getBytes())
                .end("kez".getBytes())
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(3, pairs.size());
    for (Pair<byte[], byte[]> pair : pairs) {
      Assertions.assertEquals(map.get(new String(pair.getKey())), new String(pair.getValue()));
    }
    List<String> resultList =
        pairs.stream().map(p -> new String(p.getKey())).collect(Collectors.toList());
    Assertions.assertEquals(Lists.newArrayList("key1", "key2", "key3"), resultList);

    pairs =
        transactionalKvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start("key1".getBytes())
                .end("kez".getBytes())
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(2, pairs.size());
    for (Pair<byte[], byte[]> pair : pairs) {
      Assertions.assertEquals(map.get(new String(pair.getKey())), new String(pair.getValue()));
    }
    resultList = pairs.stream().map(p -> new String(p.getKey())).collect(Collectors.toList());
    Assertions.assertEquals(Lists.newArrayList("key2", "key3"), resultList);

    pairs =
        transactionalKvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start("key1".getBytes())
                .end("key3".getBytes())
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(1, pairs.size());
    for (Pair<byte[], byte[]> pair : pairs) {
      Assertions.assertEquals(map.get(new String(pair.getKey())), new String(pair.getValue()));
    }
    resultList = pairs.stream().map(p -> new String(p.getKey())).collect(Collectors.toList());
    Assertions.assertEquals(Lists.newArrayList("key2"), resultList);

    pairs =
        transactionalKvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start("key3".getBytes())
                .end("kez".getBytes())
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(1, pairs.size());
    for (Pair<byte[], byte[]> pair : pairs) {
      Assertions.assertEquals(map.get(new String(pair.getKey())), new String(pair.getValue()));
    }
    resultList = pairs.stream().map(p -> new String(p.getKey())).collect(Collectors.toList());
    Assertions.assertEquals(Lists.newArrayList("key3"), resultList);

    pairs =
        transactionalKvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start("kf".getBytes())
                .end("kg".getBytes())
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(0, pairs.size());
  }

  @Test
  void testDeleteRange() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.put("key3".getBytes(), "value3".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    transactionalKvBackend.deleteRange(
        new KvRangeScan.KvRangeScanBuilder()
            .start("key1".getBytes())
            .end("key3".getBytes())
            .startInclusive(true)
            .endInclusive(true)
            .build());
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    transactionalKvBackend.begin();
    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key2".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key3".getBytes()));
  }

  @Test
  void testException() throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionalKvBackendImpl kvTransactionManager =
        new TransactionalKvBackendImpl(
            kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
    kvTransactionManager.begin();
    List<Pair<byte[], byte[]>> pairs =
        Lists.newArrayList(
            Pair.of(
                kvTransactionManager.constructKey("key1".getBytes()),
                kvTransactionManager.constructValue("value1".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key2".getBytes()),
                kvTransactionManager.constructValue("value2".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key3".getBytes()),
                kvTransactionManager.constructValue("value3".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key4".getBytes()),
                kvTransactionManager.constructValue("value4".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key5".getBytes()),
                kvTransactionManager.constructValue("value6".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key6".getBytes()),
                kvTransactionManager.constructValue("value7".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key7".getBytes()),
                kvTransactionManager.constructValue("value8".getBytes(), ValueStatusEnum.NORMAL)),
            Pair.of(
                kvTransactionManager.constructKey("key8".getBytes()),
                kvTransactionManager.constructValue("value9".getBytes(), ValueStatusEnum.NORMAL)),

            // Will throw NPE to roll back the transaction.
            Pair.of(kvTransactionManager.constructKey("key9".getBytes()), null));

    Pair<byte[], byte[]>[] arrayPair = pairs.toArray(new Pair[0]);

    for (int i = 0; i < 1000; i++) {
      ArrayUtils.shuffle(arrayPair);
      kvTransactionManager.putPairs.addAll(Arrays.stream(arrayPair).collect(Collectors.toList()));

      Assertions.assertThrows(Exception.class, kvTransactionManager::commit);

      kvTransactionManager =
          new TransactionalKvBackendImpl(
              kvBackend, new TransactionIdGeneratorImpl(kvBackend, config));
      kvTransactionManager.begin();
      Assertions.assertNull(kvTransactionManager.get("key1".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key2".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key3".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key4".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key5".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key6".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key7".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key8".getBytes()));
      Assertions.assertNull(kvTransactionManager.get("key9".getBytes()));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1})
  void testTransactionIdGeneratorQPS(int threadNum) throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            threadNum,
            threadNum,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("testTransactionIdGenerator-%d")
                .build());

    AtomicLong atomicLong = new AtomicLong(0);
    for (int i = 0; i < threadNum; i++) {
      threadPoolExecutor.execute(
          () -> {
            long current = System.currentTimeMillis();
            while (System.currentTimeMillis() - current <= 3000) {
              transactionIdGenerator.nextId();
              atomicLong.getAndIncrement();
            }
          });
    }

    Thread.currentThread().sleep(4000);

    System.out.println(String.format("%d thread qps is: %d/s", threadNum, atomicLong.get() / 3));
  }
}
