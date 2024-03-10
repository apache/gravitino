/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("DefaultCharset")
class TestTransactionalKvBackend {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestTransactionalKvBackend.class);

  private Config getConfig() throws IOException {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3000L);
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
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    Assertions.assertEquals("value1", new String(transactionalKvBackend.get("key1".getBytes())));
    Assertions.assertEquals("value2", new String(transactionalKvBackend.get("key2".getBytes())));

    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value3".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value4".getBytes(), true);
    transactionalKvBackend.commit();
    transactionalKvBackend.close();

    transactionalKvBackend.begin();
    Assertions.assertEquals("value3", new String(transactionalKvBackend.get("key1".getBytes())));
    Assertions.assertEquals("value4", new String(transactionalKvBackend.get("key2".getBytes())));
    transactionalKvBackend.close();
    transactionIdGenerator.close();
  }

  @Test
  void testDelete() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    transactionalKvBackend.delete("key1".getBytes());
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNotNull(transactionalKvBackend.get("key2".getBytes()));

    transactionalKvBackend.begin();
    transactionalKvBackend.delete("key2".getBytes());
    transactionalKvBackend.commit();

    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key2".getBytes()));
    transactionalKvBackend.close();
    transactionIdGenerator.close();
  }

  @Test
  void testScan() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
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
            new KvRange.KvRangeBuilder()
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
            new KvRange.KvRangeBuilder()
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
            new KvRange.KvRangeBuilder()
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
            new KvRange.KvRangeBuilder()
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
            new KvRange.KvRangeBuilder()
                .start("kf".getBytes())
                .end("kg".getBytes())
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(0, pairs.size());
    transactionIdGenerator.close();
  }

  @Test
  void testDeleteRange() throws IOException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    transactionalKvBackend.begin();
    transactionalKvBackend.put("key1".getBytes(), "value1".getBytes(), true);
    transactionalKvBackend.put("key2".getBytes(), "value2".getBytes(), true);
    transactionalKvBackend.put("key3".getBytes(), "value3".getBytes(), true);
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    transactionalKvBackend.deleteRange(
        new KvRange.KvRangeBuilder()
            .start("key1".getBytes())
            .end("key3".getBytes())
            .startInclusive(true)
            .endInclusive(true)
            .build());
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    Assertions.assertNull(transactionalKvBackend.get("key1".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key2".getBytes()));
    Assertions.assertNull(transactionalKvBackend.get("key3".getBytes()));
    transactionalKvBackend.close();
    transactionIdGenerator.close();
  }

  @Test
  void testException() throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackendImpl kvTransactionManager =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
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

    kvTransactionManager = new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    for (int i = 0; i < 10000; i++) {
      ArrayUtils.shuffle(arrayPair);

      kvTransactionManager
          .putPairs
          .get()
          .addAll(Arrays.stream(arrayPair).collect(Collectors.toList()));
      Assertions.assertThrows(Exception.class, kvTransactionManager::commit);

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
    Thread.sleep(1000);
    transactionIdGenerator.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {16})
  @Disabled("It's very time-consuming, so we disable it by default.")
  void testConcurrentRead(int threadNum) throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    transactionalKvBackend.begin();
    for (int i = 0; i < threadNum; i++) {
      transactionalKvBackend.put(
          ("key" + i).getBytes(StandardCharsets.UTF_8),
          ("value" + i).getBytes(StandardCharsets.UTF_8),
          true);
    }
    transactionalKvBackend.commit();
    transactionalKvBackend.closeTransaction();

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            threadNum,
            threadNum,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("testTransactionIdGenerator-%d")
                .build());

    ThreadLocalRandom random = ThreadLocalRandom.current();
    long current = System.currentTimeMillis();
    AtomicLong atomicLong = new AtomicLong(0);
    threadPoolExecutor.submit(
        () -> {
          while (System.currentTimeMillis() - current <= 2000) {
            transactionalKvBackend.begin();
            int i = random.nextInt(threadNum) + 1;
            byte[] binaryValue =
                Assertions.assertDoesNotThrow(
                    () -> transactionalKvBackend.get(("key" + i).getBytes(StandardCharsets.UTF_8)));
            Assertions.assertEquals(("value" + i).getBytes(StandardCharsets.UTF_8), binaryValue);
            transactionalKvBackend.closeTransaction();
            atomicLong.getAndIncrement();
          }
        });

    Thread.sleep(100);
    threadPoolExecutor.shutdown();
    threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
    LOGGER.info(String.format("%d thread qps is: %d/s", threadNum, atomicLong.get() / 2));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 16, 32})
  @Disabled("It's very time-consuming, so we disable it by default.")
  void testConcurrentWrite(int threadNum) throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            threadNum,
            threadNum,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("testTransactionIdGenerator-%d")
                .build());

    ThreadLocalRandom random = ThreadLocalRandom.current();
    long current = System.currentTimeMillis();
    AtomicLong atomicLong = new AtomicLong(0);
    threadPoolExecutor.submit(
        () -> {
          while (System.currentTimeMillis() - current <= 2000) {
            transactionalKvBackend.begin();
            int i = random.nextInt(threadNum) + 1;
            Assertions.assertDoesNotThrow(
                () ->
                    transactionalKvBackend.put(
                        ("key" + i).getBytes(StandardCharsets.UTF_8),
                        ("value" + i).getBytes(StandardCharsets.UTF_8),
                        true));
            transactionalKvBackend.closeTransaction();
            atomicLong.getAndIncrement();
          }
        });

    Thread.sleep(100);
    threadPoolExecutor.shutdown();
    threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
    LOGGER.info(String.format("%d thread write qps is: %d/s", threadNum, atomicLong.get() / 2));
  }

  @Test
  void testPrefixError() throws IOException, InterruptedException {
    Config config = getConfig();
    KvBackend kvBackend = getKvBackEnd(config);
    TransactionIdGenerator transactionIdGenerator =
        new TransactionIdGeneratorImpl(kvBackend, config);
    TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(kvBackend, transactionIdGenerator);

    transactionalKvBackend.begin();
    transactionalKvBackend.put(
        "abcefghi ".getBytes(StandardCharsets.UTF_8),
        "value1".getBytes(StandardCharsets.UTF_8),
        true);
    transactionalKvBackend.commit();

    // Start to read;
    transactionalKvBackend.begin();
    Assertions.assertEquals(
        "value1", new String(transactionalKvBackend.get("abcefghi ".getBytes())));
    Assertions.assertNull(transactionalKvBackend.get("abcefghi".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcefgh".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcefg".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcef".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abce".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abc".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("ab".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("a".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("_".getBytes(StandardCharsets.UTF_8)));

    transactionalKvBackend.begin();
    transactionalKvBackend.put(
        "abc".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8), true);
    transactionalKvBackend.commit();

    transactionalKvBackend.begin();
    Assertions.assertEquals("value1", new String(transactionalKvBackend.get("abc".getBytes())));
    Assertions.assertNull(transactionalKvBackend.get("ab".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("a".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abce".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcd".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcef".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(transactionalKvBackend.get("abcefg".getBytes(StandardCharsets.UTF_8)));
    transactionalKvBackend.commit();

    // Start to test scan
    // We randomly repeated insert some data and test the result
    int repeatedTimes = new Random().nextInt(10) + 1;
    for (int i = 0; i < repeatedTimes; i++) {
      transactionalKvBackend.begin();
      transactionalKvBackend.put(
          "m".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "mb".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "mbc".getBytes(StandardCharsets.UTF_8), "value3".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "mbcd".getBytes(StandardCharsets.UTF_8), "value4".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "mbcde".getBytes(StandardCharsets.UTF_8),
          "value5".getBytes(StandardCharsets.UTF_8),
          true);
      transactionalKvBackend.put(
          "mbcdf".getBytes(StandardCharsets.UTF_8),
          "value6".getBytes(StandardCharsets.UTF_8),
          true);
      transactionalKvBackend.put(
          "mbcdfg".getBytes(StandardCharsets.UTF_8),
          "value7".getBytes(StandardCharsets.UTF_8),
          true);
      transactionalKvBackend.commit();
      Thread.sleep(1);
    }

    transactionalKvBackend.begin();

    // start is 'mb' and end is 'mc'
    List<Pair<byte[], byte[]>> data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(6, data.size());

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("mbc", new String(data.get(0).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(6, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(6, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));

    // Start is 'mb' and end is 'mbcde'
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbcde".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(2, data.size());
    Assertions.assertEquals("mbcd", new String(data.get(1).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbcde".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(3, data.size());
    Assertions.assertEquals("mbcde", new String(data.get(2).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbcde".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());

    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("mbcde", new String(data.get(3).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbcde".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());

    Assertions.assertEquals(3, data.size());
    Assertions.assertEquals("mbcd", new String(data.get(2).getKey()));

    // Start is 'mb' and end is 'mbc'
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(1, data.size());
    Assertions.assertEquals("mbc", new String(data.get(0).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(0, data.size());

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(1, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mbc".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(2, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));
    Assertions.assertEquals("mbc", new String(data.get(1).getKey()));

    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("mb".getBytes(StandardCharsets.UTF_8))
                .end("mb".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());

    Assertions.assertEquals(1, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));

    transactionalKvBackend.scan(
        KvRange.builder()
            .start("mb".getBytes(StandardCharsets.UTF_8))
            .end("mc".getBytes(StandardCharsets.UTF_8))
            .startInclusive(true)
            .endInclusive(true)
            .build());

    Assertions.assertEquals(1, data.size());
    Assertions.assertEquals("mb", new String(data.get(0).getKey()));

    repeatedTimes = new Random().nextInt(10) + 1;
    for (int i = 0; i < repeatedTimes; i++) {
      transactionalKvBackend.begin();
      transactionalKvBackend.put(
          "bc".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "bcd".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "bcde".getBytes(StandardCharsets.UTF_8), "value3".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "bce".getBytes(StandardCharsets.UTF_8), "value4".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "bcef".getBytes(StandardCharsets.UTF_8), "value5".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "bef".getBytes(StandardCharsets.UTF_8), "value6".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.put(
          "beg".getBytes(StandardCharsets.UTF_8), "value7".getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.commit();
      Thread.sleep(1);
    }

    transactionalKvBackend.begin();

    // Start is "bc" and end is "bcef"
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bcef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(3, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bce", new String(data.get(2).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bcef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(3).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bcef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(4).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bcef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bce", new String(data.get(3).getKey()));

    // Start is "bc" and end is "bef"
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(3).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bef", new String(data.get(4).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(6, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bef", new String(data.get(5).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(4).getKey()));

    // Start is "bc" and end is "be"
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("be".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(3).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("be".getBytes(StandardCharsets.UTF_8))
                .startInclusive(false)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(4, data.size());
    Assertions.assertEquals("bcd", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(3).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("be".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(true)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(4).getKey()));
    data =
        transactionalKvBackend.scan(
            KvRange.builder()
                .start("bc".getBytes(StandardCharsets.UTF_8))
                .end("bef".getBytes(StandardCharsets.UTF_8))
                .startInclusive(true)
                .endInclusive(false)
                .build());
    Assertions.assertEquals(5, data.size());
    Assertions.assertEquals("bc", new String(data.get(0).getKey()));
    Assertions.assertEquals("bcef", new String(data.get(4).getKey()));
  }

  @Test
  void testRevertByte() {
    byte[] bytes = {0x01, 0x02, 0x03, 0x04};
    byte[] copy = ArrayUtils.clone(bytes);
    byte[] revertBytes = TransactionalKvBackendImpl.revertByteArray(bytes);
    Assertions.assertArrayEquals(copy, bytes);
    revertBytes = TransactionalKvBackendImpl.revertByteArray(revertBytes);
    Assertions.assertArrayEquals(bytes, revertBytes);
  }
}
