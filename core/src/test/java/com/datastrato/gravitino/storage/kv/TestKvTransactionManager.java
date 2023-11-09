/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvTransactionManager {

  private KvBackend getKvBackEnd() throws IOException {
    Config config = Mockito.mock(Config.class);
    File file = Files.createTempDir();
    file.deleteOnExit();
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    KvBackend kvBackend = new RocksDBKvBackend();
    kvBackend.initialize(config);
    return kvBackend;
  }

  @Test
  void testGet() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    KvTransactionManager kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.put("key1".getBytes(), "value1".getBytes(), true);
    kvTransactionManager.put("key2".getBytes(), "value2".getBytes(), true);
    kvTransactionManager.commit();
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    Assertions.assertEquals("value1", new String(kvTransactionManager.get("key1".getBytes())));
    Assertions.assertEquals("value2", new String(kvTransactionManager.get("key2".getBytes())));
  }

  @Test
  void testDelete() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    KvTransactionManager kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.put("key1".getBytes(), "value1".getBytes(), true);
    kvTransactionManager.put("key2".getBytes(), "value2".getBytes(), true);
    kvTransactionManager.commit();
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.delete("key1".getBytes());
    kvTransactionManager.commit();
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    Assertions.assertNull(kvTransactionManager.get("key1".getBytes()));
    Assertions.assertNotNull(kvTransactionManager.get("key2".getBytes()));
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.delete("key2".getBytes());
    kvTransactionManager.commit();
    kvTransactionManager.close();

    Assertions.assertNull(kvTransactionManager.get("key1".getBytes()));
    Assertions.assertNull(kvTransactionManager.get("key2".getBytes()));
  }

  @Test
  void testScan() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    KvTransactionManager kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.put("key1".getBytes(), "value1".getBytes(), true);
    kvTransactionManager.put("key2".getBytes(), "value2".getBytes(), true);
    kvTransactionManager.put("key3".getBytes(), "value3".getBytes(), true);
    kvTransactionManager.commit();
    kvTransactionManager.close();

    Map<String, String> map =
        new HashMap<String, String>() {
          {
            put("key1", "value1");
            put("key2", "value2");
            put("key3", "value3");
          }
        };

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    List<Pair<byte[], byte[]>> pairs =
        kvTransactionManager.scan(
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
        kvTransactionManager.scan(
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
        kvTransactionManager.scan(
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
        kvTransactionManager.scan(
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
        kvTransactionManager.scan(
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
    KvBackend kvBackend = getKvBackEnd();
    KvTransactionManager kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.put("key1".getBytes(), "value1".getBytes(), true);
    kvTransactionManager.put("key2".getBytes(), "value2".getBytes(), true);
    kvTransactionManager.put("key3".getBytes(), "value3".getBytes(), true);
    kvTransactionManager.commit();
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    kvTransactionManager.deleteRange(
        new KvRangeScan.KvRangeScanBuilder()
            .start("key1".getBytes())
            .end("key3".getBytes())
            .startInclusive(true)
            .endInclusive(true)
            .build());
    kvTransactionManager.commit();
    kvTransactionManager.close();

    kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
    Assertions.assertNull(kvTransactionManager.get("key1".getBytes()));
    Assertions.assertNull(kvTransactionManager.get("key2".getBytes()));
    Assertions.assertNull(kvTransactionManager.get("key3".getBytes()));
  }

  @Test
  void testException() throws IOException, InterruptedException {
    KvBackend kvBackend = getKvBackEnd();
    KvTransactionManagerImpl kvTransactionManager = new KvTransactionManagerImpl(kvBackend);
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
            Pair.of(kvTransactionManager.constructKey("key9".getBytes()), null));

    Pair<byte[], byte[]>[] arrayPair = pairs.toArray(new Pair[0]);

    for (int i = 0; i < 1000; i++) {
      ArrayUtils.shuffle(arrayPair);
      kvTransactionManager.putPairs.addAll(Arrays.stream(arrayPair).collect(Collectors.toList()));

      Assertions.assertThrows(Exception.class, kvTransactionManager::commit);

      // 2 milliseconds is enough for the data to be visible to other threads
      Thread.currentThread().sleep(2);
      kvTransactionManager = new KvTransactionManagerImpl(kvBackend);

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
}
