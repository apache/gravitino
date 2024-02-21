/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class TestRocksDBKvBackend {

  private KvBackend getKvBackEnd() throws IOException {
    Config config = Mockito.mock(Config.class);

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());

    KvBackend kvBackend = new RocksDBKvBackend();
    kvBackend.initialize(config);
    return kvBackend;
  }

  @Test
  void testStoragePath() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn("/a/b");
    RocksDBKvBackend kvBackend = new RocksDBKvBackend();
    String path = kvBackend.getStoragePath(config);
    Assertions.assertEquals("/a/b", path);

    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn("");
    kvBackend = new RocksDBKvBackend();
    path = kvBackend.getStoragePath(config);
    // We haven't set the GRAVITINO_HOME
    Assertions.assertEquals("null/data/rocksdb", path);
  }

  @Test
  void testPutAndGet() throws IOException, RocksDBException {
    KvBackend kvBackend = getKvBackEnd();
    kvBackend.put(
        "testKey".getBytes(StandardCharsets.UTF_8),
        "testValue".getBytes(StandardCharsets.UTF_8),
        false);
    byte[] bytes = kvBackend.get("testKey".getBytes(StandardCharsets.UTF_8));

    Assertions.assertNotNull(bytes);
    Assertions.assertEquals("testValue", new String(bytes, StandardCharsets.UTF_8));

    Assertions.assertThrowsExactly(
        AlreadyExistsException.class,
        () ->
            kvBackend.put(
                "testKey".getBytes(StandardCharsets.UTF_8),
                "testValue2".getBytes(StandardCharsets.UTF_8),
                false));
    kvBackend.put(
        "testKey".getBytes(StandardCharsets.UTF_8),
        "testValue2".getBytes(StandardCharsets.UTF_8),
        true);

    RocksDBKvBackend rocksDBKvBackend = (RocksDBKvBackend) kvBackend;
    RocksDBKvBackend spyRocksDBKvBackend = Mockito.spy(rocksDBKvBackend);
    Mockito.doThrow(new RocksDBException("Mock: Store file not found"))
        .when(spyRocksDBKvBackend)
        .handlePut(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

    Exception exception =
        Assertions.assertThrowsExactly(
            IOException.class,
            () ->
                spyRocksDBKvBackend.put(
                    "testKey".getBytes(StandardCharsets.UTF_8),
                    "testValue2".getBytes(StandardCharsets.UTF_8),
                    true));
    Assertions.assertTrue(exception.getMessage().contains("Mock: Store file not found"));
  }

  @Test
  void testDelete() throws IOException, RocksDBException {
    KvBackend kvBackend = getKvBackEnd();
    Assertions.assertDoesNotThrow(
        () -> kvBackend.delete("testKey".getBytes(StandardCharsets.UTF_8)));
    kvBackend.put(
        "testKey".getBytes(StandardCharsets.UTF_8),
        "testValue".getBytes(StandardCharsets.UTF_8),
        false);
    Assertions.assertDoesNotThrow(
        () -> kvBackend.delete("testKey".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertDoesNotThrow(
        () -> kvBackend.delete("testKey".getBytes(StandardCharsets.UTF_8)));

    RocksDBKvBackend rocksDBKvBackend = (RocksDBKvBackend) kvBackend;

    RocksDB db = rocksDBKvBackend.getDb();
    RocksDB spyDb = Mockito.spy(db);
    Mockito.doThrow(new RocksDBException("Mock: Network is unstable"))
        .when(spyDb)
        .delete(Mockito.any());
    rocksDBKvBackend.setDb(spyDb);

    Exception e =
        Assertions.assertThrowsExactly(
            IOException.class, () -> kvBackend.delete("testKey".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertTrue(e.getMessage().contains("Mock: Network is unstable"));
  }

  @Test
  void testDeleteRange() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    kvBackend.put(
        "abc".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abd".getBytes(StandardCharsets.UTF_8), "abd".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abffff".getBytes(StandardCharsets.UTF_8),
        "abffff".getBytes(StandardCharsets.UTF_8),
        false);
    kvBackend.put(
        "abeee".getBytes(StandardCharsets.UTF_8), "abeee".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acc".getBytes(StandardCharsets.UTF_8), "acc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acca".getBytes(StandardCharsets.UTF_8), "acca".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "accb".getBytes(StandardCharsets.UTF_8), "accb".getBytes(StandardCharsets.UTF_8), false);

    // More test case please refer to TestTransactionalKvBackend
    KvRange kvRange =
        new KvRange.KvRangeBuilder()
            .start("ab".getBytes(StandardCharsets.UTF_8))
            .end("ac".getBytes(StandardCharsets.UTF_8))
            .startInclusive(false)
            .endInclusive(false)
            .build();

    Assertions.assertDoesNotThrow(() -> kvBackend.deleteRange(kvRange));
    Assertions.assertNull(kvBackend.get("abc".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(kvBackend.get("abd".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(kvBackend.get("abffff".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNull(kvBackend.get("abeee".getBytes(StandardCharsets.UTF_8)));

    Assertions.assertNotNull(kvBackend.get("acc".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNotNull(kvBackend.get("acca".getBytes(StandardCharsets.UTF_8)));
    Assertions.assertNotNull(kvBackend.get("accb".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testScanWithBrokenRocksDB() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    kvBackend.put(
        "abc".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abd".getBytes(StandardCharsets.UTF_8), "abd".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abffff".getBytes(StandardCharsets.UTF_8),
        "abffff".getBytes(StandardCharsets.UTF_8),
        false);
    kvBackend.put(
        "abeee".getBytes(StandardCharsets.UTF_8), "abeee".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acc".getBytes(StandardCharsets.UTF_8), "acc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acca".getBytes(StandardCharsets.UTF_8), "acca".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "accb".getBytes(StandardCharsets.UTF_8), "accb".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "accg".getBytes(StandardCharsets.UTF_8), "accg".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acf".getBytes(StandardCharsets.UTF_8), "acf".getBytes(StandardCharsets.UTF_8), false);

    // More test case please refer to TestTransactionalKvBackend
    KvRange kvRange =
        new KvRange.KvRangeBuilder()
            .start("ab".getBytes(StandardCharsets.UTF_8))
            .end("ac".getBytes(StandardCharsets.UTF_8))
            .startInclusive(false)
            .endInclusive(false)
            .build();

    List<Pair<byte[], byte[]>> data = kvBackend.scan(kvRange);
    Assertions.assertEquals(4, data.size());

    RocksDBKvBackend rocksDBKvBackend = (RocksDBKvBackend) kvBackend;
    RocksDB db = rocksDBKvBackend.getDb();
    RocksDB spyDb = Mockito.spy(db);

    Mockito.when(spyDb.newIterator()).thenThrow(new RuntimeException("Mock: RocksDB is broken"));
    rocksDBKvBackend.setDb(spyDb);

    Exception e =
        Assertions.assertThrowsExactly(RuntimeException.class, () -> kvBackend.scan(kvRange));
    Assertions.assertTrue(e.getMessage().contains("Mock: RocksDB is broken"));
  }

  @Test
  void testScan() throws IOException {
    KvBackend kvBackend = getKvBackEnd();
    kvBackend.put(
        "abc".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abd".getBytes(StandardCharsets.UTF_8), "abd".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "abffff".getBytes(StandardCharsets.UTF_8),
        "abffff".getBytes(StandardCharsets.UTF_8),
        false);
    kvBackend.put(
        "abeee".getBytes(StandardCharsets.UTF_8), "abeee".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acc".getBytes(StandardCharsets.UTF_8), "acc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acca".getBytes(StandardCharsets.UTF_8), "acca".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "accb".getBytes(StandardCharsets.UTF_8), "accb".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "accg".getBytes(StandardCharsets.UTF_8), "accg".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "acf".getBytes(StandardCharsets.UTF_8), "acf".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "adc".getBytes(StandardCharsets.UTF_8), "adc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "add".getBytes(StandardCharsets.UTF_8), "add".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "ae".getBytes(StandardCharsets.UTF_8), "ae".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "aef".getBytes(StandardCharsets.UTF_8), "aef".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "ag".getBytes(StandardCharsets.UTF_8), "ag".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "b".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "ba".getBytes(StandardCharsets.UTF_8), "ba".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "bc".getBytes(StandardCharsets.UTF_8), "bc".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "c".getBytes(StandardCharsets.UTF_8), "f".getBytes(StandardCharsets.UTF_8), false);
    kvBackend.put(
        "f".getBytes(StandardCharsets.UTF_8), "f".getBytes(StandardCharsets.UTF_8), false);

    KvRange kvRange =
        new KvRange.KvRangeBuilder()
            .start("ab".getBytes(StandardCharsets.UTF_8))
            .end("ac".getBytes(StandardCharsets.UTF_8))
            .startInclusive(false)
            .endInclusive(false)
            .build();

    List<Pair<byte[], byte[]>> data = kvBackend.scan(kvRange);
    Assertions.assertEquals(4, data.size());

    RocksDBKvBackend rocksDBKvBackend = (RocksDBKvBackend) kvBackend;
    RocksDB db = rocksDBKvBackend.getDb();
    RocksDB spyDb = Mockito.spy(db);

    Mockito.when(spyDb.newIterator()).thenCallRealMethod();
    Assertions.assertDoesNotThrow(() -> kvBackend.scan(kvRange));
  }
}
