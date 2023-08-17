/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import static com.datastrato.graviton.Configs.DEFUALT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.storage.kv.KvBackend;
import com.datastrato.graviton.storage.kv.RocksDBKvBackend;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvNameMappingService {

  public static final String ROCKS_DB_STORE_PATH = "/tmp/graviton_name_mapping";

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
  public void testNameMappingService() throws IOException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);
    IdGenerator idGenerator = IdGeneratorFactory.getIdGeneratorByName(config);

    KvBackend backend = new RocksDBKvBackend();
    backend.initialize(config);
    NameMappingService nameMappingService = new KvNameMappingService(backend);

    // First test read and write
    Assertions.assertNull(nameMappingService.getIdByName("name1"));
    long name1Id = idGenerator.nextId();
    nameMappingService.addBinding("name1", name1Id);
    Long name1IdRead = nameMappingService.getIdByName("name1");
    Assertions.assertEquals(name1Id, name1IdRead);

    Assertions.assertNull(nameMappingService.getIdByName("name2"));
    long name2Id = idGenerator.nextId();
    nameMappingService.addBinding("name2", name2Id);
    Long name2IdRead = nameMappingService.getIdByName("name2");
    Assertions.assertEquals(name2Id, name2IdRead);

    Assertions.assertNotEquals(name1Id, name2Id);

    // Test update
    boolean result = nameMappingService.update("name1", "name3");
    Assertions.assertTrue(result);
    Long name3Id = nameMappingService.getIdByName("name3");
    Assertions.assertEquals(name1Id, name3Id);
    Assertions.assertNull(nameMappingService.getIdByName("name1"));

    result = nameMappingService.update("name1", "name1_");
    Assertions.assertFalse(result);

    // Test or create
    Assertions.assertNull(nameMappingService.getIdByName("name4"));
    long name4Id = idGenerator.nextId();
    nameMappingService.addBinding("name4", name4Id);
    Assertions.assertNotEquals(name4Id, name1Id);

    // Test delete
    nameMappingService.removeBinding("name4");
    Assertions.assertNull(nameMappingService.getIdByName("name4"));

    KvBackend spyKvBackend = Mockito.spy(backend);
    Mockito.doThrow(new ArithmeticException()).when(spyKvBackend).delete(Mockito.any());
    KvNameMappingService mockNameMappingService = new KvNameMappingService(spyKvBackend);

    // Now we try to use update. It should fail.
    Assertions.assertThrowsExactly(
        ArithmeticException.class, () -> mockNameMappingService.update("name3", "name4"));
    Assertions.assertNull(mockNameMappingService.getIdByName("name4"));
    Assertions.assertNotNull(mockNameMappingService.getIdByName("name3"));
  }
}
