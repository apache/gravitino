/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.DEFUALT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.storage.IdGenerator;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.storage.RandomIdGenerator;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.ClassOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.mockito.Mockito;

@TestClassOrder(OrderAnnotation.class)
public class TestKvNameMappingService {

  public static final String ROCKS_DB_STORE_PATH = "/tmp/graviton_name_mapping";
  private IdGenerator idGenerator = new RandomIdGenerator();

  @BeforeEach
  public void cleanEnv() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  private KvNameMappingService createNameMappingService(String kvPath) throws IOException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(kvPath);

    KvBackend backend = new RocksDBKvBackend();
    backend.initialize(config);
    return new KvNameMappingService(backend);
  }

  @Test
  @Order(1)
  public void testGetIdByName() throws Exception {
    IdGenerator idGenerator = new RandomIdGenerator();
    try (NameMappingService nameMappingService =
        createNameMappingService(ROCKS_DB_STORE_PATH + "/1")) {
      Assertions.assertNull(nameMappingService.getIdByName("name1"));
      long name1Id = idGenerator.nextId();
      nameMappingService.bindNameAndId("name1", name1Id);
      Long name1IdRead = nameMappingService.getIdByName("name1");
      Assertions.assertEquals(name1Id, name1IdRead);

      Assertions.assertNull(nameMappingService.getIdByName("name2"));
      long name2Id = idGenerator.nextId();
      nameMappingService.bindNameAndId("name2", name2Id);
      Long name2IdRead = nameMappingService.getIdByName("name2");
      Assertions.assertEquals(name2Id, name2IdRead);
    }
  }

  @Test
  @Order(2)
  public void testUpdateName() throws Exception {
    long name1Id = idGenerator.nextId();
    try (NameMappingService nameMappingService =
        createNameMappingService(ROCKS_DB_STORE_PATH + "/2")) {
      nameMappingService.bindNameAndId("name1", name1Id);
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

      long name2Id = idGenerator.nextId();
      nameMappingService.bindNameAndId("name2", name2Id);
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));
      Assertions.assertNotEquals(name1Id, name2Id);

      boolean result = nameMappingService.updateName("name1", "name3");
      Assertions.assertTrue(result);

      Long name3Id = nameMappingService.getIdByName("name3");
      Assertions.assertEquals(name1Id, name3Id);
      Assertions.assertNull(nameMappingService.getIdByName("name1"));

      Assertions.assertFalse(nameMappingService.updateName("name1", "name4"));
    }
  }

  @Test
  @Order(3)
  public void testBindAndUnBind() throws IOException {
    long name1Id = idGenerator.nextId();
    NameMappingService nameMappingService = createNameMappingService(ROCKS_DB_STORE_PATH + "/3");
    nameMappingService.bindNameAndId("name1", name1Id);
    Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

    boolean result = nameMappingService.unbindNameAndId("name1");
    Assertions.assertTrue(result);
    Assertions.assertNull(nameMappingService.getIdByName("name1"));

    long name2Id = idGenerator.nextId();
    nameMappingService.bindNameAndId("name2", name2Id);

    KvBackend spyKvBackend = Mockito.spy(((KvNameMappingService) nameMappingService).backend);
    Mockito.doThrow(new ArithmeticException()).when(spyKvBackend).delete(Mockito.any());
    final NameMappingService mock = new KvNameMappingService(spyKvBackend);

    // Now we try to use update. It should fail.
    Assertions.assertThrowsExactly(
        ArithmeticException.class, () -> mock.updateName("name2", "name3"));
    Assertions.assertNull(mock.getIdByName("name3"));
    Assertions.assertNotNull(mock.getIdByName("name2"));
  }
}
