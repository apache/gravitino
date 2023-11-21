/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import java.io.IOException;
import java.lang.reflect.Field;
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

  public static final String ROCKS_DB_STORE_PATH = "/tmp/gravitino_name_mapping";
  private final IdGenerator idGenerator = new RandomIdGenerator();

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
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(kvPath);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3L);

    KvBackend backend = new RocksDBKvBackend();
    backend.initialize(config);
    return new KvNameMappingService(backend, new TransactionIdGeneratorImpl(backend, config));
  }

  private IdGenerator getIdGeneratorByReflection(NameMappingService nameMappingService)
      throws Exception {
    Field field = nameMappingService.getClass().getDeclaredField("idGenerator");
    field.setAccessible(true);
    IdGenerator idGenerator = (IdGenerator) field.get(nameMappingService);
    IdGenerator spyIdGenerator = Mockito.spy(idGenerator);
    field.set(nameMappingService, spyIdGenerator);
    return spyIdGenerator;
  }

  @Test
  @Order(1)
  public void testGetIdByName() throws Exception {
    try (NameMappingService nameMappingService =
        createNameMappingService(ROCKS_DB_STORE_PATH + "/1")) {
      Assertions.assertNull(nameMappingService.getIdByName("name1"));

      IdGenerator spyIdGenerator = getIdGeneratorByReflection(nameMappingService);
      Mockito.doReturn(1L).when(spyIdGenerator).nextId();

      long name1Id = nameMappingService.getOrCreateIdFromName("name1");
      Long name1IdRead = nameMappingService.getIdByName("name1");
      Assertions.assertEquals(name1Id, name1IdRead);

      Assertions.assertNull(nameMappingService.getIdByName("name2"));
      Mockito.doReturn(2L).when(spyIdGenerator).nextId();
      long name2Id = nameMappingService.getOrCreateIdFromName("name2");
      Long name2IdRead = nameMappingService.getIdByName("name2");
      Assertions.assertEquals(name2Id, name2IdRead);
    }
  }

  @Test
  @Order(2)
  public void testUpdateName() throws Exception {
    try (NameMappingService nameMappingService =
        createNameMappingService(ROCKS_DB_STORE_PATH + "/2")) {
      IdGenerator idGenerator = getIdGeneratorByReflection(nameMappingService);
      Mockito.doReturn(1L).when(idGenerator).nextId();
      long name1IdRead = nameMappingService.getOrCreateIdFromName("name1");
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

      Mockito.doReturn(2L).when(idGenerator).nextId();
      long name2IdRead = nameMappingService.getOrCreateIdFromName("name2");
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));
      Assertions.assertNotEquals(name1IdRead, name2IdRead);

      boolean result = nameMappingService.updateName("name1", "name3");
      Assertions.assertTrue(result);

      Long name3Id = nameMappingService.getIdByName("name3");
      Assertions.assertEquals(name1IdRead, name3Id);
      Assertions.assertNull(nameMappingService.getIdByName("name1"));

      Assertions.assertFalse(nameMappingService.updateName("name1", "name4"));
    }
  }

  @Test
  @Order(3)
  public void testBindAndUnBind() throws Exception {
    idGenerator.nextId();
    NameMappingService nameMappingService = createNameMappingService(ROCKS_DB_STORE_PATH + "/3");
    IdGenerator idGenerator = getIdGeneratorByReflection(nameMappingService);

    Mockito.doReturn(1L).when(idGenerator).nextId();
    nameMappingService.getOrCreateIdFromName("name1");
    Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

    boolean result = nameMappingService.unbindNameAndId("name1");
    Assertions.assertTrue(result);
    Assertions.assertNull(nameMappingService.getIdByName("name1"));

    Mockito.doReturn(2L).when(idGenerator).nextId();
    nameMappingService.getOrCreateIdFromName("name2");

    KvBackend spyKvBackend = Mockito.spy(((KvNameMappingService) nameMappingService).backend);
    // All deletes && puts will be converted to put operations.
    Mockito.doThrow(new ArithmeticException())
        .when(spyKvBackend)
        .put(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3L);
    final NameMappingService mock =
        new KvNameMappingService(
            spyKvBackend, new TransactionIdGeneratorImpl(spyKvBackend, config));

    // Now we try to use update. It should fail.
    Assertions.assertThrowsExactly(
        ArithmeticException.class, () -> mock.updateName("name2", "name3"));
    Mockito.doCallRealMethod()
        .when(spyKvBackend)
        .put(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Assertions.assertNull(mock.getIdByName("name3"));
    Assertions.assertNotNull(mock.getIdByName("name2"));
  }
}
