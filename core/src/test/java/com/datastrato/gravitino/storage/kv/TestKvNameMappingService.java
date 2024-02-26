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

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.NameMappingService;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKvNameMappingService {
  private Config getConfig() throws IOException {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(file.getAbsolutePath());
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3000L);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    return config;
  }

  private KvEntityStore getKvEntityStore(Config config) {
    KvEntityStore kvEntityStore = (KvEntityStore) EntityStoreFactory.createEntityStore(config);
    kvEntityStore.initialize(config);
    kvEntityStore.setSerDe(EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
    return kvEntityStore;
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
  public void testGetIdByName() throws Exception {
    try (KvEntityStore kvEntityStore = getKvEntityStore(getConfig())) {
      NameMappingService nameMappingService = kvEntityStore.nameMappingService;
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
  public void testUpdateName() throws Exception {
    try (KvEntityStore kvEntityStore = getKvEntityStore(getConfig())) {
      NameMappingService nameMappingService = kvEntityStore.nameMappingService;
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
  void testUpdateNameWithExistingName() throws Exception {
    try (KvEntityStore kvEntityStore = getKvEntityStore(getConfig())) {
      NameMappingService nameMappingService = kvEntityStore.nameMappingService;
      IdGenerator idGenerator = getIdGeneratorByReflection(nameMappingService);
      Mockito.doReturn(1L).when(idGenerator).nextId();
      long name1IdRead = nameMappingService.getOrCreateIdFromName("name1");
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

      Mockito.doReturn(2L).when(idGenerator).nextId();
      long name2IdRead = nameMappingService.getOrCreateIdFromName("name2");
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));
      Assertions.assertNotEquals(name1IdRead, name2IdRead);

      // Update name1 to an existing name like name2.
      boolean result = nameMappingService.updateName("name1", "name2");
      Assertions.assertTrue(result);

      Long name2Id = nameMappingService.getIdByName("name2");
      Assertions.assertEquals(1L, name2Id);

      Assertions.assertNull(nameMappingService.getIdByName("name1"));
    }
  }

  @Test
  public void testBindAndUnBind() throws Exception {
    try (KvEntityStore kvEntityStore = getKvEntityStore(getConfig())) {
      KvNameMappingService nameMappingService =
          (KvNameMappingService) kvEntityStore.nameMappingService;
      IdGenerator idGenerator = getIdGeneratorByReflection(nameMappingService);

      Mockito.doReturn(1L).when(idGenerator).nextId();
      nameMappingService.getOrCreateIdFromName("name1");
      Assertions.assertNotNull(nameMappingService.getIdByName("name1"));

      boolean result = nameMappingService.unbindNameAndId("name1");
      Assertions.assertTrue(result);
      Assertions.assertNull(nameMappingService.getIdByName("name1"));

      Mockito.doReturn(2L).when(idGenerator).nextId();
      nameMappingService.getOrCreateIdFromName("name2");

      TransactionalKvBackend spyKvBackend = Mockito.spy(nameMappingService.transactionalKvBackend);
      // All deletes && puts will be converted to put operations.
      Mockito.doThrow(new ArithmeticException())
          .when(spyKvBackend)
          .put(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
      Config config = Mockito.mock(Config.class);
      Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(3000L);
      final NameMappingService mock =
          new KvNameMappingService(spyKvBackend, nameMappingService.lock);

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
}
