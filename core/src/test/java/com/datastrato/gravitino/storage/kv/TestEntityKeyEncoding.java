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
import static com.datastrato.gravitino.storage.kv.BinaryEntityKeyEncoder.BYTABLE_NAMESPACE_SEPARATOR;
import static com.datastrato.gravitino.storage.kv.BinaryEntityKeyEncoder.WILD_CARD;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestEntityKeyEncoding {
  private Config getConfig() throws IOException {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = java.nio.file.Files.createTempDirectory(baseDir.toPath(), "test").toFile();
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

  private IdGenerator getIdGeneratorAndSpy(BinaryEntityKeyEncoder entityKeyEncoder)
      throws IllegalAccessException, NoSuchFieldException {
    KvNameMappingService nameMappingService =
        (KvNameMappingService) entityKeyEncoder.nameMappingService;

    Field field = nameMappingService.getClass().getDeclaredField("idGenerator");
    field.setAccessible(true);
    IdGenerator idGenerator = (IdGenerator) field.get(nameMappingService);
    IdGenerator spyIdGenerator = Mockito.spy(idGenerator);
    field.set(nameMappingService, spyIdGenerator);
    return spyIdGenerator;
  }

  @Test
  public void testIdentifierEncoding()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    Config config = getConfig();
    try (KvEntityStore kvEntityStore = getKvEntityStore(config)) {
      BinaryEntityKeyEncoder encoder = (BinaryEntityKeyEncoder) kvEntityStore.entityKeyEncoder;

      // Metalake
      // metalake1 --> 0
      Namespace namespace = Namespace.of();
      IdGenerator mockIdGenerator = getIdGeneratorAndSpy(encoder);

      Mockito.doReturn(0L).when(mockIdGenerator).nextId();
      NameIdentifier mateLakeIdentifier1 = NameIdentifier.of(namespace, "metalake1");
      byte[] realKey = encoder.encode(mateLakeIdentifier1, EntityType.METALAKE);
      byte[] expectKey =
          Bytes.concat(
              EntityType.METALAKE.getShortName().getBytes(StandardCharsets.UTF_8),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L));
      Assertions.assertArrayEquals(expectKey, realKey);

      // name ---> id
      // catalog1 --> 1
      // catalog2 --> 2
      // catalog3 --> 3
      Namespace catalogNamespace = Namespace.of("metalake1");
      NameIdentifier catalogIdentifier1 = NameIdentifier.of(catalogNamespace, "catalog1");
      NameIdentifier catalogIdentifier2 = NameIdentifier.of(catalogNamespace, "catalog2");
      NameIdentifier catalogIdentifier3 = NameIdentifier.of(catalogNamespace, "catalog3");
      NameIdentifier[] catalogIdentifiers =
          new NameIdentifier[] {catalogIdentifier1, catalogIdentifier2, catalogIdentifier3};

      for (int i = 0; i < catalogIdentifiers.length; i++) {
        Mockito.doReturn(1L + i).when(mockIdGenerator).nextId();
        NameIdentifier identifier = catalogIdentifiers[i];
        realKey = encoder.encode(identifier, EntityType.CATALOG);
        expectKey =
            Bytes.concat(
                EntityType.CATALOG.getShortName().getBytes(StandardCharsets.UTF_8),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(0L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(1L + i));
        Assertions.assertArrayEquals(expectKey, realKey);
      }

      // name ---> id
      // schema1 --> 4
      // schema2 --> 5
      // schema3 --> 6
      Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
      NameIdentifier schemaIdentifier1 = NameIdentifier.of(schemaNameSpace, "schema1");
      NameIdentifier schemaIdentifier2 = NameIdentifier.of(schemaNameSpace, "schema2");
      NameIdentifier schemaIdentifier3 = NameIdentifier.of(schemaNameSpace, "schema3");
      NameIdentifier[] schemaIdentifiers =
          new NameIdentifier[] {schemaIdentifier1, schemaIdentifier2, schemaIdentifier3};

      for (int i = 0; i < schemaIdentifiers.length; i++) {
        NameIdentifier identifier = schemaIdentifiers[i];
        Mockito.doReturn(4L + i).when(mockIdGenerator).nextId();
        realKey = encoder.encode(identifier, EntityType.SCHEMA);
        expectKey =
            Bytes.concat(
                EntityType.SCHEMA.getShortName().getBytes(StandardCharsets.UTF_8),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(0L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(2L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(4L + i));
        Assertions.assertArrayEquals(expectKey, realKey);
      }

      // name ---> id
      // table1 --> 7
      // table2 --> 8
      // table3 --> 9
      Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
      NameIdentifier tableIdentifier1 = NameIdentifier.of(tableNameSpace, "table1");
      NameIdentifier tableIdentifier2 = NameIdentifier.of(tableNameSpace, "table2");
      NameIdentifier tableIdentifier3 = NameIdentifier.of(tableNameSpace, "table3");
      NameIdentifier[] tableIdentifiers =
          new NameIdentifier[] {tableIdentifier1, tableIdentifier2, tableIdentifier3};

      for (int i = 0; i < tableIdentifiers.length; i++) {
        NameIdentifier identifier = tableIdentifiers[i];
        Mockito.doReturn(7L + i).when(mockIdGenerator).nextId();
        realKey = encoder.encode(identifier, EntityType.TABLE);
        expectKey =
            Bytes.concat(
                EntityType.TABLE.getShortName().getBytes(StandardCharsets.UTF_8),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(0L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(2L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(6L),
                BYTABLE_NAMESPACE_SEPARATOR,
                ByteUtils.longToByte(i + 7L));
        Assertions.assertArrayEquals(expectKey, realKey);
      }

      // Unsupported operation
      Mockito.doReturn(10L).when(mockIdGenerator).nextId();
      Assertions.assertThrows(
          UnsupportedOperationException.class,
          () -> {
            encoder.encode(
                NameIdentifier.of(
                    Namespace.of("metalake1", "catalog2", "schema3", "table1"), "column1"),
                EntityType.COLUMN);
          });
    }
  }

  @Test
  public void testNamespaceEncoding()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    Config config = getConfig();
    try (KvEntityStore kvEntityStore = getKvEntityStore(config)) {
      BinaryEntityKeyEncoder encoder = (BinaryEntityKeyEncoder) kvEntityStore.entityKeyEncoder;
      // Scan all Metalake
      Namespace namespace = Namespace.of();
      IdGenerator mockIdGenerator = getIdGeneratorAndSpy(encoder);

      NameIdentifier metalakeIdentifier = NameIdentifier.of(namespace, WILD_CARD);
      byte[] realKey = encoder.encode(metalakeIdentifier, EntityType.METALAKE);
      byte[] expectKey =
          Bytes.concat(
              EntityType.METALAKE.getShortName().getBytes(StandardCharsets.UTF_8),
              BYTABLE_NAMESPACE_SEPARATOR);
      Assertions.assertArrayEquals(expectKey, realKey);

      // Scan all catalog in metalake1
      // metalake1 --> 0L
      Mockito.doReturn(0L).when(mockIdGenerator).nextId();
      Namespace catalogNamespace = Namespace.of("metalake1");
      NameIdentifier catalogIdentifier = NameIdentifier.of(catalogNamespace, WILD_CARD);
      realKey = encoder.encode(catalogIdentifier, EntityType.CATALOG);
      expectKey =
          Bytes.concat(
              EntityType.CATALOG.getShortName().getBytes(StandardCharsets.UTF_8),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L),
              BYTABLE_NAMESPACE_SEPARATOR);
      Assertions.assertArrayEquals(expectKey, realKey);

      // Scan all sc in metalake1.catalog2
      // catalog2 --> 1
      Mockito.doReturn(1L).when(mockIdGenerator).nextId();
      Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
      NameIdentifier schemaIdentifier = NameIdentifier.of(schemaNameSpace, WILD_CARD);
      realKey = encoder.encode(schemaIdentifier, EntityType.SCHEMA);
      expectKey =
          Bytes.concat(
              EntityType.SCHEMA.getShortName().getBytes(StandardCharsets.UTF_8),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1L),
              BYTABLE_NAMESPACE_SEPARATOR);
      Assertions.assertArrayEquals(expectKey, realKey);

      // Scan all table in metalake1.catalog2.schema3
      // schema3 --> 2
      Mockito.doReturn(2L).when(mockIdGenerator).nextId();
      Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
      NameIdentifier tableIdentifier = NameIdentifier.of(tableNameSpace, WILD_CARD);
      realKey = encoder.encode(tableIdentifier, EntityType.TABLE);
      expectKey =
          Bytes.concat(
              EntityType.TABLE.getShortName().getBytes(StandardCharsets.UTF_8),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1L),
              BYTABLE_NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(2L),
              BYTABLE_NAMESPACE_SEPARATOR);
      Assertions.assertArrayEquals(expectKey, realKey);

      Mockito.doReturn(3L).when(mockIdGenerator).nextId();
      Assertions.assertThrows(
          UnsupportedOperationException.class,
          () ->
              encoder.encode(
                  NameIdentifier.of(
                      Namespace.of("metalake1", "catalog2", "schema3", "table1"), WILD_CARD),
                  EntityType.COLUMN));
    }
  }
}
