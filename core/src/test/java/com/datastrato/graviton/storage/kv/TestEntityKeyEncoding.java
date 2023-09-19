/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder.NAMESPACE_SEPARATOR_BYTE_ARRAY;
import static com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder.WILD_CARD;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntitySerDeFactory;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.EntityStoreFactory;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.storage.IdGenerator;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.utils.ByteUtils;
import com.datastrato.graviton.utils.Bytes;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.ClassOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
@TestClassOrder(OrderAnnotation.class)
public class TestEntityKeyEncoding {
  private static final String ROCKS_DB_STORE_PATH = "/tmp/graviton_test_entity_key_encoding";
  private static Config config;

  public static EntityStore ENTITY_STORE_INSTANCE;
  private static BinaryEntityKeyEncoder ENCODER;

  @BeforeEach
  public void createEntityEncoderInstance() {
    ENTITY_STORE_INSTANCE = EntityStoreFactory.createEntityStore(config);
    Assertions.assertTrue(ENTITY_STORE_INSTANCE instanceof KvEntityStore);
    ENTITY_STORE_INSTANCE.initialize(config);
    ENTITY_STORE_INSTANCE.setSerDe(
        EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
    NameMappingService nameMappingService =
        new KvNameMappingService(((KvEntityStore) ENTITY_STORE_INSTANCE).getBackend());
    ENCODER = new BinaryEntityKeyEncoder(nameMappingService);
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

  @AfterEach
  public void cleanEnv() {
    try {
      if (ENTITY_STORE_INSTANCE != null) {
        ENTITY_STORE_INSTANCE.close();
      }

      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));

    } catch (Exception e) {
      // Ignore
    }
  }

  @BeforeAll
  public void prepare() {
    config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);
  }

  @Test
  @Order(1)
  public void testIdentifierEncoding()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    // Metalake
    // metalake1 --> 0
    Namespace namespace = Namespace.of();
    IdGenerator mockIdGenerator = getIdGeneratorAndSpy(ENCODER);

    Mockito.doReturn(0L).when(mockIdGenerator).nextId();
    NameIdentifier mateLakeIdentifier1 = NameIdentifier.of(namespace, "metalake1");
    byte[] realKey = ENCODER.encode(mateLakeIdentifier1, EntityType.METALAKE);
    byte[] expectKey =
        Bytes.concat(
            EntityType.METALAKE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
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
      realKey = ENCODER.encode(identifier, EntityType.CATALOG);
      expectKey =
          Bytes.concat(
              EntityType.CATALOG.getShortName().getBytes(),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(0L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
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
      realKey = ENCODER.encode(identifier, EntityType.SCHEMA);
      expectKey =
          Bytes.concat(
              EntityType.SCHEMA.getShortName().getBytes(),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(0L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(2L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
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
      realKey = ENCODER.encode(identifier, EntityType.TABLE);
      expectKey =
          Bytes.concat(
              EntityType.TABLE.getShortName().getBytes(),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(0L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(2L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(6L),
              NAMESPACE_SEPARATOR_BYTE_ARRAY,
              ByteUtils.longToByte(i + 7L));
      Assertions.assertArrayEquals(expectKey, realKey);
    }

    // Unsupported operation
    Mockito.doReturn(10L).when(mockIdGenerator).nextId();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          ENCODER.encode(
              NameIdentifier.of(
                  Namespace.of("metalake1", "catalog2", "schema3", "table1"), "column1"),
              EntityType.COLUMN);
        });
  }

  @Test
  @Order(10)
  public void testNamespaceEncoding()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    // Scan all Metalake
    Namespace namespace = Namespace.of();
    IdGenerator mockIdGenerator = getIdGeneratorAndSpy(ENCODER);

    NameIdentifier metalakeIdentifier = NameIdentifier.of(namespace, WILD_CARD);
    byte[] realKey = ENCODER.encode(metalakeIdentifier, EntityType.METALAKE);
    byte[] expectKey =
        Bytes.concat(EntityType.METALAKE.getShortName().getBytes(), NAMESPACE_SEPARATOR_BYTE_ARRAY);
    Assertions.assertArrayEquals(expectKey, realKey);

    // Scan all catalog in metalake1
    // metalake1 --> 0L
    Mockito.doReturn(0L).when(mockIdGenerator).nextId();
    Namespace catalogNamespace = Namespace.of("metalake1");
    NameIdentifier catalogIdentifier = NameIdentifier.of(catalogNamespace, WILD_CARD);
    realKey = ENCODER.encode(catalogIdentifier, EntityType.CATALOG);
    expectKey =
        Bytes.concat(
            EntityType.CATALOG.getShortName().getBytes(),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY);
    Assertions.assertArrayEquals(expectKey, realKey);

    // Scan all sc in metalake1.catalog2
    // catalog2 --> 1
    Mockito.doReturn(1L).when(mockIdGenerator).nextId();
    Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
    NameIdentifier schemaIdentifier = NameIdentifier.of(schemaNameSpace, WILD_CARD);
    realKey = ENCODER.encode(schemaIdentifier, EntityType.SCHEMA);
    expectKey =
        Bytes.concat(
            EntityType.SCHEMA.getShortName().getBytes(),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(1L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY);
    Assertions.assertArrayEquals(expectKey, realKey);

    // Scan all table in metalake1.catalog2.schema3
    // schema3 --> 2
    Mockito.doReturn(2L).when(mockIdGenerator).nextId();
    Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
    NameIdentifier tableIdentifier = NameIdentifier.of(tableNameSpace, WILD_CARD);
    realKey = ENCODER.encode(tableIdentifier, EntityType.TABLE);
    expectKey =
        Bytes.concat(
            EntityType.TABLE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(1L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY,
            ByteUtils.longToByte(2L),
            NAMESPACE_SEPARATOR_BYTE_ARRAY);
    Assertions.assertArrayEquals(expectKey, realKey);

    Mockito.doReturn(3L).when(mockIdGenerator).nextId();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ENCODER.encode(
                NameIdentifier.of(
                    Namespace.of("metalake1", "catalog2", "schema3", "table1"), WILD_CARD),
                EntityType.COLUMN));
  }

  @Test
  void testDecode() throws IOException {
    // Test decode metalake
    NameIdentifier metaLakeIdentifier1 = NameIdentifier.of("metalake1");
    byte[] makeLakeEncodeKey = ENCODER.encode(metaLakeIdentifier1, EntityType.METALAKE);
    Pair<NameIdentifier, EntityType> decodeIdentifier = ENCODER.decode(makeLakeEncodeKey);
    Assertions.assertEquals(metaLakeIdentifier1, decodeIdentifier.getKey());
    Assertions.assertEquals(EntityType.METALAKE, decodeIdentifier.getValue());

    // Test decode catalog
    NameIdentifier catalogIdentifier1 = NameIdentifier.of("metalake1", "catalog1");
    NameIdentifier catalogIdentifier2 = NameIdentifier.of("metalake1", "catalog1");

    byte[] encodeCatalog1 = ENCODER.encode(catalogIdentifier1, EntityType.CATALOG);
    Pair<NameIdentifier, EntityType> decodeIdentifier1 = ENCODER.decode(encodeCatalog1);
    Assertions.assertEquals(catalogIdentifier1, decodeIdentifier1.getKey());
    Assertions.assertEquals(EntityType.CATALOG, decodeIdentifier1.getValue());

    byte[] encodeCatalog2 = ENCODER.encode(catalogIdentifier2, EntityType.CATALOG);
    Pair<NameIdentifier, EntityType> decodeIdentifier2 = ENCODER.decode(encodeCatalog2);
    Assertions.assertEquals(catalogIdentifier2, decodeIdentifier2.getKey());
    Assertions.assertEquals(EntityType.CATALOG, decodeIdentifier2.getValue());

    // Test decode schema
    NameIdentifier schemaIdentifier1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    byte[] encodeSchema1 = ENCODER.encode(schemaIdentifier1, EntityType.SCHEMA);
    Pair<NameIdentifier, EntityType> decodeIdentifier3 = ENCODER.decode(encodeSchema1);
    Assertions.assertEquals(schemaIdentifier1, decodeIdentifier3.getKey());
    Assertions.assertEquals(EntityType.SCHEMA, decodeIdentifier3.getValue());

    // Test decode table
    NameIdentifier tableIdentifier1 =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    byte[] encodeTable1 = ENCODER.encode(tableIdentifier1, EntityType.TABLE);
    Pair<NameIdentifier, EntityType> decodeIdentifier4 = ENCODER.decode(encodeTable1);
    Assertions.assertEquals(tableIdentifier1, decodeIdentifier4.getKey());
    Assertions.assertEquals(EntityType.TABLE, decodeIdentifier4.getValue());

    NameIdentifier tableIdentifier2 =
        NameIdentifier.of("metalake1", "catalog1", "schema2", "table1");
    byte[] encodeTable2 = ENCODER.encode(tableIdentifier2, EntityType.TABLE);
    Pair<NameIdentifier, EntityType> decodeIdentifier5 = ENCODER.decode(encodeTable2);
    Assertions.assertEquals(tableIdentifier2, decodeIdentifier5.getKey());
    Assertions.assertEquals(EntityType.TABLE, decodeIdentifier5.getValue());

    NameIdentifier tableIdentifier3 =
        NameIdentifier.of("metalake1", "catalog2", "schema2", "table1");
    byte[] encodeTable3 = ENCODER.encode(tableIdentifier3, EntityType.TABLE);
    Pair<NameIdentifier, EntityType> decodeIdentifier6 = ENCODER.decode(encodeTable3);
    Assertions.assertEquals(tableIdentifier3, decodeIdentifier6.getKey());
    Assertions.assertEquals(EntityType.TABLE, decodeIdentifier6.getValue());
  }
}
