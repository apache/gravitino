/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.DEFUALT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.graviton.storage.kv.CustomEntityKeyEncoder.NAMESPACE_SEPARATOR;
import static com.datastrato.graviton.storage.kv.CustomEntityKeyEncoder.WILD_CARD;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.Entity.EntityIdentifer;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntitySerDeFactory;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.EntityStoreFactory;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
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
  private static CustomEntityKeyEncoder ENCODER;

  @BeforeEach
  public void createEntityEncoderInstance() {
    ENTITY_STORE_INSTANCE = EntityStoreFactory.createEntityStore(config);
    Assertions.assertTrue(ENTITY_STORE_INSTANCE instanceof KvEntityStore);
    ENTITY_STORE_INSTANCE.initialize(config);
    ENTITY_STORE_INSTANCE.setSerDe(
        EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE)));
    ENCODER = new CustomEntityKeyEncoder(((KvEntityStore) ENTITY_STORE_INSTANCE).getBackend());
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
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFUALT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);
  }

  @Test
  @Order(1)
  public void testIdentifierEncoding() throws IOException {
    // Metalake
    // metalake1 --> 1000000
    Namespace namespace = Namespace.of();
    CustomEntityKeyEncoder mockEncoder = Mockito.spy(ENCODER);

    Mockito.doReturn(1000000L)
        .when(mockEncoder)
        .getOrCreateId(Mockito.eq("metalake1"), Mockito.eq(true));
    NameIdentifier mateLakeIdentifier1 = NameIdentifier.of(namespace, "metalake1");
    byte[] realKey =
        mockEncoder.encode(EntityIdentifer.of(mateLakeIdentifier1, EntityType.METALAKE));
    byte[] expenctKey =
        Bytes.concat(
            EntityType.METALAKE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1000000L));
    Assertions.assertArrayEquals(expenctKey, realKey);

    // name ---> id
    // catalog1 --> 0
    // catalog2 --> 1
    // catalog3 --> 2
    Namespace catalogNamespace = Namespace.of("metalake1");
    NameIdentifier catalogIdentifier1 = NameIdentifier.of(catalogNamespace, "catalog1");
    NameIdentifier catalogIdentifier2 = NameIdentifier.of(catalogNamespace, "catalog2");
    NameIdentifier catalogIdentifier3 = NameIdentifier.of(catalogNamespace, "catalog3");
    NameIdentifier[] catalogIdentifiers =
        new NameIdentifier[] {catalogIdentifier1, catalogIdentifier2, catalogIdentifier3};

    for (int i = 0; i < catalogIdentifiers.length; i++) {
      NameIdentifier identifier = catalogIdentifiers[i];
      realKey = mockEncoder.encode(EntityIdentifer.of(identifier, EntityType.CATALOG));
      expenctKey =
          Bytes.concat(
              EntityType.CATALOG.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1000000L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(3, ENCODER.getNextUsableId());

    // name ---> id
    // schema1 --> 3
    // schema2 --> 4
    // schema3 --> 5
    Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
    NameIdentifier schemaIdentifier1 = NameIdentifier.of(schemaNameSpace, "schema1");
    NameIdentifier schemaIdentifier2 = NameIdentifier.of(schemaNameSpace, "schema2");
    NameIdentifier schemaIdentifier3 = NameIdentifier.of(schemaNameSpace, "schema3");
    NameIdentifier[] schemaIdentifiers =
        new NameIdentifier[] {schemaIdentifier1, schemaIdentifier2, schemaIdentifier3};

    for (int i = 0; i < schemaIdentifiers.length; i++) {
      NameIdentifier identifier = schemaIdentifiers[i];
      realKey = mockEncoder.encode(EntityIdentifer.of(identifier, EntityType.SCHEMA));
      expenctKey =
          Bytes.concat(
              EntityType.SCHEMA.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1000000L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i + 3L));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(6, ENCODER.getNextUsableId());

    // name ---> id
    // table1 --> 6
    // table2 --> 7
    // table3 --> 8
    Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
    NameIdentifier tableIdentifier1 = NameIdentifier.of(tableNameSpace, "table1");
    NameIdentifier tableIdentifier2 = NameIdentifier.of(tableNameSpace, "table2");
    NameIdentifier tableIdentifier3 = NameIdentifier.of(tableNameSpace, "table3");
    NameIdentifier[] tableIdentifiers =
        new NameIdentifier[] {tableIdentifier1, tableIdentifier2, tableIdentifier3};

    for (int i = 0; i < tableIdentifiers.length; i++) {
      NameIdentifier identifier = tableIdentifiers[i];
      realKey = mockEncoder.encode(EntityIdentifer.of(identifier, EntityType.TABLE));
      expenctKey =
          Bytes.concat(
              EntityType.TABLE.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1000000L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(1L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(5L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i + 6L));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(9, ENCODER.getNextUsableId());

    // Unsupported operation
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          ENCODER.encode(
              EntityIdentifer.of(
                  NameIdentifier.of(
                      Namespace.of("metalake1", "catalog2", "schema3", "table1"), "column1"),
                  EntityType.COLUMN));
        });
  }

  @Test
  @Order(10)
  public void testNamespaceEncoding() throws IOException {
    // Scan all Metalake
    Namespace namespace = Namespace.of();
    CustomEntityKeyEncoder mockEncoder = Mockito.spy(ENCODER);

    Mockito.doReturn(1000000L)
        .when(mockEncoder)
        .getOrCreateId(Mockito.eq("metalake1"), Mockito.eq(true));
    NameIdentifier metalakeIdentifier = NameIdentifier.of(namespace, WILD_CARD);
    byte[] realKey =
        mockEncoder.encode(EntityIdentifer.of(metalakeIdentifier, EntityType.METALAKE));
    byte[] expenctKey =
        Bytes.concat(EntityType.METALAKE.getShortName().getBytes(), NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);

    // Scan all catalog in metalake1
    // metalake1 --> 1000000L
    Namespace catalogNamespace = Namespace.of("metalake1");
    NameIdentifier catalogIdentifier = NameIdentifier.of(catalogNamespace, WILD_CARD);
    realKey = mockEncoder.encode(EntityIdentifer.of(catalogIdentifier, EntityType.CATALOG));
    expenctKey =
        Bytes.concat(
            EntityType.CATALOG.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1000000L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert next useable id
    Assertions.assertEquals(0, ENCODER.getNextUsableId());

    // Scan all sc in metalake1.catalog2
    // catalog2 --> 0
    Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
    NameIdentifier schemaIdentifier = NameIdentifier.of(schemaNameSpace, WILD_CARD);
    realKey = mockEncoder.encode(EntityIdentifer.of(schemaIdentifier, EntityType.SCHEMA));
    expenctKey =
        Bytes.concat(
            EntityType.SCHEMA.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1000000L),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert next useable id
    Assertions.assertEquals(1, ENCODER.getNextUsableId());

    // Scan all table in metalake1.catalog2.schema3
    // schema3 --> 1
    Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
    NameIdentifier tableIdentifier = NameIdentifier.of(tableNameSpace, WILD_CARD);
    realKey = mockEncoder.encode(EntityIdentifer.of(tableIdentifier, EntityType.TABLE));
    expenctKey =
        Bytes.concat(
            EntityType.TABLE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1000000L),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert next useable id
    Assertions.assertEquals(2, ENCODER.getNextUsableId());

    // Unsupported operation
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          mockEncoder.encode(
              EntityIdentifer.of(
                  NameIdentifier.of(
                      Namespace.of("metalake1", "catalog2", "schema3", "table1"), WILD_CARD),
                  EntityType.COLUMN));
        });
  }
}
