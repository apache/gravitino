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

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestEntityKeyEncoding {
  private static final String ROCKS_DB_STORE_PATH = "/tmp/graviton_test_entity_key_encoding";
  private static Config config;

  public static EntityStore ENTITY_STORE_INSTANCE;
  private static CustomEntityKeyEncoder ENCODER;

  @BeforeEach
  public void createEntityEncoderInstance() {
    ENTITY_STORE_INSTANCE = EntityStoreFactory.createEntityStore(config);
    Assertions.assertTrue(ENTITY_STORE_INSTANCE instanceof KvEntityStore);
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
  public void testIdentifierEncoding() throws IOException {
    // Metalake
    // metalake1 --> 0
    Namespace namespace = Namespace.of();
    NameIdentifier mateLakeIdentifier1 = NameIdentifier.of(namespace, "metalake1");
    byte[] realKey = ENCODER.encode(mateLakeIdentifier1, EntityType.METALAKE);
    byte[] expenctKey =
        Bytes.concat(
            EntityType.METALAKE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(0L));
    Assertions.assertArrayEquals(expenctKey, realKey);

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
      NameIdentifier identifier = catalogIdentifiers[i];
      realKey = ENCODER.encode(identifier, EntityType.CATALOG);
      expenctKey =
          Bytes.concat(
              EntityType.CATALOG.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i + 1));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(4, ENCODER.getNextUsableId());

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
      realKey = ENCODER.encode(identifier, EntityType.SCHEMA);
      expenctKey =
          Bytes.concat(
              EntityType.SCHEMA.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(0L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(2L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i + 4L));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(7, ENCODER.getNextUsableId());

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
      realKey = ENCODER.encode(identifier, EntityType.TABLE);
      expenctKey =
          Bytes.concat(
              EntityType.TABLE.getShortName().getBytes(),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(6L),
              NAMESPACE_SEPARATOR,
              ByteUtils.longToByte(i + 7L));
      Assertions.assertArrayEquals(expenctKey, realKey);
    }
    // Assert next useable id
    Assertions.assertEquals(10, ENCODER.getNextUsableId());

    // test error
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
  public void testNamespaceEncoding() throws IOException {
    // Scan all Metalake
    Namespace namespace = Namespace.of();
    byte[] realKey = ENCODER.encode(namespace, EntityType.METALAKE);
    byte[] expenctKey =
        Bytes.concat(EntityType.METALAKE.getShortName().getBytes(), NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);

    // Scan all catalog in metalake1
    // metalake1 --> 0
    Namespace catalogNamespace = Namespace.of("metalake1");
    realKey = ENCODER.encode(catalogNamespace, EntityType.CATALOG);
    expenctKey =
        Bytes.concat(
            EntityType.CATALOG.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert next useable id
    Assertions.assertEquals(1, ENCODER.getNextUsableId());

    // Scan all sc in metalake1.catalog2
    // catalog2 --> 1
    Namespace schemaNameSpace = Namespace.of("metalake1", "catalog2");
    realKey = ENCODER.encode(schemaNameSpace, EntityType.SCHEMA);
    expenctKey =
        Bytes.concat(
            EntityType.SCHEMA.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(0L),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(1L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert next useable id
    Assertions.assertEquals(2, ENCODER.getNextUsableId());

    // Scan all table in metalake1.catalog2.schema3
    // schema3 --> 2
    Namespace tableNameSpace = Namespace.of("metalake1", "catalog2", "schema3");
    realKey = ENCODER.encode(tableNameSpace, EntityType.TABLE);
    expenctKey =
        Bytes.concat(
            EntityType.TABLE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(2L),
            NAMESPACE_SEPARATOR);
    Assertions.assertArrayEquals(expenctKey, realKey);
    // Assert max id
    Assertions.assertEquals(3, ENCODER.getNextUsableId());

    // test error
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          ENCODER.encode(
              Namespace.of("metalake1", "catalog2", "schema3", "table1"), EntityType.COLUMN);
        });
  }
}
