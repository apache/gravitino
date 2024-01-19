/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.file;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHadoopCatalogOperations {

  private static final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String TEST_ROOT_PATH =
      "file:///tmp/gravitino_test_catalog_" + UUID.randomUUID().toString().replace("-", "");

  private static EntityStore store;

  private static IdGenerator idGenerator;

  @BeforeAll
  public static void setUp() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);

    Assertions.assertEquals(ROCKS_DB_STORE_PATH, config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    store.setSerDe(EntitySerDeFactory.createEntitySerDe(config));
    idGenerator = new RandomIdGenerator();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    store.close();
    FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    new Path(TEST_ROOT_PATH)
        .getFileSystem(new Configuration())
        .delete(new Path(TEST_ROOT_PATH), true);
  }

  @Test
  public void testHadoopCatalogConfiguration() {
    Map<String, String> emptyProps = Maps.newHashMap();
    HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator);
    ops.initialize(emptyProps);
    Configuration conf = ops.hadoopConf;
    String value = conf.get("fs.defaultFS");
    Assertions.assertEquals("file:///", value);

    emptyProps.put(CATALOG_BYPASS_PREFIX + "fs.defaultFS", "hdfs://localhost:9000");
    ops.initialize(emptyProps);
    Configuration conf1 = ops.hadoopConf;
    String value1 = conf1.get("fs.defaultFS");
    Assertions.assertEquals("hdfs://localhost:9000", value1);

    Assertions.assertFalse(ops.catalogStorageLocation.isPresent());

    emptyProps.put(HadoopCatalogPropertiesMetadata.LOCATION, "file:///tmp/catalog");
    ops.initialize(emptyProps);
    Assertions.assertTrue(ops.catalogStorageLocation.isPresent());
    Path expectedPath = new Path("file:///tmp/catalog");
    Assertions.assertEquals(expectedPath, ops.catalogStorageLocation.get());
  }

  @Test
  public void testCreateSchemaWithNoLocation() throws IOException {
    String name = "schema11";
    String comment = "comment11";
    Schema schema = createSchema(name, comment, null, null);
    Assertions.assertEquals(name, schema.name());
    Assertions.assertEquals(comment, schema.comment());
    Map<String, String> props = schema.properties();
    Assertions.assertTrue(
        props.containsKey(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
    Assertions.assertEquals(
        "true", props.get(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));

    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class, () -> createSchema(name, comment, null, null));
    Assertions.assertEquals("Schema schema11 already exists", exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithCatalogLocation() throws IOException {
    String name = "schema12";
    String comment = "comment12";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog12";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath = new Path(catalogPath, name);
    FileSystem fs = schemaPath.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath));
    Assertions.assertTrue(fs.isDirectory(schemaPath));
    Assertions.assertTrue(fs.listStatus(schemaPath).length == 0);
  }

  @Test
  public void testCreateSchemaWithSchemaLocation() throws IOException {
    String name = "schema13";
    String comment = "comment13";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog13";
    String schemaPath = catalogPath + "/" + name;
    Schema schema = createSchema(name, comment, null, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.isDirectory(schemaPath1));
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);

    // Test create schema on the same location
    String name1 = "schema13_1";
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> createSchema(name1, comment, null, schemaPath));
    Assertions.assertEquals(
        "Schema schema13_1 location " + schemaPath1 + " already exists", exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithCatalogAndSchemaLocation() throws IOException {
    String name = "schema14";
    String comment = "comment14";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog14";
    String schemaPath = TEST_ROOT_PATH + "/" + "schema14";
    Schema schema = createSchema(name, comment, catalogPath, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.isDirectory(schemaPath1));
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);

    Assertions.assertFalse(fs.exists(new Path(catalogPath)));
    Assertions.assertFalse(fs.exists(new Path(catalogPath, name)));
  }

  @Test
  public void testLoadSchema() throws IOException {
    String name = "schema15";
    String comment = "comment15";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog15";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator)) {
      ops.initialize(Maps.newHashMap());
      Schema schema1 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(
          props.containsKey(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertEquals(
          "true", props.get(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      Throwable exception =
          Assertions.assertThrows(
              NoSuchSchemaException.class,
              () -> ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", "schema16")));
      Assertions.assertEquals("Schema schema16 does not exist", exception.getMessage());
    }
  }

  @Test
  public void testListSchema() throws IOException {
    String name = "schema17";
    String comment = "comment17";
    String name1 = "schema18";
    String comment1 = "comment18";
    createSchema(name, comment, null, null);
    createSchema(name1, comment1, null, null);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator)) {
      ops.initialize(Maps.newHashMap());
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listSchemas(Namespace.of("m1", "c1"))).collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 2);
      Assertions.assertTrue(idents.contains(NameIdentifier.ofSchema("m1", "c1", name)));
      Assertions.assertTrue(idents.contains(NameIdentifier.ofSchema("m1", "c1", name1)));
    }
  }

  @Test
  public void testAlterSchema() throws IOException {
    String name = "schema19";
    String comment = "comment19";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog19";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator)) {
      ops.initialize(Maps.newHashMap());
      Schema schema1 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(
          props.containsKey(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertEquals(
          "true", props.get(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      String newKey = "k1";
      String newValue = "v1";
      SchemaChange setProperty = SchemaChange.setProperty(newKey, newValue);
      Schema schema2 = ops.alterSchema(NameIdentifier.ofSchema("m1", "c1", name), setProperty);
      Assertions.assertEquals(name, schema2.name());
      Assertions.assertEquals(comment, schema2.comment());
      Map<String, String> props2 = schema2.properties();
      Assertions.assertTrue(props2.containsKey(newKey));
      Assertions.assertEquals(newValue, props2.get(newKey));

      Schema schema3 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Map<String, String> props3 = schema3.properties();
      Assertions.assertTrue(props3.containsKey(newKey));
      Assertions.assertEquals(newValue, props3.get(newKey));

      SchemaChange removeProperty = SchemaChange.removeProperty(newKey);
      Schema schema4 = ops.alterSchema(NameIdentifier.ofSchema("m1", "c1", name), removeProperty);
      Assertions.assertEquals(name, schema4.name());
      Assertions.assertEquals(comment, schema4.comment());
      Map<String, String> props4 = schema4.properties();
      Assertions.assertFalse(props4.containsKey(newKey));

      Schema schema5 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Map<String, String> props5 = schema5.properties();
      Assertions.assertFalse(props5.containsKey(newKey));
    }
  }

  @Test
  public void testDropSchema() throws IOException {
    String name = "schema20";
    String comment = "comment20";
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog20";
    Schema schema = createSchema(name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator)) {
      ops.initialize(ImmutableMap.of(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath));
      Schema schema1 = ops.loadSchema(NameIdentifier.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(
          props.containsKey(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertEquals(
          "true", props.get(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      ops.dropSchema(NameIdentifier.ofSchema("m1", "c1", name), false);

      Path schemaPath = new Path(new Path(catalogPath), name);
      FileSystem fs = schemaPath.getFileSystem(new Configuration());
      Assertions.assertFalse(fs.exists(schemaPath));

      // Test drop non-empty schema with cascade = false
      Path subPath = new Path(schemaPath, "test1");
      fs.mkdirs(subPath);
      Assertions.assertTrue(fs.exists(subPath));

      Throwable exception1 =
          Assertions.assertThrows(
              NonEmptySchemaException.class,
              () -> ops.dropSchema(NameIdentifier.ofSchema("m1", "c1", name), false));
      Assertions.assertEquals(
          "Schema schema20 with location " + schemaPath + " is not empty", exception1.getMessage());

      // Test drop non-empty schema with cascade = true
      ops.dropSchema(NameIdentifier.ofSchema("m1", "c1", name), true);
      Assertions.assertFalse(fs.exists(schemaPath));
    }
  }

  private Schema createSchema(String name, String comment, String catalogPath, String schemaPath)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    if (catalogPath != null) {
      props.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store, idGenerator)) {
      ops.initialize(props);

      NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", name);
      Map<String, String> schemaProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      schemaProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, schemaProps));

      if (schemaPath != null) {
        schemaProps.put(HadoopSchemaPropertiesMetadata.LOCATION, schemaPath);
      }

      return ops.createSchema(schemaIdent, comment, schemaProps);
    }
  }
}
