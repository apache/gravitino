/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

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
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class TestHadoopCatalogOperations {

  private static final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String UNFORMALIZED_TEST_ROOT_PATH =
      "/tmp/gravitino_test_catalog_" + UUID.randomUUID().toString().replace("-", "");

  private static final String TEST_ROOT_PATH = "file:" + UNFORMALIZED_TEST_ROOT_PATH;

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
    HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store);
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
    Assertions.assertEquals("Schema m1.c1.schema11 already exists", exception.getMessage());
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
    NameIdentifier schema16 = NameIdentifier.ofSchema("m1", "c1", "schema16");

    Assertions.assertEquals(name, schema.name());

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
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
          Assertions.assertThrows(NoSuchSchemaException.class, () -> ops.loadSchema(schema16));
      Assertions.assertEquals("Schema m1.c1.schema16 does not exist", exception.getMessage());
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

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
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

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
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
    NameIdentifier id = NameIdentifier.ofSchema("m1", "c1", name);

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(ImmutableMap.of(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath));
      Schema schema1 = ops.loadSchema(id);
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(
          props.containsKey(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertEquals(
          "true", props.get(BaseCatalogPropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      ops.dropSchema(id, false);

      Path schemaPath = new Path(new Path(catalogPath), name);
      FileSystem fs = schemaPath.getFileSystem(new Configuration());
      Assertions.assertFalse(fs.exists(schemaPath));

      // Test drop non-empty schema with cascade = false
      Path subPath = new Path(schemaPath, "test1");
      fs.mkdirs(subPath);
      Assertions.assertTrue(fs.exists(subPath));

      Throwable exception1 =
          Assertions.assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(id, false));
      Assertions.assertEquals(
          "Schema m1.c1.schema20 with location " + schemaPath + " is not empty",
          exception1.getMessage());

      // Test drop non-empty schema with cascade = true
      ops.dropSchema(id, true);
      Assertions.assertFalse(fs.exists(schemaPath));
    }
  }

  @ParameterizedTest
  @MethodSource("locationArguments")
  public void testCreateLoadAndDeleteFilesetWithLocations(
      String name,
      Fileset.Type type,
      String catalogPath,
      String schemaPath,
      String storageLocation,
      String expect)
      throws IOException {
    String schemaName = "s1_" + name;
    String comment = "comment_s1";
    Map<String, String> catalogProps = Maps.newHashMap();
    if (catalogPath != null) {
      catalogProps.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", schemaName);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(catalogProps);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(schemaName, comment, catalogPath, schemaPath);
      }
      Fileset fileset =
          createFileset(name, schemaName, "comment", type, catalogPath, storageLocation);

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());
      Assertions.assertEquals(expect, fileset.storageLocation());

      // Test load
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Fileset loadedFileset = ops.loadFileset(filesetIdent);
      Assertions.assertEquals(name, loadedFileset.name());
      Assertions.assertEquals(type, loadedFileset.type());
      Assertions.assertEquals("comment", loadedFileset.comment());
      Assertions.assertEquals(expect, loadedFileset.storageLocation());

      // Test drop
      ops.dropFileset(filesetIdent);
      Path expectedPath = new Path(expect);
      FileSystem fs = expectedPath.getFileSystem(new Configuration());
      if (type == Fileset.Type.MANAGED) {
        Assertions.assertFalse(fs.exists(expectedPath));
      } else {
        Assertions.assertTrue(fs.exists(expectedPath));
      }
    }
  }

  @Test
  public void testCreateFilesetWithExceptions() throws IOException {
    String schemaName = "schema22";
    String comment = "comment22";
    createSchema(schemaName, comment, null, null);
    String name = "fileset22";
    NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

    // If neither catalog location, nor schema location and storageLocation is specified.
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> createFileset(name, schemaName, comment, Fileset.Type.MANAGED, null, null));
    Assertions.assertEquals(
        "Storage location must be set for fileset "
            + filesetIdent
            + " when it's catalog and schema "
            + "location are not set",
        exception.getMessage());
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(Maps.newHashMap());
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals("Fileset m1.c1.schema22.fileset22 does not exist", e.getMessage());
    }

    // For external fileset, if storageLocation is not specified.
    Throwable exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> createFileset(name, schemaName, comment, Fileset.Type.EXTERNAL, null, null));
    Assertions.assertEquals(
        "Storage location must be set for external fileset " + filesetIdent,
        exception1.getMessage());
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(Maps.newHashMap());
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals("Fileset " + filesetIdent + " does not exist", e.getMessage());
    }
  }

  @Test
  public void testListFilesets() throws IOException {
    String schemaName = "schema23";
    String comment = "comment23";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);
    String[] filesets = new String[] {"fileset23_1", "fileset23_2", "fileset23_3"};
    for (String fileset : filesets) {
      createFileset(fileset, schemaName, comment, Fileset.Type.MANAGED, null, null);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(Maps.newHashMap());
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listFilesets(Namespace.of("m1", "c1", schemaName)))
              .collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 3);
      for (String fileset : filesets) {
        Assertions.assertTrue(idents.contains(NameIdentifier.of("m1", "c1", schemaName, fileset)));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("testRenameArguments")
  public void testRenameFileset(
      String name,
      String newName,
      Fileset.Type type,
      String catalogPath,
      String schemaPath,
      String storageLocation,
      String expect)
      throws IOException {
    String schemaName = "s24_" + name;
    String comment = "comment_s24";
    Map<String, String> catalogProps = Maps.newHashMap();
    if (catalogPath != null) {
      catalogProps.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifier.ofSchema("m1", "c1", schemaName);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(catalogProps);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(schemaName, comment, catalogPath, schemaPath);
      }
      Fileset fileset =
          createFileset(name, schemaName, "comment", type, catalogPath, storageLocation);

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Fileset loadedFileset = ops.loadFileset(filesetIdent);
      Assertions.assertEquals(name, loadedFileset.name());
      Assertions.assertEquals(type, loadedFileset.type());
      Assertions.assertEquals("comment", loadedFileset.comment());

      Fileset renamedFileset = ops.alterFileset(filesetIdent, FilesetChange.rename(newName));
      Assertions.assertEquals(newName, renamedFileset.name());
      Assertions.assertEquals(type, renamedFileset.type());
      Assertions.assertEquals("comment", renamedFileset.comment());
      Assertions.assertEquals(expect, renamedFileset.storageLocation());

      Fileset loadedRenamedFileset =
          ops.loadFileset(NameIdentifier.of("m1", "c1", schemaName, newName));
      Assertions.assertEquals(newName, loadedRenamedFileset.name());
      Assertions.assertEquals(type, loadedRenamedFileset.type());
      Assertions.assertEquals("comment", loadedRenamedFileset.comment());
      Assertions.assertEquals(expect, loadedRenamedFileset.storageLocation());
    }
  }

  @Test
  public void testAlterFilesetProperties() throws IOException {
    String schemaName = "schema25";
    String comment = "comment25";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String name = "fileset25";
    Fileset fileset = createFileset(name, schemaName, comment, Fileset.Type.MANAGED, null, null);

    FilesetChange change1 = FilesetChange.setProperty("k1", "v1");
    FilesetChange change2 = FilesetChange.removeProperty("k1");

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(Maps.newHashMap());
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("comment25", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey("k1"));
      Assertions.assertEquals("v1", props1.get("k1"));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change2);
      Assertions.assertEquals(name, fileset2.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type());
      Assertions.assertEquals("comment25", fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertFalse(props2.containsKey("k1"));
    }
  }

  @Test
  public void testFormalizePath() throws IOException {

    String[] paths =
        new String[] {
          "tmp/catalog",
          "/tmp/catalog",
          "file:/tmp/catalog",
          "file:///tmp/catalog",
          "hdfs://localhost:9000/tmp/catalog",
          "s3://bucket/tmp/catalog",
          "gs://bucket/tmp/catalog"
        };

    String[] expected =
        new String[] {
          "file:" + Paths.get("").toAbsolutePath() + "/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog",
          "hdfs://localhost:9000/tmp/catalog",
          "s3://bucket/tmp/catalog",
          "gs://bucket/tmp/catalog"
        };

    for (int i = 0; i < paths.length; i++) {
      Path actual = HadoopCatalogOperations.formalizePath(new Path(paths[i]), new Configuration());
      Assertions.assertEquals(expected[i], actual.toString());
    }
  }

  @Test
  public void testUpdateFilesetComment() throws IOException {
    String schemaName = "schema26";
    String comment = "comment26";
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    createSchema(schemaName, comment, null, schemaPath);

    String name = "fileset26";
    Fileset fileset = createFileset(name, schemaName, comment, Fileset.Type.MANAGED, null, null);

    FilesetChange change1 = FilesetChange.updateComment("comment26_new");
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(Maps.newHashMap());
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals("comment26_new", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    }
  }

  private static Stream<Arguments> locationArguments() {
    return Stream.of(
        // Honor the catalog location
        Arguments.of(
            "fileset11",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog21",
            null,
            null,
            TEST_ROOT_PATH + "/catalog21/s1_fileset11/fileset11"),
        Arguments.of(
            // honor the schema location
            "fileset12",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/s1_fileset12",
            null,
            TEST_ROOT_PATH + "/s1_fileset12/fileset12"),
        Arguments.of(
            // honor the schema location
            "fileset13",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog22",
            TEST_ROOT_PATH + "/s1_fileset13",
            null,
            TEST_ROOT_PATH + "/s1_fileset13/fileset13"),
        Arguments.of(
            // honor the storage location
            "fileset14",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog23",
            TEST_ROOT_PATH + "/s1_fileset14",
            TEST_ROOT_PATH + "/fileset14",
            TEST_ROOT_PATH + "/fileset14"),
        Arguments.of(
            // honor the storage location
            "fileset15",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/fileset15",
            TEST_ROOT_PATH + "/fileset15"),
        Arguments.of(
            // honor the storage location
            "fileset16",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog24",
            null,
            TEST_ROOT_PATH + "/fileset16",
            TEST_ROOT_PATH + "/fileset16"),
        Arguments.of(
            // honor the storage location
            "fileset17",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/catalog25",
            TEST_ROOT_PATH + "/s1_fileset17",
            TEST_ROOT_PATH + "/fileset17",
            TEST_ROOT_PATH + "/fileset17"),
        Arguments.of(
            // honor the storage location
            "fileset18",
            Fileset.Type.EXTERNAL,
            null,
            TEST_ROOT_PATH + "/s1_fileset18",
            TEST_ROOT_PATH + "/fileset18",
            TEST_ROOT_PATH + "/fileset18"),
        Arguments.of(
            // honor the storage location
            "fileset19",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset19",
            TEST_ROOT_PATH + "/fileset19"),
        // Honor the catalog location
        Arguments.of(
            "fileset101",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog201",
            null,
            null,
            TEST_ROOT_PATH + "/catalog201/s1_fileset101/fileset101"),
        Arguments.of(
            // honor the schema location
            "fileset102",
            Fileset.Type.MANAGED,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset102",
            null,
            TEST_ROOT_PATH + "/s1_fileset102/fileset102"),
        Arguments.of(
            // honor the schema location
            "fileset103",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog202",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset103",
            null,
            TEST_ROOT_PATH + "/s1_fileset103/fileset103"),
        Arguments.of(
            // honor the storage location
            "fileset104",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog203",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset104",
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset104",
            TEST_ROOT_PATH + "/fileset104"),
        Arguments.of(
            // honor the storage location
            "fileset105",
            Fileset.Type.MANAGED,
            null,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset105",
            TEST_ROOT_PATH + "/fileset105"),
        Arguments.of(
            // honor the storage location
            "fileset106",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog204",
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset106",
            TEST_ROOT_PATH + "/fileset106"),
        Arguments.of(
            // honor the storage location
            "fileset107",
            Fileset.Type.EXTERNAL,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog205",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset107",
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset107",
            TEST_ROOT_PATH + "/fileset107"),
        Arguments.of(
            // honor the storage location
            "fileset108",
            Fileset.Type.EXTERNAL,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset108",
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset108",
            TEST_ROOT_PATH + "/fileset108"),
        Arguments.of(
            // honor the storage location
            "fileset109",
            Fileset.Type.EXTERNAL,
            null,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset109",
            TEST_ROOT_PATH + "/fileset109"));
  }

  private static Stream<Arguments> testRenameArguments() {
    return Stream.of(
        // Honor the catalog location
        Arguments.of(
            "fileset31",
            "fileset31_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog21",
            null,
            null,
            TEST_ROOT_PATH + "/catalog21/s24_fileset31/fileset31"),
        Arguments.of(
            // honor the schema location
            "fileset32",
            "fileset32_new",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/s24_fileset32",
            null,
            TEST_ROOT_PATH + "/s24_fileset32/fileset32"),
        Arguments.of(
            // honor the schema location
            "fileset33",
            "fileset33_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog22",
            TEST_ROOT_PATH + "/s24_fileset33",
            null,
            TEST_ROOT_PATH + "/s24_fileset33/fileset33"),
        Arguments.of(
            // honor the storage location
            "fileset34",
            "fileset34_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog23",
            TEST_ROOT_PATH + "/s24_fileset34",
            TEST_ROOT_PATH + "/fileset34",
            TEST_ROOT_PATH + "/fileset34"),
        Arguments.of(
            // honor the storage location
            "fileset35",
            "fileset35_new",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/fileset35",
            TEST_ROOT_PATH + "/fileset35"),
        Arguments.of(
            // honor the storage location
            "fileset36",
            "fileset36_new",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog24",
            null,
            TEST_ROOT_PATH + "/fileset36",
            TEST_ROOT_PATH + "/fileset36"),
        Arguments.of(
            // honor the storage location
            "fileset37",
            "fileset37_new",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/catalog25",
            TEST_ROOT_PATH + "/s24_fileset37",
            TEST_ROOT_PATH + "/fileset37",
            TEST_ROOT_PATH + "/fileset37"),
        Arguments.of(
            // honor the storage location
            "fileset38",
            "fileset38_new",
            Fileset.Type.EXTERNAL,
            null,
            TEST_ROOT_PATH + "/s24_fileset38",
            TEST_ROOT_PATH + "/fileset38",
            TEST_ROOT_PATH + "/fileset38"),
        Arguments.of(
            // honor the storage location
            "fileset39",
            "fileset39_new",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset39",
            TEST_ROOT_PATH + "/fileset39"));
  }

  private Schema createSchema(String name, String comment, String catalogPath, String schemaPath)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    if (catalogPath != null) {
      props.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
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

  private Fileset createFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      String catalogPath,
      String storageLocation)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    if (catalogPath != null) {
      props.put(HadoopCatalogPropertiesMetadata.LOCATION, catalogPath);
    }

    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(null, store)) {
      ops.initialize(props);

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Map<String, String> filesetProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      filesetProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps));

      return ops.createFileset(filesetIdent, comment, type, storageLocation, filesetProps);
    }
  }
}
