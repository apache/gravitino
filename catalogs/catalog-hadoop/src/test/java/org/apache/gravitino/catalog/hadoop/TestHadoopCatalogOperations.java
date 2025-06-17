/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.hadoop;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalog.CATALOG_PROPERTIES_META;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalog.FILESET_PROPERTIES_META;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalog.SCHEMA_PROPERTIES_META;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.DISABLE_FILESYSTEM_OPS;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.LOCATION;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestHadoopCatalogOperations {

  private static final String STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String H2_file = STORE_PATH + ".mv.db";
  private static final String UNFORMALIZED_TEST_ROOT_PATH =
      "/tmp/gravitino_test_catalog_" + UUID.randomUUID().toString().replace("-", "");

  private static final String TEST_ROOT_PATH = "file:" + UNFORMALIZED_TEST_ROOT_PATH;

  private static final HasPropertyMetadata HADOOP_PROPERTIES_METADATA =
      new HasPropertyMetadata() {
        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support table properties");
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
          return FILESET_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support topic properties");
        }

        @Override
        public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support model properties");
        }
      };

  private static EntityStore store;

  private static IdGenerator idGenerator;
  private static SchemaMetaService spySchemaMetaService;

  private static CatalogInfo randomCatalogInfo() {
    return new CatalogInfo(
        idGenerator.nextId(),
        "catalog1",
        CatalogInfo.Type.FILESET,
        "provider1",
        "comment1",
        Maps.newHashMap(),
        null,
        Namespace.of("m1", "c1"));
  }

  private static CatalogInfo randomCatalogInfo(String metalakeName, String catalogName) {
    return new CatalogInfo(
        idGenerator.nextId(),
        catalogName,
        CatalogInfo.Type.FILESET,
        "hadoop",
        "comment1",
        Maps.newHashMap(),
        null,
        Namespace.of(metalakeName));
  }

  private static CatalogInfo randomCatalogInfo(
      String metalakeName, String catalogName, Map<String, String> props) {
    return new CatalogInfo(
        idGenerator.nextId(),
        catalogName,
        CatalogInfo.Type.FILESET,
        "hadoop",
        "comment1",
        props,
        null,
        Namespace.of(metalakeName));
  }

  @BeforeAll
  public static void setUp() {
    Config config = Mockito.mock(Config.class);
    when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(STORE_PATH);

    // The following properties are used to create the JDBC connection; they are just for test, in
    // the real world,
    // they will be set automatically by the configuration file if you set ENTITY_RELATIONAL_STORE
    // as EMBEDDED_ENTITY_RELATIONAL_STORE.
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

    File f = FileUtils.getFile(STORE_PATH);
    f.deleteOnExit();

    when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    idGenerator = new RandomIdGenerator();

    // Mock
    MetalakeMetaService metalakeMetaService = MetalakeMetaService.getInstance();
    MetalakeMetaService spyMetaService = Mockito.spy(metalakeMetaService);
    doReturn(1L).when(spyMetaService).getMetalakeIdByName(Mockito.anyString());

    CatalogMetaService catalogMetaService = CatalogMetaService.getInstance();
    CatalogMetaService spyCatalogMetaService = Mockito.spy(catalogMetaService);
    doReturn(1L)
        .when(spyCatalogMetaService)
        .getCatalogIdByMetalakeIdAndName(Mockito.anyLong(), Mockito.anyString());

    SchemaMetaService serviceMetaService = SchemaMetaService.getInstance();
    spySchemaMetaService = Mockito.spy(serviceMetaService);

    doReturn(new CatalogIds(1L, 1L))
        .when(spyCatalogMetaService)
        .getCatalogIdByMetalakeAndCatalogName(Mockito.anyString(), Mockito.anyString());

    Stream<Arguments> argumentsStream = testRenameArguments();
    argumentsStream.forEach(
        arguments -> {
          String oldName = (String) arguments.get()[0];
          String newName = (String) arguments.get()[1];
          long schemaId = idGenerator.nextId();
          doReturn(new SchemaIds(1L, 1L, schemaId))
              .when(spySchemaMetaService)
              .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                  Mockito.anyString(), Mockito.anyString(), Mockito.eq("s24_" + oldName));
          doReturn(new SchemaIds(1L, 1L, schemaId))
              .when(spySchemaMetaService)
              .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                  Mockito.anyString(), Mockito.anyString(), Mockito.eq("s24_" + newName));
        });

    locationArguments()
        .forEach(
            arguments -> {
              String name = (String) arguments.get()[0];
              long schemaId = idGenerator.nextId();
              doReturn(new SchemaIds(1L, 1L, schemaId))
                  .when(spySchemaMetaService)
                  .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                      Mockito.anyString(), Mockito.anyString(), Mockito.eq("s1_" + name));
            });

    locationWithPlaceholdersArguments()
        .forEach(
            arguments -> {
              String name = (String) arguments.get()[0];
              long schemaId = idGenerator.nextId();
              doReturn(new SchemaIds(1L, 1L, schemaId))
                  .when(spySchemaMetaService)
                  .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                      Mockito.anyString(), Mockito.anyString(), Mockito.eq("s1_" + name));
            });

    multipleLocationsArguments()
        .forEach(
            arguments -> {
              String name = (String) arguments.get()[0];
              long schemaId = idGenerator.nextId();
              doReturn(new SchemaIds(1L, 1L, schemaId))
                  .when(spySchemaMetaService)
                  .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                      Mockito.anyString(), Mockito.anyString(), Mockito.eq("s1_" + name));
            });

    MockedStatic<MetalakeMetaService> metalakeMetaServiceMockedStatic =
        Mockito.mockStatic(MetalakeMetaService.class);
    MockedStatic<CatalogMetaService> catalogMetaServiceMockedStatic =
        Mockito.mockStatic(CatalogMetaService.class);
    MockedStatic<SchemaMetaService> schemaMetaServiceMockedStatic =
        Mockito.mockStatic(SchemaMetaService.class);

    metalakeMetaServiceMockedStatic
        .when(MetalakeMetaService::getInstance)
        .thenReturn(spyMetaService);
    catalogMetaServiceMockedStatic
        .when(CatalogMetaService::getInstance)
        .thenReturn(spyCatalogMetaService);
    schemaMetaServiceMockedStatic
        .when(SchemaMetaService::getInstance)
        .thenReturn(spySchemaMetaService);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    store.close();
    new Path(TEST_ROOT_PATH)
        .getFileSystem(new Configuration())
        .delete(new Path(TEST_ROOT_PATH), true);

    File f = FileUtils.getFile(H2_file);
    f.delete();
  }

  @Test
  public void testHadoopCatalogConfiguration() {
    Map<String, String> emptyProps = Maps.newHashMap();
    SecureHadoopCatalogOperations secOps = new SecureHadoopCatalogOperations(store);

    HadoopCatalogOperations ops = secOps.getBaseHadoopCatalogOperations();

    CatalogInfo catalogInfo = randomCatalogInfo();
    ops.initialize(emptyProps, catalogInfo, HADOOP_PROPERTIES_METADATA);
    Configuration conf = ops.getHadoopConf();
    String value = conf.get("fs.defaultFS");
    Assertions.assertEquals("file:///", value);

    emptyProps.put(LOCATION, "file:///tmp/catalog");
    ops.initialize(emptyProps, catalogInfo, HADOOP_PROPERTIES_METADATA);
    Assertions.assertEquals(1, ops.catalogStorageLocations.size());
    Path expectedPath = new Path("file:///tmp/catalog");
    Assertions.assertEquals(expectedPath, ops.catalogStorageLocations.get(LOCATION_NAME_UNKNOWN));

    // test placeholder in location
    emptyProps.put(LOCATION, "file:///tmp/{{catalog}}");
    ops.initialize(emptyProps, catalogInfo, HADOOP_PROPERTIES_METADATA);
    Assertions.assertEquals(1, ops.catalogStorageLocations.size());
    expectedPath = new Path("file:///tmp/{{catalog}}");
    Assertions.assertEquals(expectedPath, ops.catalogStorageLocations.get(LOCATION_NAME_UNKNOWN));

    // test illegal placeholder in location
    emptyProps.put(LOCATION, "file:///tmp/{{}}");
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ops.initialize(emptyProps, catalogInfo, HADOOP_PROPERTIES_METADATA));
    Assertions.assertTrue(
        exception.getMessage().contains("Placeholder in location should not be empty"),
        exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithNoLocation() throws IOException {
    final long testId = generateTestId();
    final String name = "schema" + testId;
    final String comment = "comment" + testId;
    Schema schema = createSchema(testId, name, comment, null, null);
    Assertions.assertEquals(name, schema.name());
    Assertions.assertEquals(comment, schema.comment());

    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> createSchema(testId, name, comment, null, null));
    Assertions.assertEquals(
        "Schema m1.c1.schema" + testId + " already exists", exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithEmptyCatalogLocation() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String catalogPath = "";

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> createSchema(testId, schemaName, comment, catalogPath, null));
    Assertions.assertEquals(
        "The value of the catalog property "
            + HadoopCatalogPropertiesMetadata.LOCATION
            + " must not be blank",
        exception.getMessage());
  }

  @Test
  public void testCreateSchemaWithCatalogLocation() throws IOException {
    final long testId = generateTestId();
    String name = "schema" + testId;
    final String comment = "comment" + testId;
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog12";
    Schema schema = createSchema(testId, name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath = new Path(catalogPath, name);
    FileSystem fs = schemaPath.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath));
    Assertions.assertTrue(fs.getFileStatus(schemaPath).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath).length == 0);

    // test placeholder in catalog location
    name = "schema" + testId + "_1";
    catalogPath = TEST_ROOT_PATH + "/" + "{{catalog}}-{{schema}}";
    schema = createSchema(testId, name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    schemaPath = new Path(catalogPath, name);
    fs = schemaPath.getFileSystem(new Configuration());
    Assertions.assertFalse(fs.exists(schemaPath));

    // Test disable server-side FS operations.
    name = "schema" + testId + "_2";
    catalogPath = TEST_ROOT_PATH + "/" + "catalog12_2";
    schema = createSchema(testId, name, comment, catalogPath, null, true);
    Assertions.assertEquals(name, schema.name());

    // Schema path should not be existed if the server-side FS operations are disabled.
    schemaPath = new Path(catalogPath, name);
    Assertions.assertFalse(fs.exists(schemaPath));
  }

  @Test
  public void testCreateSchemaWithSchemaLocation() throws IOException {
    final long testId = generateTestId();
    String name = "schema" + testId;
    final String comment = "comment" + testId;
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;
    String schemaPath = catalogPath + "/" + name;
    Schema schema = createSchema(testId, name, comment, null, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.getFileStatus(schemaPath1).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);

    // test placeholder in schema location
    name = "schema" + testId + "_1";
    schemaPath = catalogPath + "/" + "{{schema}}";
    schema = createSchema(testId, name, comment, null, schemaPath);
    Assertions.assertEquals(name, schema.name());

    schemaPath1 = new Path(schemaPath);
    fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertFalse(fs.exists(schemaPath1));

    // test illegal placeholder in schema location
    String schemaName1 = "schema" + testId + "_2";
    String schemaPath2 = catalogPath + "/" + "{{}}";
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> createSchema(testId, schemaName1, comment, null, schemaPath2));
    Assertions.assertTrue(
        exception.getMessage().contains("Placeholder in location should not be empty"),
        exception.getMessage());

    // Test disable server-side FS operations.
    name = "schema" + testId + "_3";
    schemaPath = catalogPath + "/" + name;
    schema = createSchema(testId, name, comment, null, schemaPath, true);
    Assertions.assertEquals(name, schema.name());

    // Schema path should not be existed if the server-side FS operations are disabled.
    Assertions.assertFalse(fs.exists(new Path(schemaPath)));
  }

  @Test
  public void testCreateSchemaWithCatalogAndSchemaLocation() throws IOException {
    final long testId = generateTestId();
    String name = "schema" + testId;
    String comment = "comment" + testId;
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;
    String schemaPath = TEST_ROOT_PATH + "/" + "schema" + testId;
    Schema schema = createSchema(testId, name, comment, catalogPath, schemaPath);
    Assertions.assertEquals(name, schema.name());

    Path schemaPath1 = new Path(schemaPath);
    FileSystem fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertTrue(fs.exists(schemaPath1));
    Assertions.assertTrue(fs.getFileStatus(schemaPath1).isDirectory());
    Assertions.assertTrue(fs.listStatus(schemaPath1).length == 0);

    Assertions.assertFalse(fs.exists(new Path(catalogPath)));
    Assertions.assertFalse(fs.exists(new Path(catalogPath, name)));

    // test placeholder in location
    name = "schema" + testId + "_1";
    catalogPath = TEST_ROOT_PATH + "/" + "{{catalog}}";
    schemaPath = TEST_ROOT_PATH + "/" + "{{schema}}";
    schema = createSchema(testId, name, comment, catalogPath, schemaPath);
    Assertions.assertEquals(name, schema.name());

    schemaPath1 = new Path(schemaPath);
    fs = schemaPath1.getFileSystem(new Configuration());
    Assertions.assertFalse(fs.exists(schemaPath1));
    Assertions.assertFalse(fs.exists(new Path(catalogPath)));
    Assertions.assertFalse(fs.exists(new Path(catalogPath, name)));

    // Test disable server-side FS operations.
    name = "schema" + testId + "_2";
    catalogPath = TEST_ROOT_PATH + "/" + "catalog14_2";
    schemaPath = TEST_ROOT_PATH + "/" + "schema14_2";
    schema = createSchema(testId, name, comment, catalogPath, schemaPath, true);
    Assertions.assertEquals(name, schema.name());

    // Schema path should not be existed if the server-side FS operations are disabled.
    Assertions.assertFalse(fs.exists(new Path(catalogPath)));
    Assertions.assertFalse(fs.exists(new Path(schemaPath)));
  }

  @Test
  public void testLoadSchema() throws IOException {
    final long testId = generateTestId();
    String name = "schema" + testId;
    String comment = "comment" + testId;
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;
    Schema schema = createSchema(testId, name, comment, catalogPath, null);
    NameIdentifier otherSchema = NameIdentifierUtil.ofSchema("m1", "c1", "otherSchema");

    Assertions.assertEquals(name, schema.name());

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Schema schema1 = ops.loadSchema(NameIdentifierUtil.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      Throwable exception =
          Assertions.assertThrows(NoSuchSchemaException.class, () -> ops.loadSchema(otherSchema));
      Assertions.assertEquals("Schema m1.c1.otherSchema does not exist", exception.getMessage());
    }
  }

  @Test
  public void testListSchema() throws IOException {
    final long testId1 = generateTestId();
    final long testId2 = generateTestId();
    String name1 = "schema" + testId1;
    String comment1 = "comment" + testId1;
    String name2 = "schema" + testId2;
    String comment2 = "comment" + testId2;
    createSchema(testId1, name1, comment1, null, null);
    createSchema(testId2, name2, comment2, null, null);

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listSchemas(Namespace.of("m1", "c1"))).collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 2);
      Assertions.assertTrue(idents.contains(NameIdentifierUtil.ofSchema("m1", "c1", name1)));
      Assertions.assertTrue(idents.contains(NameIdentifierUtil.ofSchema("m1", "c1", name2)));
    }
  }

  @Test
  public void testAlterSchema() throws IOException {
    final long testId = generateTestId();
    String name = "schema" + testId;
    String comment = "comment" + testId;
    String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;
    Schema schema = createSchema(testId, name, comment, catalogPath, null);
    Assertions.assertEquals(name, schema.name());

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Schema schema1 = ops.loadSchema(NameIdentifierUtil.ofSchema("m1", "c1", name));
      Assertions.assertEquals(name, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      String newKey = "k1";
      String newValue = "v1";
      SchemaChange setProperty = SchemaChange.setProperty(newKey, newValue);
      Schema schema2 = ops.alterSchema(NameIdentifierUtil.ofSchema("m1", "c1", name), setProperty);
      Assertions.assertEquals(name, schema2.name());
      Assertions.assertEquals(comment, schema2.comment());
      Map<String, String> props2 = schema2.properties();
      Assertions.assertTrue(props2.containsKey(newKey));
      Assertions.assertEquals(newValue, props2.get(newKey));

      Schema schema3 = ops.loadSchema(NameIdentifierUtil.ofSchema("m1", "c1", name));
      Map<String, String> props3 = schema3.properties();
      Assertions.assertTrue(props3.containsKey(newKey));
      Assertions.assertEquals(newValue, props3.get(newKey));

      SchemaChange removeProperty = SchemaChange.removeProperty(newKey);
      Schema schema4 =
          ops.alterSchema(NameIdentifierUtil.ofSchema("m1", "c1", name), removeProperty);
      Assertions.assertEquals(name, schema4.name());
      Assertions.assertEquals(comment, schema4.comment());
      Map<String, String> props4 = schema4.properties();
      Assertions.assertFalse(props4.containsKey(newKey));

      Schema schema5 = ops.loadSchema(NameIdentifierUtil.ofSchema("m1", "c1", name));
      Map<String, String> props5 = schema5.properties();
      Assertions.assertFalse(props5.containsKey(newKey));
    }
  }

  @Test
  public void testDropSchema() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;

    Schema schema = createSchema(testId, schemaName, comment, catalogPath, null);
    Assertions.assertEquals(schemaName, schema.name());
    NameIdentifier id = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(
          ImmutableMap.of(LOCATION, catalogPath),
          randomCatalogInfo("m1", "c1"),
          HADOOP_PROPERTIES_METADATA);
      Schema schema1 = ops.loadSchema(id);
      Assertions.assertEquals(schemaName, schema1.name());
      Assertions.assertEquals(comment, schema1.comment());

      Map<String, String> props = schema1.properties();
      Assertions.assertTrue(props.containsKey(StringIdentifier.ID_KEY));

      ops.dropSchema(id, false);

      Path schemaPath = new Path(new Path(catalogPath), schemaName);
      FileSystem fs = schemaPath.getFileSystem(new Configuration());
      Assertions.assertFalse(fs.exists(schemaPath));

      // Test drop non-empty schema with cascade = false
      createSchema(testId, schemaName, comment, catalogPath, null);
      Fileset fs1 =
          createFileset("fs1", schemaName, "comment", Fileset.Type.MANAGED, catalogPath, null);
      Path fs1Path = new Path(fs1.storageLocation());

      Throwable exception1 =
          Assertions.assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(id, false));
      Assertions.assertEquals(
          "Schema m1.c1.schema" + testId + " is not empty", exception1.getMessage());

      // Test drop non-empty schema with cascade = true
      ops.dropSchema(id, true);
      Assertions.assertFalse(fs.exists(schemaPath));
      Assertions.assertFalse(fs.exists(fs1Path));

      // Test drop both managed and external filesets
      createSchema(testId, schemaName, comment, catalogPath, null);
      Fileset fs2 =
          createFileset("fs2", schemaName, "comment", Fileset.Type.MANAGED, catalogPath, null);
      Path fs2Path = new Path(fs2.storageLocation());

      Path fs3Path = new Path(schemaPath, "fs3");
      createFileset(
          "fs3", schemaName, "comment", Fileset.Type.EXTERNAL, catalogPath, fs3Path.toString());

      ops.dropSchema(id, true);
      Assertions.assertTrue(fs.exists(schemaPath));
      Assertions.assertFalse(fs.exists(fs2Path));
      // The path of external fileset should not be deleted
      Assertions.assertTrue(fs.exists(fs3Path));

      // Test drop schema with different storage location
      createSchema(testId, schemaName, comment, catalogPath, null);
      Path fs4Path = new Path(TEST_ROOT_PATH + "/fs4");
      createFileset(
          "fs4", schemaName, "comment", Fileset.Type.MANAGED, catalogPath, fs4Path.toString());
      ops.dropSchema(id, true);
      Assertions.assertFalse(fs.exists(fs4Path));
    }
  }

  @Test
  public void testDropSchemaWithFSOpsDisabled() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;
    final String catalogPath = TEST_ROOT_PATH + "/" + "catalog" + testId;

    Schema schema = createSchema(testId, schemaName, comment, catalogPath, null);
    Assertions.assertEquals(schemaName, schema.name());
    NameIdentifier id = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(
          ImmutableMap.of(LOCATION, catalogPath, DISABLE_FILESYSTEM_OPS, "true"),
          randomCatalogInfo("m1", "c1"),
          HADOOP_PROPERTIES_METADATA);

      ops.dropSchema(id, false);

      Path schemaPath = new Path(new Path(catalogPath), schemaName);
      FileSystem fs = schemaPath.getFileSystem(new Configuration());
      Assertions.assertTrue(fs.exists(schemaPath));

      createSchema(testId, schemaName, comment, catalogPath, null);
      Fileset fs1 =
          createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, catalogPath, null);
      Path fs1Path = new Path(fs1.storageLocation());

      // Test drop non-empty schema with cascade = true
      ops.dropSchema(id, true);
      Assertions.assertTrue(fs.exists(schemaPath));
      Assertions.assertTrue(fs.exists(fs1Path));
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
      catalogProps.put(LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(generateTestId(), schemaName, comment, catalogPath, schemaPath);
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
      // clean expected path if exist
      fs.delete(expectedPath, true);

      // Test drop non-existent fileset
      Assertions.assertFalse(ops.dropFileset(filesetIdent), "fileset should be non-existent");
    }
  }

  @ParameterizedTest
  @MethodSource("locationArguments")
  public void testCreateLoadAndDeleteFilesetWithLocationsWhenFSOpsDisabled(
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
    catalogProps.put(DISABLE_FILESYSTEM_OPS, "true");
    if (catalogPath != null) {
      catalogProps.put(LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(generateTestId(), schemaName, comment, catalogPath, schemaPath, true);
      }

      Fileset fileset;
      try {
        fileset =
            createFileset(name, schemaName, "comment", type, catalogPath, storageLocation, true);
      } catch (Exception e) {
        String locationPath =
            storageLocation != null
                ? storageLocation
                : schemaPath != null ? schemaPath : catalogPath;
        if (new Path(locationPath).toUri().getScheme() == null) {
          Assertions.assertInstanceOf(IllegalArgumentException.class, e);
          return;
        } else {
          throw e;
        }
      }

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());
      Assertions.assertEquals(expect, fileset.storageLocation());

      // Fileset storage location should not be existed.
      Path storagePath = new Path(fileset.storageLocation());
      FileSystem fs = storagePath.getFileSystem(new Configuration());
      Assertions.assertFalse(fs.exists(storagePath));

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
      Assertions.assertFalse(fs.exists(expectedPath));

      // clean expected path if exist
      fs.delete(expectedPath, true);

      // Test drop non-existent fileset
      Assertions.assertFalse(ops.dropFileset(filesetIdent), "fileset should be non-existent");
    }
  }

  @Test
  public void testCreateFilesetWithExceptions() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;

    createSchema(testId, schemaName, comment, null, null);
    NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);

    // If neither catalog location, nor schema location and storageLocation is specified.
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null));
    Assertions.assertEquals(
        "Storage location must be set for fileset "
            + filesetIdent
            + " when it's catalog and schema "
            + "location are not set",
        exception.getMessage());
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals(
          "Fileset m1.c1.schema" + testId + ".fileset" + testId + " does not exist",
          e.getMessage());
    }

    // For external fileset, if storageLocation is not specified.
    Throwable exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(filesetName, schemaName, comment, Fileset.Type.EXTERNAL, null, null));
    Assertions.assertEquals(
        "Storage location must be set for external fileset " + filesetIdent,
        exception1.getMessage());
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Throwable e =
          Assertions.assertThrows(
              NoSuchFilesetException.class, () -> ops.loadFileset(filesetIdent));
      Assertions.assertEquals("Fileset " + filesetIdent + " does not exist", e.getMessage());
    }
  }

  @Test
  public void testListFilesets() throws IOException {
    final long testId = generateTestId();
    String schemaName = "schema" + testId;
    String comment = "comment" + testId;
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;

    createSchema(testId, schemaName, comment, null, schemaPath);

    String[] filesets = {
      "fileset" + testId + "_1", "fileset" + testId + "_2", "fileset" + testId + "_3"
    };
    for (String fileset : filesets) {
      createFileset(fileset, schemaName, comment, Fileset.Type.MANAGED, null, null);
    }

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      Set<NameIdentifier> idents =
          Arrays.stream(ops.listFilesets(Namespace.of("m1", "c1", schemaName)))
              .collect(Collectors.toSet());
      Assertions.assertTrue(idents.size() >= 3);
      for (String fileset : filesets) {
        Assertions.assertTrue(idents.contains(NameIdentifier.of("m1", "c1", schemaName, fileset)));
      }
    }
  }

  @Test
  public void testListFilesetFiles() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    final NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);

    createSchema(testId, schemaName, comment, null, schemaPath);
    createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null);

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);

      Path testDir = new Path(schemaPath + "/" + filesetName);
      FileSystem fs = testDir.getFileSystem(new Configuration());
      fs.mkdirs(testDir);
      fs.create(new Path(testDir, "test_file1.txt")).close();
      fs.create(new Path(testDir, "test_file2.txt")).close();
      fs.mkdirs(new Path(testDir, "test_subdir"));

      FileInfo[] files = ops.listFiles(filesetIdent, null, "/");

      Assertions.assertNotNull(files);
      Assertions.assertTrue(files.length >= 3);

      Set<String> fileNames = Arrays.stream(files).map(FileInfo::name).collect(Collectors.toSet());

      Assertions.assertTrue(fileNames.contains("test_file1.txt"));
      Assertions.assertTrue(fileNames.contains("test_file2.txt"));
      Assertions.assertTrue(fileNames.contains("test_subdir"));

      for (FileInfo file : files) {
        // verify file type related properties
        if (file.name().equals("test_file1.txt") || file.name().equals("test_file2.txt")) {
          Assertions.assertFalse(file.isDir(), "File should not be directory: " + file.name());
          Assertions.assertTrue(file.size() >= 0, "File size should be non-negative");
        } else if (file.name().equals("test_subdir")) {
          Assertions.assertTrue(file.isDir(), "Directory should be marked as directory");
          Assertions.assertEquals(0, file.size(), "Directory size should be 0");
        }
        // verify other properties
        Assertions.assertNotNull(file.name(), "File name should not be null");
        Assertions.assertNotNull(file.path(), "File path should not be null");
        Assertions.assertTrue(file.lastModified() > 0, "Last modified time should be positive");
      }
    }
  }

  @Test
  public void testListFilesetFilesWithFSOpsDisabled() throws Exception {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    final NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);

    createSchema(testId, schemaName, comment, null, schemaPath);
    createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null);

    Map<String, String> catalogProps = Collections.singletonMap(DISABLE_FILESYSTEM_OPS, "true");

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      UnsupportedOperationException ex =
          Assertions.assertThrows(
              UnsupportedOperationException.class,
              () -> ops.listFiles(filesetIdent, null, "/"),
              "Expected listFiles to throw UnsupportedOperationException when disableFSOps is true");
      Assertions.assertTrue(
          ex.getMessage().contains("Filesystem operations are disabled on this server"),
          "Exception message should mention 'Filesystem operations are disabled on this server'");
    }
  }

  @Test
  public void testListFilesetFilesWithNonExistentPath() throws IOException {
    final long testId = generateTestId();
    String schemaName = "schema" + testId;
    String comment = "comment" + testId;
    String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    String filesetName = "fileset" + testId;
    final String nonExistentSubPath = "/non_existent_file.txt";

    Schema schema = createSchema(testId, schemaName, comment, null, schemaPath);
    Fileset fileset =
        createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null);
    final NameIdentifier filesetIdent =
        NameIdentifier.of("m1", "c1", schema.name(), fileset.name());

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> ops.listFiles(filesetIdent, null, nonExistentSubPath),
              "Listing a non-existent fileset directory should throw IllegalArgumentException");

      Assertions.assertTrue(
          ex.getMessage().contains("does not exist"),
          "Exception message should mention that the path does not exist");
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
      catalogProps.put(LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(generateTestId(), schemaName, comment, catalogPath, schemaPath);
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
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;

    createSchema(testId, schemaName, comment, null, schemaPath);
    Fileset fileset =
        createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null);

    FilesetChange change1 = FilesetChange.setProperty("k1", "v1");
    FilesetChange change2 = FilesetChange.removeProperty("k1");

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(filesetName, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals(comment, fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
      Map<String, String> props1 = fileset1.properties();
      Assertions.assertTrue(props1.containsKey("k1"));
      Assertions.assertEquals("v1", props1.get("k1"));

      Fileset fileset2 = ops.alterFileset(filesetIdent, change2);
      Assertions.assertEquals(filesetName, fileset2.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type());
      Assertions.assertEquals(comment, fileset2.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset2.storageLocation());
      Map<String, String> props2 = fileset2.properties();
      Assertions.assertFalse(props2.containsKey("k1"));
    }
  }

  @Test
  public void testFormalizePath() throws IOException, IllegalAccessException {

    String[] paths =
        new String[] {"tmp/catalog", "/tmp/catalog", "file:/tmp/catalog", "file:///tmp/catalog"};

    String[] expected =
        new String[] {
          "file:" + Paths.get("").toAbsolutePath() + "/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog",
          "file:/tmp/catalog"
        };

    HasPropertyMetadata hasPropertyMetadata =
        new HasPropertyMetadata() {
          @Override
          public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
            return null;
          }

          @Override
          public PropertiesMetadata catalogPropertiesMetadata()
              throws UnsupportedOperationException {
            return new PropertiesMetadata() {
              @Override
              public Map<String, PropertyEntry<?>> propertyEntries() {
                return new HadoopCatalogPropertiesMetadata().propertyEntries();
              }
            };
          }

          @Override
          public PropertiesMetadata schemaPropertiesMetadata()
              throws UnsupportedOperationException {
            return null;
          }

          @Override
          public PropertiesMetadata filesetPropertiesMetadata()
              throws UnsupportedOperationException {
            return null;
          }

          @Override
          public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
            return null;
          }

          @Override
          public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
            return null;
          }
        };

    try {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "entityStore", new RelationalEntityStore(), true);
      try (HadoopCatalogOperations hadoopCatalogOperations = new HadoopCatalogOperations()) {
        Map<String, String> map = ImmutableMap.of("default-filesystem", "file:///");
        hadoopCatalogOperations.initialize(map, null, hasPropertyMetadata);
        for (int i = 0; i < paths.length; i++) {
          Path actual = hadoopCatalogOperations.formalizePath(new Path(paths[i]), map);
          Assertions.assertEquals(expected[i], actual.toString());
        }
      }
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", null, true);
    }
  }

  @Test
  public void testUpdateFilesetComment() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String name = "fileset" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;

    createSchema(testId, schemaName, comment, null, schemaPath);
    Fileset fileset = createFileset(name, schemaName, comment, Fileset.Type.MANAGED, null, null);

    FilesetChange change1 = FilesetChange.updateComment(comment + "_new");
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(name, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertEquals(comment + "_new", fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    }
  }

  @Test
  public void testRemoveFilesetComment() throws IOException {
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String filesetName = "fileset" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;

    createSchema(testId, schemaName, comment, null, schemaPath);
    Fileset fileset =
        createFileset(filesetName, schemaName, comment, Fileset.Type.MANAGED, null, null);

    FilesetChange change1 = FilesetChange.updateComment(null);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);

      Fileset fileset1 = ops.alterFileset(filesetIdent, change1);
      Assertions.assertEquals(filesetName, fileset1.name());
      Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
      Assertions.assertNull(fileset1.comment());
      Assertions.assertEquals(fileset.storageLocation(), fileset1.storageLocation());
    }
  }

  @Test
  public void testTestConnection() {
    SecureHadoopCatalogOperations catalogOperations = new SecureHadoopCatalogOperations(store);
    Assertions.assertDoesNotThrow(
        () ->
            catalogOperations.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.FILESET,
                "hadoop",
                "comment",
                ImmutableMap.of()));
  }

  @Test
  void testTrailSlash() throws IOException {
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {

      String location = "hdfs://localhost:9000";
      Map<String, String> catalogProperties = Maps.newHashMap();
      catalogProperties.put(LOCATION, location);

      ops.initialize(catalogProperties, randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);

      String schemaName = "schema1024";
      NameIdentifier nameIdentifier = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);

      Map<String, String> schemaProperties = Maps.newHashMap();
      schemaProperties.put(LOCATION, "hdfs://localhost:9000/user1");
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      schemaProperties =
          Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, schemaProperties));

      Map<String, String> finalSchemaProperties = schemaProperties;

      // If not fixed by #5296, this method will throw java.lang.IllegalArgumentException:
      // java.net.URISyntaxException: Relative path in absolute URI: hdfs://localhost:9000schema1024
      // After #5296, this method will throw java.lang.RuntimeException: Failed to create
      // schema m1.c1.schema1024 location hdfs://localhost:9000/user1
      Exception exception =
          Assertions.assertThrows(
              Exception.class,
              () -> ops.createSchema(nameIdentifier, "comment", finalSchemaProperties));
      Assertions.assertTrue(exception.getCause() instanceof ConnectException);
    }
  }

  @Test
  public void testGetFileLocation() throws IOException {
    final long testId = generateTestId();
    final String catalogName = "catalog" + testId;
    final String schemaName = "schema" + testId;
    final String comment = "comment" + testId;
    final String schemaPath = TEST_ROOT_PATH + "/" + schemaName;
    final String filesetName = "fileset" + testId;
    final String storageLocation =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName;

    createSchema(testId, schemaName, comment, null, schemaPath);
    Fileset fileset =
        createFileset(
            filesetName, schemaName, comment, Fileset.Type.MANAGED, null, storageLocation);

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName);
      // test sub path starts with "/"
      String subPath1 = "/test/test.parquet";
      String fileLocation1 = ops.getFileLocation(filesetIdent, subPath1);
      Assertions.assertEquals(
          String.format("%s%s", fileset.storageLocation(), subPath1), fileLocation1);

      // test sub path not starts with "/"
      String subPath2 = "test/test.parquet";
      String fileLocation2 = ops.getFileLocation(filesetIdent, subPath2);
      Assertions.assertEquals(
          String.format("%s/%s", fileset.storageLocation(), subPath2), fileLocation2);

      // test sub path is null
      String subPath3 = null;
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> ops.getFileLocation(filesetIdent, subPath3));

      // test sub path is blank but not null
      String subPath4 = "";
      String fileLocation3 = ops.getFileLocation(filesetIdent, subPath4);
      Assertions.assertEquals(fileset.storageLocation(), fileLocation3);
    }

    // test mount a single file
    final String filesetName2 = "test_get_file_location_2";
    final String filesetLocation2 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName2;
    Path filesetLocationPath2 = new Path(filesetLocation2);
    createFileset(filesetName2, schemaName, comment, Fileset.Type.MANAGED, null, filesetLocation2);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store);
        FileSystem localFileSystem = filesetLocationPath2.getFileSystem(new Configuration())) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName2);
      // replace fileset location to a single file
      Assertions.assertTrue(localFileSystem.exists(filesetLocationPath2));
      Assertions.assertTrue(localFileSystem.getFileStatus(filesetLocationPath2).isDirectory());
      localFileSystem.delete(filesetLocationPath2, true);
      localFileSystem.create(filesetLocationPath2);
      Assertions.assertTrue(localFileSystem.exists(filesetLocationPath2));
      Assertions.assertTrue(localFileSystem.getFileStatus(filesetLocationPath2).isFile());

      String subPath = "/year=2024/month=07/day=22/test.parquet";
      Map<String, String> contextMap = Maps.newHashMap();
      contextMap.put(
          FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
          FilesetDataOperation.RENAME.name());
      CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
      CallerContext.CallerContextHolder.set(callerContext);

      Assertions.assertThrows(
          GravitinoRuntimeException.class, () -> ops.getFileLocation(filesetIdent, subPath));
    } finally {
      CallerContext.CallerContextHolder.remove();
    }

    // test rename with an empty subPath
    String filesetName3 = "test_get_file_location_3";
    String filesetLocation3 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName3;
    Path filesetLocationPath3 = new Path(filesetLocation3);
    createFileset(filesetName3, schemaName, comment, Fileset.Type.MANAGED, null, filesetLocation3);
    try (HadoopCatalogOperations ops = new HadoopCatalogOperations(store);
        FileSystem localFileSystem = filesetLocationPath3.getFileSystem(new Configuration())) {
      ops.initialize(Maps.newHashMap(), randomCatalogInfo(), HADOOP_PROPERTIES_METADATA);
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName3);
      // replace fileset location to a single file
      Assertions.assertTrue(localFileSystem.exists(filesetLocationPath3));
      Assertions.assertTrue(localFileSystem.getFileStatus(filesetLocationPath3).isDirectory());
      localFileSystem.delete(filesetLocationPath3, true);
      localFileSystem.create(filesetLocationPath3);
      Assertions.assertTrue(localFileSystem.exists(filesetLocationPath3));
      Assertions.assertTrue(localFileSystem.getFileStatus(filesetLocationPath3).isFile());

      Map<String, String> contextMap = Maps.newHashMap();
      contextMap.put(
          FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
          FilesetDataOperation.RENAME.name());
      CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
      CallerContext.CallerContextHolder.set(callerContext);
      Assertions.assertThrows(
          GravitinoRuntimeException.class, () -> ops.getFileLocation(filesetIdent, ""));
    }

    // test storage location end with "/"
    String filesetName4 = "test_get_file_location_4";
    String filesetLocation4 =
        TEST_ROOT_PATH + "/" + catalogName + "/" + schemaName + "/" + filesetName4 + "/";
    NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, filesetName4);
    Fileset mockFileset = Mockito.mock(Fileset.class);
    when(mockFileset.name()).thenReturn(filesetName4);
    when(mockFileset.storageLocation()).thenReturn(filesetLocation4);
    when(mockFileset.storageLocations())
        .thenReturn(ImmutableMap.of(LOCATION_NAME_UNKNOWN, filesetLocation4));
    when(mockFileset.properties())
        .thenReturn(ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, LOCATION_NAME_UNKNOWN));

    try (HadoopCatalogOperations mockOps = Mockito.mock(HadoopCatalogOperations.class)) {
      mockOps.hadoopConf = new Configuration();
      when(mockOps.loadFileset(filesetIdent)).thenReturn(mockFileset);
      when(mockOps.getConf()).thenReturn(Maps.newHashMap());
      String subPath = "/test/test.parquet";
      when(mockOps.getFileLocation(filesetIdent, subPath)).thenCallRealMethod();
      when(mockOps.getFileLocation(filesetIdent, subPath, null)).thenCallRealMethod();
      when(mockOps.getFileSystem(Mockito.any(), Mockito.any()))
          .thenReturn(FileSystem.getLocal(new Configuration()));
      String fileLocation = mockOps.getFileLocation(filesetIdent, subPath);
      Assertions.assertEquals(
          String.format("%s%s", mockFileset.storageLocation(), subPath.substring(1)), fileLocation);
    }
  }

  @Test
  public void testLocationPlaceholdersWithException() throws IOException {
    // test empty placeholder value
    final long testId = generateTestId();
    final String schemaName = "schema" + testId;
    final String filesetName = "fileset" + testId;
    String storageLocation = TEST_ROOT_PATH + "/{{fileset}}/{{user}}/{{id}}";

    createSchema(testId, schemaName, null, null, null);

    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(
                    filesetName,
                    schemaName,
                    "comment",
                    Fileset.Type.MANAGED,
                    null,
                    storageLocation,
                    ImmutableMap.of("user", "tom")));
    Assertions.assertEquals("No value found for placeholder: user", exception.getMessage());

    // test placeholder value not found
    String storageLocation1 = TEST_ROOT_PATH + "/{{fileset}}/{{}}";
    Exception exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createFileset(
                    filesetName,
                    schemaName,
                    "comment",
                    Fileset.Type.MANAGED,
                    null,
                    storageLocation1,
                    ImmutableMap.of("user", "tom")));
    Assertions.assertEquals(
        "Placeholder in location should not be empty, location: " + storageLocation1,
        exception1.getMessage());
  }

  @ParameterizedTest
  @MethodSource("locationWithPlaceholdersArguments")
  public void testPlaceholdersInLocation(
      String name,
      Fileset.Type type,
      String catalogPath,
      String schemaPath,
      String storageLocation,
      Map<String, String> placeholders,
      String expect)
      throws IOException {
    String schemaName = "s1_" + name;
    String comment = "comment_s1";
    Map<String, String> catalogProps = Maps.newHashMap();
    if (catalogPath != null) {
      catalogProps.put(LOCATION, catalogPath);
    }

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogProps, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      if (!ops.schemaExists(schemaIdent)) {
        createSchema(generateTestId(), schemaName, comment, catalogPath, schemaPath);
      }
      Fileset fileset =
          createFileset(
              name, schemaName, "comment", type, catalogPath, storageLocation, placeholders);

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());
      Assertions.assertEquals(expect, fileset.storageLocation());
      placeholders.forEach((k, v) -> Assertions.assertEquals(fileset.properties().get(k), v));

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
      // clean expected path if exist
      fs.delete(expectedPath, true);

      // Test drop non-existent fileset
      Assertions.assertFalse(ops.dropFileset(filesetIdent), "fileset should be non-existent");
    }
  }

  @ParameterizedTest
  @MethodSource("multipleLocationsArguments")
  public void testMultipleLocations(
      String name,
      Fileset.Type type,
      Map<String, String> catalogPaths,
      Map<String, String> schemaPaths,
      Map<String, String> storageLocations,
      Map<String, String> filesetProps,
      Map<String, String> expect)
      throws IOException {
    String schemaName = "s1_" + name;
    String comment = "comment_s1";

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", schemaName);
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogPaths, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      if (!ops.schemaExists(schemaIdent)) {
        createMultiLocationSchema(schemaName, comment, catalogPaths, schemaPaths);
      }
      Fileset fileset =
          createMultiLocationFileset(
              name, schemaName, "comment", type, catalogPaths, storageLocations, filesetProps);

      Assertions.assertEquals(name, fileset.name());
      Assertions.assertEquals(type, fileset.type());
      Assertions.assertEquals("comment", fileset.comment());
      Assertions.assertEquals(expect, fileset.storageLocations());
      Assertions.assertEquals(
          fileset.storageLocation(), fileset.storageLocations().get(LOCATION_NAME_UNKNOWN));
      Assertions.assertNotNull(fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
      if (filesetProps != null && filesetProps.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)) {
        Assertions.assertEquals(
            filesetProps.get(PROPERTY_DEFAULT_LOCATION_NAME),
            fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
      }
      if (filesetProps == null || !filesetProps.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)) {}

      // Test load
      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Fileset loadedFileset = ops.loadFileset(filesetIdent);
      Assertions.assertEquals(name, loadedFileset.name());
      Assertions.assertEquals(type, loadedFileset.type());
      Assertions.assertEquals("comment", loadedFileset.comment());
      Assertions.assertEquals(expect, loadedFileset.storageLocations());
      Assertions.assertEquals(
          loadedFileset.storageLocation(),
          loadedFileset.storageLocations().get(LOCATION_NAME_UNKNOWN));

      // Test drop
      ops.dropFileset(filesetIdent);
      for (Map.Entry<String, String> location : expect.entrySet()) {
        Path expectedPath = new Path(location.getValue());
        FileSystem fs = expectedPath.getFileSystem(new Configuration());
        if (type == Fileset.Type.MANAGED) {
          Assertions.assertFalse(fs.exists(expectedPath));
        } else {
          Assertions.assertTrue(
              fs.exists(expectedPath),
              "location with name "
                  + location.getKey()
                  + " should exist, path: "
                  + location.getValue());
        }
      }

      // clean expected path if exist
      try (FileSystem fs = FileSystem.newInstance(new Configuration())) {
        for (String location : expect.values()) {
          fs.delete(new Path(location), true);
        }
      }

      // Test drop non-existent fileset
      Assertions.assertFalse(ops.dropFileset(filesetIdent), "fileset should be non-existent");
    }
  }

  @Test
  public void testCreateMultipleLocationsWithExceptions() throws IOException {
    // empty location name in catalog location
    Map<String, String> illegalLocations =
        ImmutableMap.of(PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "", TEST_ROOT_PATH + "/catalog31_1");
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      Exception exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  ops.initialize(
                      illegalLocations, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA));
      Assertions.assertEquals("Location name must not be blank", exception.getMessage());

      // empty location name in schema location
      ops.initialize(ImmutableMap.of(), randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createMultiLocationSchema("s1", "comment", ImmutableMap.of(), illegalLocations));
      Assertions.assertEquals("Location name must not be blank", exception.getMessage());

      // empty location name in storage location
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createMultiLocationFileset(
                      "fileset_test",
                      "s1",
                      null,
                      Fileset.Type.MANAGED,
                      ImmutableMap.of(),
                      ImmutableMap.of("", TEST_ROOT_PATH + "/fileset31"),
                      null));
      Assertions.assertEquals("Location name must not be blank", exception.getMessage());

      // empty location in catalog location
      Map<String, String> illegalLocations2 =
          new HashMap<String, String>() {
            {
              put(PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1", null);
            }
          };
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  ops.initialize(
                      illegalLocations2,
                      randomCatalogInfo("m1", "c1"),
                      HADOOP_PROPERTIES_METADATA));
      Assertions.assertEquals(
          "Location value must not be blank for location name: v1", exception.getMessage());

      // empty location in schema location
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createMultiLocationSchema(
                      "s1", "comment", ImmutableMap.of(), ImmutableMap.of("location", "")));
      Assertions.assertEquals(
          "The value of the schema property location must not be blank", exception.getMessage());

      // empty fileset storage location
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createMultiLocationFileset(
                      "fileset_test",
                      "s1",
                      null,
                      Fileset.Type.MANAGED,
                      ImmutableMap.of(),
                      ImmutableMap.of("location1", ""),
                      null));
      Assertions.assertEquals(
          "Storage location must not be blank for location name: location1",
          exception.getMessage());

      // storage location is parent of schema location
      Schema multipLocationSchema =
          createMultiLocationSchema(
              "s1",
              "comment",
              ImmutableMap.of(),
              ImmutableMap.of(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1", TEST_ROOT_PATH + "/s1/a/b/c"));
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createMultiLocationFileset(
                      "fileset_test",
                      multipLocationSchema.name(),
                      null,
                      Fileset.Type.MANAGED,
                      ImmutableMap.of(),
                      ImmutableMap.of(LOCATION_NAME_UNKNOWN, TEST_ROOT_PATH + "/s1/a"),
                      null));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains(
                  "Default location name must be set and must be one of the fileset locations"),
          "Exception message: " + exception.getMessage());
    }
  }

  @Test
  public void testGetTargetLocation() throws IOException {
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(
          Collections.emptyMap(), randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      Fileset fileset = Mockito.mock(Fileset.class);
      when(fileset.name()).thenReturn("fileset_single_location");
      when(fileset.storageLocations())
          .thenReturn(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "file://a/b/c"));
      when(fileset.properties())
          .thenReturn(ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, LOCATION_NAME_UNKNOWN));
      Assertions.assertEquals("file://a/b/c", ops.getTargetLocation(fileset));

      try (MockedStatic<CallerContext.CallerContextHolder> callerContextHolder =
          mockStatic(CallerContext.CallerContextHolder.class)) {
        CallerContext callerContext = Mockito.mock(CallerContext.class);
        when(callerContext.context())
            .thenReturn(
                ImmutableMap.of(
                    CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME, LOCATION_NAME_UNKNOWN));
        callerContextHolder.when(CallerContext.CallerContextHolder::get).thenReturn(callerContext);
        Assertions.assertEquals("file://a/b/c", ops.getTargetLocation(fileset));
      }

      Fileset filesetWithMultipleLocation = Mockito.mock(Fileset.class);
      when(filesetWithMultipleLocation.name()).thenReturn("fileset_multiple_location");
      when(filesetWithMultipleLocation.storageLocations())
          .thenReturn(
              ImmutableMap.of(
                  LOCATION_NAME_UNKNOWN,
                  "file://a/b/c",
                  "location_1",
                  "file://a/b/d",
                  "location_2",
                  "file://a/b/e"));
      when(filesetWithMultipleLocation.properties())
          .thenReturn(ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "location_1"));
      Assertions.assertEquals("file://a/b/d", ops.getTargetLocation(filesetWithMultipleLocation));

      try (MockedStatic<CallerContext.CallerContextHolder> callerContextHolder =
          mockStatic(CallerContext.CallerContextHolder.class)) {
        CallerContext callerContext = Mockito.mock(CallerContext.class);
        when(callerContext.context())
            .thenReturn(
                ImmutableMap.of(
                    CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME, "location_2"));
        callerContextHolder.when(CallerContext.CallerContextHolder::get).thenReturn(callerContext);
        Assertions.assertEquals("file://a/b/e", ops.getTargetLocation(filesetWithMultipleLocation));
      }
    }
  }

  private static Stream<Arguments> multipleLocationsArguments() {
    return Stream.of(
        // Honor the catalog location
        Arguments.of(
            "fileset51",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/catalog31_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog31_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog31_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/catalog31_1/s1_fileset51/fileset51",
                "v1",
                TEST_ROOT_PATH + "/catalog31_1/s1_fileset51/fileset51",
                "v2",
                TEST_ROOT_PATH + "/catalog31_2/s1_fileset51/fileset51")),
        Arguments.of(
            // honor the schema location
            "fileset52",
            Fileset.Type.MANAGED,
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/s1_fileset52_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/s1_fileset52_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset52_2/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v2"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/s1_fileset52_1/fileset52",
                "v1",
                TEST_ROOT_PATH + "/s1_fileset52_1/fileset52",
                "v2",
                TEST_ROOT_PATH + "/s1_fileset52_2/fileset52")),
        Arguments.of(
            // honor the schema location
            "fileset53",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/catalog32_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog32_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog32_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/s1_fileset53_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/s1_fileset53_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset53_2/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "unknown"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/s1_fileset53_1/fileset53",
                "v1",
                TEST_ROOT_PATH + "/s1_fileset53_1/fileset53",
                "v2",
                TEST_ROOT_PATH + "/s1_fileset53_2/fileset53")),
        Arguments.of(
            // honor the storage location
            "fileset54",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/catalog33_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog33_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog33_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/s1_fileset54_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/s1_fileset54_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset54_2/{{fileset}}"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset54_1",
                "v1",
                TEST_ROOT_PATH + "/fileset54_1",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset54_1",
                "v1",
                TEST_ROOT_PATH + "/fileset54_1",
                "v2",
                TEST_ROOT_PATH + "/fileset54_2")),
        Arguments.of(
            // honor the storage location
            "fileset55",
            Fileset.Type.MANAGED,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset55_1",
                "v1",
                TEST_ROOT_PATH + "/fileset55_1",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset55_1",
                "v1",
                TEST_ROOT_PATH + "/fileset55_1",
                "v2",
                TEST_ROOT_PATH + "/fileset55_2")),
        Arguments.of(
            // honor the storage location
            "fileset56",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/catalog34_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog34_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog34_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset56_1",
                "v1",
                TEST_ROOT_PATH + "/fileset56_1",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset56_1",
                "v1",
                TEST_ROOT_PATH + "/fileset56_1",
                "v2",
                TEST_ROOT_PATH + "/fileset56_2")),
        Arguments.of(
            // honor partial catalog/schema/fileset locations
            "fileset510",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog34_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog34_2",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v3",
                TEST_ROOT_PATH + "/catalog34_3"),
            ImmutableMap.of(
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset510_2",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v3",
                TEST_ROOT_PATH + "/s1_{{fileset}}_3",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v4",
                TEST_ROOT_PATH + "/s1_fileset510_4"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset510",
                "v3",
                TEST_ROOT_PATH + "/fileset510_3",
                "v4",
                TEST_ROOT_PATH + "/fileset510_4",
                "v5",
                TEST_ROOT_PATH + "/{{fileset}}_5"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v5"),
            ImmutableMap.<String, String>builder()
                .put(LOCATION_NAME_UNKNOWN, TEST_ROOT_PATH + "/fileset510")
                .put("v1", TEST_ROOT_PATH + "/catalog34_1/s1_fileset510/fileset510")
                .put("v2", TEST_ROOT_PATH + "/s1_fileset510_2/fileset510")
                .put("v3", TEST_ROOT_PATH + "/fileset510_3")
                .put("v4", TEST_ROOT_PATH + "/fileset510_4")
                .put("v5", TEST_ROOT_PATH + "/fileset510_5")
                .build()),
        Arguments.of(
            // test without unnamed storage location
            "fileset511",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog34_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog34_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(
                "v1", TEST_ROOT_PATH + "/fileset511_1", "v2", TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                "v1", TEST_ROOT_PATH + "/fileset511_1", "v2", TEST_ROOT_PATH + "/fileset511_2")),
        Arguments.of(
            // test single location without unnamed storage location
            "fileset512",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog34_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of("v1", TEST_ROOT_PATH + "/{{fileset}}_2"),
            null,
            ImmutableMap.of("v1", TEST_ROOT_PATH + "/fileset512_2")),
        Arguments.of(
            // honor the storage location
            "fileset57",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/catalog35_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/catalog35_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/catalog35_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/s1_fileset57_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/s1_fileset57_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset57_2/{{fileset}}"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset57_1",
                "v1",
                TEST_ROOT_PATH + "/fileset57_1",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset57_1",
                "v1",
                TEST_ROOT_PATH + "/fileset57_1",
                "v2",
                TEST_ROOT_PATH + "/fileset57_2")),
        Arguments.of(
            // honor the storage location
            "fileset58",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION,
                TEST_ROOT_PATH + "/s1_fileset58_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                TEST_ROOT_PATH + "/s1_fileset58_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                TEST_ROOT_PATH + "/s1_fileset58_2/{{fileset}}"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset58_1",
                "v1",
                TEST_ROOT_PATH + "/fileset58_1",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset58_1",
                "v1",
                TEST_ROOT_PATH + "/fileset58_1",
                "v2",
                TEST_ROOT_PATH + "/fileset58_2")),
        Arguments.of(
            // honor the storage location
            "fileset59",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset59",
                "v1",
                TEST_ROOT_PATH + "/fileset59",
                "v2",
                TEST_ROOT_PATH + "/{{fileset}}"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset59",
                "v1",
                TEST_ROOT_PATH + "/fileset59",
                "v2",
                TEST_ROOT_PATH + "/fileset59")),
        // Honor the catalog location
        Arguments.of(
            "fileset501",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog301_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog301_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog301_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/catalog301_1/s1_fileset501/fileset501",
                "v1",
                TEST_ROOT_PATH + "/catalog301_1/s1_fileset501/fileset501",
                "v2",
                TEST_ROOT_PATH + "/catalog301_2/s1_fileset501/fileset501")),
        Arguments.of(
            // honor the schema location
            "fileset502",
            Fileset.Type.MANAGED,
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset502_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset502_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset502_2/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/s1_fileset502_1/fileset502",
                "v1",
                TEST_ROOT_PATH + "/s1_fileset502_1/fileset502",
                "v2",
                TEST_ROOT_PATH + "/s1_fileset502_2/fileset502")),
        Arguments.of(
            // honor the schema location
            "fileset503",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog302_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog302_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog302_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset503_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset503_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset503_2/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/s1_fileset503_1/fileset503",
                "v1",
                TEST_ROOT_PATH + "/s1_fileset503_1/fileset503",
                "v2",
                TEST_ROOT_PATH + "/s1_fileset503_2/fileset503")),
        Arguments.of(
            // honor the storage location
            "fileset504",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog303_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog303_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog303_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset504_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset504_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset504_2/{{fileset}}"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset504_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset504_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset504_1",
                "v1",
                TEST_ROOT_PATH + "/fileset504_1",
                "v2",
                TEST_ROOT_PATH + "/fileset504_2")),
        Arguments.of(
            // honor the storage location
            "fileset505",
            Fileset.Type.MANAGED,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset505_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset505_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset505_1",
                "v1",
                TEST_ROOT_PATH + "/fileset505_1",
                "v2",
                TEST_ROOT_PATH + "/fileset505_2")),
        Arguments.of(
            // honor the storage location
            "fileset506",
            Fileset.Type.MANAGED,
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog304_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog304_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog304_2/{{schema}}/{{fileset}}"),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset506_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset506_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset506_1",
                "v1",
                TEST_ROOT_PATH + "/fileset506_1",
                "v2",
                TEST_ROOT_PATH + "/fileset506_2")),
        Arguments.of(
            // honor the storage location
            "fileset507",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog305_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog305_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/catalog305_2"),
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset507_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset507_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset507_2"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset507_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset507_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset507_1",
                "v1",
                TEST_ROOT_PATH + "/fileset507_1",
                "v2",
                TEST_ROOT_PATH + "/fileset507_2")),
        Arguments.of(
            // honor the storage location
            "fileset508",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION,
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset508_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset508_1",
                PROPERTY_MULTIPLE_LOCATIONS_PREFIX + "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset508_2"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset508_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset508_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset508_1",
                "v1",
                TEST_ROOT_PATH + "/fileset508_1",
                "v2",
                TEST_ROOT_PATH + "/fileset508_2")),
        Arguments.of(
            // honor the storage location
            "fileset509",
            Fileset.Type.EXTERNAL,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset509_1",
                "v1",
                UNFORMALIZED_TEST_ROOT_PATH + "/fileset509_1",
                "v2",
                UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}_2"),
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "v1"),
            ImmutableMap.of(
                LOCATION_NAME_UNKNOWN,
                TEST_ROOT_PATH + "/fileset509_1",
                "v1",
                TEST_ROOT_PATH + "/fileset509_1",
                "v2",
                TEST_ROOT_PATH + "/fileset509_2")));
  }

  private static Stream<Arguments> locationWithPlaceholdersArguments() {
    return Stream.of(
        // placeholders in catalog location
        Arguments.of(
            "fileset41",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            null,
            null,
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset41-fileset41/workspace/tom"),
        // placeholders in schema location
        Arguments.of(
            "fileset42",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            null,
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset42-fileset42/workspace/tom"),
        // placeholders in schema location
        Arguments.of(
            "fileset43",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/{{catalog}}",
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            null,
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset43-fileset43/workspace/tom"),
        // placeholders in storage location
        Arguments.of(
            "fileset44",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/{{catalog}}",
            TEST_ROOT_PATH + "/{{schema}}",
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset44-fileset44/workspace/tom"),
        // placeholders in storage location
        Arguments.of(
            "fileset45",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset45-fileset45/workspace/tom"),
        // placeholders in storage location
        Arguments.of(
            "fileset46",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/{{catalog}}",
            null,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "tom"),
            TEST_ROOT_PATH + "/c1-s1_fileset46-fileset46/workspace/tom"),
        // placeholders in storage location
        Arguments.of(
            "fileset47",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/{{catalog}}",
            TEST_ROOT_PATH + "/{{schema}}",
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "jack"),
            TEST_ROOT_PATH + "/c1-s1_fileset47-fileset47/workspace/jack"),
        // placeholders in storage location
        Arguments.of(
            "fileset48",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/{{catalog}}",
            null,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "jack"),
            TEST_ROOT_PATH + "/c1-s1_fileset48-fileset48/workspace/jack"),
        // placeholders in storage location
        Arguments.of(
            "fileset49",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/{{catalog}}-{{schema}}-{{fileset}}/workspace/{{user}}",
            ImmutableMap.of("placeholder-user", "jack"),
            TEST_ROOT_PATH + "/c1-s1_fileset49-fileset49/workspace/jack"));
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
            // honor the catalog location with placeholder
            "fileset11",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog21/{{schema}}/{{fileset}}",
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
            // honor the schema location with placeholder
            "fileset12",
            Fileset.Type.MANAGED,
            null,
            TEST_ROOT_PATH + "/{{schema}}/{{fileset}}",
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
            // honor the schema location with placeholder
            "fileset13",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog22",
            TEST_ROOT_PATH + "/{{schema}}/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset14",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog23",
            TEST_ROOT_PATH + "/{{schema}}",
            TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset15",
            Fileset.Type.MANAGED,
            null,
            null,
            TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset16",
            Fileset.Type.MANAGED,
            TEST_ROOT_PATH + "/catalog24",
            null,
            TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset17",
            Fileset.Type.EXTERNAL,
            TEST_ROOT_PATH + "/catalog25",
            TEST_ROOT_PATH + "/s1_fileset17",
            TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset18",
            Fileset.Type.EXTERNAL,
            null,
            TEST_ROOT_PATH + "/s1_fileset18",
            TEST_ROOT_PATH + "/{{fileset}}",
            TEST_ROOT_PATH + "/fileset18"),
        Arguments.of(
            // honor the storage location
            "fileset19",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/fileset19",
            TEST_ROOT_PATH + "/fileset19"),
        Arguments.of(
            // honor the storage location with placeholder
            "fileset19",
            Fileset.Type.EXTERNAL,
            null,
            null,
            TEST_ROOT_PATH + "/{{fileset}}",
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
            // Honor the catalog location with placeholder
            "fileset101",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog201/{{schema}}/{{fileset}}",
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
            // honor the schema location with placeholder
            "fileset102",
            Fileset.Type.MANAGED,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset102/{{fileset}}",
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
            // honor the schema location with placeholder
            "fileset103",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog202",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset103/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset104",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog203",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset104",
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset105",
            Fileset.Type.MANAGED,
            null,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset106",
            Fileset.Type.MANAGED,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog204",
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset107",
            Fileset.Type.EXTERNAL,
            UNFORMALIZED_TEST_ROOT_PATH + "/catalog205",
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset107",
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
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
            // honor the storage location with placeholder
            "fileset108",
            Fileset.Type.EXTERNAL,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/s1_fileset108",
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
            TEST_ROOT_PATH + "/fileset108"),
        Arguments.of(
            // honor the storage location
            "fileset109",
            Fileset.Type.EXTERNAL,
            null,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/fileset109",
            TEST_ROOT_PATH + "/fileset109"),
        Arguments.of(
            // honor the storage location with placeholder
            "fileset109",
            Fileset.Type.EXTERNAL,
            null,
            null,
            UNFORMALIZED_TEST_ROOT_PATH + "/{{fileset}}",
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

  private Schema createSchema(
      long testId, String name, String comment, String catalogPath, String schemaPath)
      throws IOException {
    return createSchema(testId, name, comment, catalogPath, schemaPath, false);
  }

  private Schema createSchema(
      long testId,
      String name,
      String comment,
      String catalogPath,
      String schemaPath,
      boolean disableFsOps)
      throws IOException {
    // stub schema
    doReturn(new SchemaIds(1L, 1L, testId))
        .when(spySchemaMetaService)
        .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
            Mockito.anyString(), Mockito.anyString(), Mockito.eq(name));

    Map<String, String> props = Maps.newHashMap();
    props.put(DISABLE_FILESYSTEM_OPS, String.valueOf(disableFsOps));
    if (catalogPath != null) {
      props.put(LOCATION, catalogPath);
    }

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(props, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", name);
      Map<String, String> schemaProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      schemaProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, schemaProps));

      if (schemaPath != null) {
        schemaProps.put(HadoopSchemaPropertiesMetadata.LOCATION, schemaPath);
      }

      return ops.createSchema(schemaIdent, comment, schemaProps);
    }
  }

  private Schema createMultiLocationSchema(
      String name,
      String comment,
      Map<String, String> catalogPaths,
      Map<String, String> schemaPaths)
      throws IOException {
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogPaths, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema("m1", "c1", name);
      Map<String, String> schemaProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      schemaProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, schemaProps));
      schemaProps.putAll(schemaPaths);

      return ops.createSchema(schemaIdent, comment, schemaProps);
    }
  }

  private Fileset createMultiLocationFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      Map<String, String> catalogPaths,
      Map<String, String> storageLocations,
      Map<String, String> filesetProps)
      throws IOException {
    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(catalogPaths, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      Map<String, String> newFilesetProps =
          Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps));

      return ops.createMultipleLocationFileset(
          filesetIdent, comment, type, storageLocations, newFilesetProps);
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
    return createFileset(name, schemaName, comment, type, catalogPath, storageLocation, false);
  }

  private Fileset createFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      String catalogPath,
      String storageLocation,
      boolean disableFsOps)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put(DISABLE_FILESYSTEM_OPS, String.valueOf(disableFsOps));
    if (catalogPath != null) {
      props.put(LOCATION, catalogPath);
    }

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(props, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      Map<String, String> filesetProps = Maps.newHashMap();
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      filesetProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps));

      return ops.createFileset(filesetIdent, comment, type, storageLocation, filesetProps);
    }
  }

  private Fileset createFileset(
      String name,
      String schemaName,
      String comment,
      Fileset.Type type,
      String catalogPath,
      String storageLocation,
      Map<String, String> filesetProps)
      throws IOException {
    Map<String, String> props = Maps.newHashMap();
    if (catalogPath != null) {
      props.put(LOCATION, catalogPath);
    }

    try (SecureHadoopCatalogOperations ops = new SecureHadoopCatalogOperations(store)) {
      ops.initialize(props, randomCatalogInfo("m1", "c1"), HADOOP_PROPERTIES_METADATA);

      NameIdentifier filesetIdent = NameIdentifier.of("m1", "c1", schemaName, name);
      filesetProps = filesetProps == null ? Maps.newHashMap() : filesetProps;
      StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
      filesetProps = Maps.newHashMap(StringIdentifier.newPropertiesWithId(stringId, filesetProps));

      return ops.createFileset(filesetIdent, comment, type, storageLocation, filesetProps);
    }
  }

  private long generateTestId() {
    return idGenerator.nextId();
  }
}
