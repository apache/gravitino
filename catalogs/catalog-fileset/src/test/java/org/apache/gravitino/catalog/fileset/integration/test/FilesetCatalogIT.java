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
package org.apache.gravitino.catalog.fileset.integration.test;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.file.Fileset.PROPERTY_LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.Type.MANAGED;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class FilesetCatalogIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetCatalogIT.class);
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  protected String metalakeName = GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
  protected String catalogName = GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
  public final String SCHEMA_PREFIX = "CatalogFilesetIT_schema";
  protected String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  protected static final String provider = "hadoop";
  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  protected FileSystem fileSystem;
  protected String defaultBaseLocation;

  protected CloseableHttpClient httpClient;

  protected void startNecessaryContainer() {
    containerSuite.startHiveContainer();
  }

  @BeforeAll
  public void setup() throws IOException {
    startNecessaryContainer();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    fileSystem = FileSystem.get(conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  @BeforeAll
  protected void setupHttpClient() {
    httpClient = HttpClients.createDefault();
  }

  @AfterAll
  public void stop() throws IOException {
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);
    if (fileSystem != null) {
      fileSystem.close();
    }
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (Exception ignored) {
      }
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
  }

  protected void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  protected void createCatalog() {
    metalake.createCatalog(
        catalogName, Catalog.Type.FILESET, provider, "comment", ImmutableMap.of());

    catalog = metalake.loadCatalog(catalogName);
  }

  protected void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put("location", defaultBaseLocation());
    String comment = "comment";

    catalog.asSchemas().createSchema(schemaName, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
    Assertions.assertNotNull(loadSchema.properties().get("location"));
  }

  private void dropSchema() {
    catalog.asSchemas().dropSchema(schemaName, true);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schemaName));
  }

  private String getFileInfos(NameIdentifier filesetIdent, String subPath, String locationName)
      throws IOException {
    String targetPath =
        "/api/metalakes/"
            + metalakeName
            + "/catalogs/"
            + catalogName
            + "/schemas/"
            + schemaName
            + "/filesets/"
            + filesetIdent.name()
            + "/files";

    URIBuilder uriBuilder;
    try {
      uriBuilder = new URIBuilder(serverUri + targetPath);
    } catch (URISyntaxException e) {
      throw new IOException("Error constructing URI: " + serverUri + targetPath, e);
    }

    if (!StringUtils.isBlank(subPath)) {
      uriBuilder.addParameter("sub_path", subPath);
    }
    if (!StringUtils.isBlank(locationName)) {
      uriBuilder.addParameter("location_name", locationName);
    }

    HttpGet httpGet;
    try {
      httpGet = new HttpGet(uriBuilder.build());
    } catch (URISyntaxException e) {
      throw new IOException("Failed to build URI with query parameters: " + uriBuilder, e);
    }
    try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
      return EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testFilesetCache() {
    Assumptions.assumeTrue(getClass() == FilesetCatalogIT.class);
    String catalogName = GravitinoITUtils.genRandomName("test_fileset_cache");
    String newCatalogName = catalogName + "_new";
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_cache_fileset");
    String location = defaultBaseLocation() + "/" + filesetName;
    Map<String, String> catalogProperties = ImmutableMap.of("location", location);

    Catalog filesetCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, provider, null, catalogProperties);
    filesetCatalog.asSchemas().createSchema(schemaName, null, null);
    filesetCatalog
        .asFilesetCatalog()
        .createFileset(NameIdentifier.of(schemaName, filesetName), null, MANAGED, null, null);

    // Load fileset
    Fileset fileset =
        filesetCatalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, filesetName));
    Assertions.assertEquals(filesetName, fileset.name());

    // rename catalog and load fileset
    Catalog alteredCatalog =
        metalake.alterCatalog(catalogName, CatalogChange.rename(newCatalogName));
    Assertions.assertTrue(
        alteredCatalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(schemaName, filesetName)));
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () ->
            filesetCatalog
                .asFilesetCatalog()
                .filesetExists(NameIdentifier.of(schemaName, filesetName)));

    // rename fileset and load fileset
    String newFilesetName = filesetName + "_new";
    Fileset alteredFileset =
        alteredCatalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.rename(newFilesetName));
    Assertions.assertEquals(newFilesetName, alteredFileset.name());
    Assertions.assertTrue(
        alteredCatalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(schemaName, newFilesetName)));

    // drop schema and load fileset
    alteredCatalog.asSchemas().dropSchema(schemaName, true);
    Assertions.assertFalse(
        alteredCatalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(schemaName, newFilesetName)));
    Assertions.assertFalse(
        alteredCatalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(schemaName, filesetName)));

    // drop catalog and load fileset
    metalake.dropCatalog(newCatalogName, true);
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () ->
            alteredCatalog
                .asFilesetCatalog()
                .filesetExists(NameIdentifier.of(schemaName, newFilesetName)));
  }

  @Test
  void testAlterCatalogLocation() throws IOException {
    Assumptions.assumeTrue(getClass() == FilesetCatalogIT.class);
    String catalogName = GravitinoITUtils.genRandomName("test_alter_catalog_location");
    String filesetName = "test_fileset1";
    String location = defaultBaseLocation();
    String newLocation = location + "/new_location";

    Map<String, String> catalogProperties = ImmutableMap.of("location", location);
    // Create a catalog using location
    Catalog filesetCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, provider, "comment", catalogProperties);
    filesetCatalog.asSchemas().createSchema(schemaName, null, null);
    filesetCatalog
        .asFilesetCatalog()
        .createFileset(NameIdentifier.of(schemaName, filesetName), null, MANAGED, null, null);

    Assertions.assertEquals(location, filesetCatalog.properties().get("location"));
    Assertions.assertTrue(
        fileSystem.exists(new Path(location + "/" + schemaName + "/" + filesetName)));

    // Now try to alter the location and change it to `newLocation`.
    Catalog modifiedCatalog =
        metalake.alterCatalog(catalogName, CatalogChange.setProperty("location", newLocation));
    String newFilesetName = "test_fileset2";
    modifiedCatalog
        .asFilesetCatalog()
        .createFileset(NameIdentifier.of(schemaName, newFilesetName), null, MANAGED, null, null);

    Assertions.assertEquals(newLocation, modifiedCatalog.properties().get("location"));
    Assertions.assertTrue(
        fileSystem.exists(new Path(newLocation + "/" + schemaName + "/" + newFilesetName)));

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testCreateFileset() throws IOException {
    // create fileset
    String filesetName = "test_create_fileset";
    String storageLocation = storageLocation(filesetName);
    Assertions.assertFalse(
        fileSystem.exists(new Path(storageLocation)), "storage location should not exists");
    Fileset fileset =
        createFileset(
            filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName);
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(MANAGED, fileset.type());
    Assertions.assertEquals(storageLocation, fileset.storageLocation());
    Assertions.assertEquals(storageLocation, fileset.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Assertions.assertEquals(2, fileset.properties().size());
    Assertions.assertEquals("v1", fileset.properties().get("k1"));
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));

    // test create a fileset that already exist
    Assertions.assertThrows(
        FilesetAlreadyExistsException.class,
        () ->
            createFileset(
                filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1")),
        "Should throw FilesetAlreadyExistsException when fileset already exists");

    // create fileset with null storage location
    String filesetName2 = "test_create_fileset_no_storage_location";
    Fileset fileset2 = createFileset(filesetName2, null, MANAGED, null, null);
    assertFilesetExists(filesetName2);
    Assertions.assertNotNull(fileset2, "fileset should be created");
    Assertions.assertNull(fileset2.comment(), "comment should be null");
    Assertions.assertEquals(MANAGED, fileset2.type(), "type should be MANAGED");
    Assertions.assertEquals(
        storageLocation(filesetName2),
        fileset2.storageLocation(),
        "storage location should be created");
    Assertions.assertEquals(
        storageLocation(filesetName2),
        fileset2.storageLocations().get(LOCATION_NAME_UNKNOWN),
        "storage location should be created");
    Assertions.assertEquals(
        ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, LOCATION_NAME_UNKNOWN),
        fileset2.properties());

    // create fileset with placeholder in storage location
    String filesetName4 = "test_create_fileset_with_placeholder";
    String storageLocation4 = defaultBaseLocation() + "/{{fileset}}";
    String expectedStorageLocation4 = defaultBaseLocation() + "/" + filesetName4;
    Assertions.assertFalse(
        fileSystem.exists(new Path(expectedStorageLocation4)),
        "storage location should not exists");
    Fileset fileset4 = createFileset(filesetName4, "comment", MANAGED, storageLocation4, null);
    assertFilesetExists(filesetName4);
    Assertions.assertNotNull(fileset4, "fileset should be created");
    Assertions.assertEquals("comment", fileset4.comment());
    Assertions.assertEquals(MANAGED, fileset4.type());
    Assertions.assertEquals(expectedStorageLocation4, fileset4.storageLocation());
    Assertions.assertEquals(
        expectedStorageLocation4, fileset4.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Assertions.assertEquals(1, fileset4.properties().size(), "properties should be empty");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, fileset4.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));

    // create fileset with multiple locations
    String filesetName5 = "test_create_fileset_with_multiple_locations";
    Map<String, String> storageLocations =
        ImmutableMap.of(
            "location1",
            storageLocation(filesetName5 + "_location1"),
            "location2",
            storageLocation(filesetName5 + "_location2"));
    fileset =
        createMultipleLocationsFileset(
            filesetName5,
            "comment",
            MANAGED,
            storageLocations,
            ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "location1"));
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(MANAGED, fileset.type());
    Map<String, String> expectedStorageLocations =
        new HashMap<String, String>(storageLocations) {
          {
            put(LOCATION_NAME_UNKNOWN, storageLocation(filesetName5));
          }
        };
    Assertions.assertEquals(expectedStorageLocations, fileset.storageLocations());
    Assertions.assertEquals(1, fileset.properties().size());
    Assertions.assertEquals("location1", fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));

    assertFilesetExists(filesetName5);
    fileset = catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, filesetName5));
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(MANAGED, fileset.type());
    Assertions.assertEquals(expectedStorageLocations, fileset.storageLocations());
    Assertions.assertEquals(1, fileset.properties().size());
    Assertions.assertEquals("location1", fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));

    // create fileset with null multiple locations
    String filesetName6 = "test_create_fileset_with_null_multiple_locations";
    createMultipleLocationsFileset(filesetName6, "comment", MANAGED, null, null);
    assertFilesetExists(filesetName6);
    fileset = catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, filesetName6));
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(MANAGED, fileset.type());
    Assertions.assertEquals(
        ImmutableMap.of(LOCATION_NAME_UNKNOWN, storageLocation(filesetName6)),
        fileset.storageLocations());

    // create fileset with null fileset name
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> createFileset(null, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1")),
        "Should throw IllegalArgumentException when fileset name is null");

    // create fileset with null fileset type
    String filesetName3 = "test_create_fileset_no_type";
    String storageLocation3 = storageLocation(filesetName3);
    Fileset fileset3 =
        createFileset(filesetName3, "comment", null, storageLocation3, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName3);
    Assertions.assertEquals(MANAGED, fileset3.type(), "fileset type should be MANAGED by default");

    // Test create fileset with a single file
    String filesetName7 = "test_create_fileset_with_a_file";
    String storageLocation7 = storageLocation(filesetName7);
    String filePath = storageLocation7 + "/file1.txt";
    fileSystem.createNewFile(new Path(filePath));
    Assertions.assertTrue(fileSystem.getFileStatus(new Path(filePath)).isFile());

    Throwable throwable =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> {
              createFileset(
                  filesetName7, "comment", MANAGED, filePath, ImmutableMap.of("k1", "v1"));
            });

    Assertions.assertTrue(throwable.getMessage().contains("Fileset location cannot be a file"));
  }

  @Test
  public void testAlterFileset() {
    // create fileset with placeholder in storage location
    String filesetName = "test_alter_fileset";
    String storageLocation = storageLocation(filesetName) + "/{{user}}";
    String placeholderKey = PROPERTY_LOCATION_PLACEHOLDER_PREFIX + "user";
    createFileset(
        filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of(placeholderKey, "test"));

    // alter fileset
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .alterFileset(
                        NameIdentifier.of(schemaName, filesetName),
                        FilesetChange.setProperty(placeholderKey, "test2")));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Property placeholder-user is immutable or reserved, cannot be set"),
        exception.getMessage());
  }

  @Test
  public void testCreateFilesetWithChinese() throws IOException {
    // create fileset
    String filesetName = "test_create_fileset_with_chinese";
    String storageLocation = storageLocation(filesetName) + "/中文目录test";
    Assertions.assertFalse(
        fileSystem.exists(new Path(storageLocation)), "storage location should not exists");
    Fileset fileset =
        createFileset(
            filesetName,
            "这是中文comment",
            MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1", "test", "中文测试test", "中文key", "test1"));

    // verify fileset is created
    assertFilesetExists(filesetName);
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("这是中文comment", fileset.comment());
    Assertions.assertEquals(MANAGED, fileset.type());
    Assertions.assertEquals(storageLocation, fileset.storageLocation());
    Assertions.assertEquals(4, fileset.properties().size());
    Assertions.assertEquals("v1", fileset.properties().get("k1"));
    Assertions.assertEquals("中文测试test", fileset.properties().get("test"));
    Assertions.assertEquals("test1", fileset.properties().get("中文key"));
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testExternalFileset() throws IOException {
    // create fileset
    String filesetName = "test_external_fileset";
    String storageLocation = storageLocation(filesetName);
    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.EXTERNAL,
            storageLocation,
            ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName);
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(Fileset.Type.EXTERNAL, fileset.type());
    Assertions.assertEquals(storageLocation, fileset.storageLocation());
    Assertions.assertEquals(2, fileset.properties().size());
    Assertions.assertEquals("v1", fileset.properties().get("k1"));
    Assertions.assertTrue(
        fileSystem.exists(new Path(storageLocation)), "storage location should be created");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));

    // create fileset with storage location that not exist
    String filesetName2 = "test_external_fileset_no_exist";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            createFileset(
                filesetName2, "comment", Fileset.Type.EXTERNAL, null, ImmutableMap.of("k1", "v1")),
        "Should throw IllegalArgumentException when storage location is null");
  }

  @Test
  void testNameSpec() {
    Assertions.assertDoesNotThrow(
        () -> metalake.createCatalog("my-catalog", Catalog.Type.FILESET, provider, null, null));

    String illegalName = "ok/test";

    // test illegal catalog name
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.createCatalog(illegalName, Catalog.Type.FILESET, provider, null, null));
    Assertions.assertTrue(exception.getMessage().contains("The catalog name 'ok/test' is illegal"));

    // test rename catalog to illegal name
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalake.alterCatalog(catalog.name(), CatalogChange.rename(illegalName)));
    Assertions.assertTrue(exception.getMessage().contains("The catalog name 'ok/test' is illegal"));

    // test illegal schema name
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> catalog.asSchemas().createSchema(illegalName, "comment", null));
    Assertions.assertTrue(
        exception.getMessage().contains("does not support '/' in the name for SCHEMA"));

    // test illegal fileset name
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, illegalName);
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .createFileset(nameIdentifier, null, MANAGED, null, null));
    Assertions.assertTrue(
        exception.getMessage().contains("does not support '/' in the name for FILESET"));

    // test rename fileset to illegal name
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .alterFileset(nameIdentifier, FilesetChange.rename(illegalName)));
    Assertions.assertTrue(
        exception.getMessage().contains("does not support '/' in the name for FILESET"));
  }

  @Test
  public void testLoadFileset() throws IOException {
    // create fileset
    String filesetName = "test_load_fileset";
    String storageLocation = storageLocation(filesetName);

    Fileset fileset =
        createFileset(
            filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // test load fileset
    Fileset loadFileset =
        catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, filesetName));
    Assertions.assertEquals(fileset.name(), loadFileset.name(), "fileset should be loaded");
    Assertions.assertEquals(fileset.comment(), loadFileset.comment(), "comment should be loaded");
    Assertions.assertEquals(fileset.type(), loadFileset.type(), "type should be loaded");
    Assertions.assertEquals(
        fileset.storageLocation(),
        loadFileset.storageLocation(),
        "storage location should be loaded");
    Assertions.assertEquals(
        fileset.properties(), loadFileset.properties(), "properties should be loaded");

    // test load a fileset that not exist
    Assertions.assertThrows(
        NoSuchFilesetException.class,
        () -> catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, "not_exist")),
        "Should throw NoSuchFilesetException when fileset does not exist");
  }

  @Test
  public void testDropManagedFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_managed_fileset";
    String storageLocation = storageLocation(filesetName);

    Assertions.assertFalse(
        fileSystem.exists(new Path(storageLocation)), "storage location should not exists");

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // drop fileset
    boolean dropped =
        catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, filesetName));
    Assertions.assertTrue(dropped, "fileset should be dropped");

    // verify fileset is dropped
    Assertions.assertFalse(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should not be exists");
    Assertions.assertFalse(
        fileSystem.exists(new Path(storageLocation)), "storage location should be dropped");
  }

  @Test
  public void testDropExternalFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_external_fileset";
    String storageLocation = storageLocation(filesetName);

    createFileset(
        filesetName,
        "comment",
        Fileset.Type.EXTERNAL,
        storageLocation,
        ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // drop fileset
    boolean dropped =
        catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, filesetName));
    Assertions.assertTrue(dropped, "fileset should be dropped");

    // verify fileset is dropped
    Assertions.assertFalse(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should not be exists");
    Assertions.assertTrue(
        fileSystem.exists(new Path(storageLocation)), "storage location should not be dropped");
  }

  @Test
  public void testListFilesets() throws IOException {
    // clear schema
    dropSchema();
    createSchema();

    // test no fileset exists
    NameIdentifier[] nameIdentifiers =
        catalog.asFilesetCatalog().listFilesets(Namespace.of(schemaName));
    Assertions.assertEquals(0, nameIdentifiers.length, "should have no fileset");

    // create fileset1
    String filesetName1 = "test_list_filesets1";
    String storageLocation = storageLocation(filesetName1);

    Fileset fileset1 =
        createFileset(
            filesetName1, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName1);

    // create fileset2
    String filesetName2 = "test_list_filesets2";
    String storageLocation2 = storageLocation(filesetName2);

    Fileset fileset2 =
        createFileset(
            filesetName2, "comment", MANAGED, storageLocation2, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName2);

    // list filesets
    NameIdentifier[] nameIdentifiers1 =
        catalog.asFilesetCatalog().listFilesets(Namespace.of(schemaName));
    Arrays.sort(nameIdentifiers1, Comparator.comparing(NameIdentifier::name));
    Assertions.assertEquals(2, nameIdentifiers1.length, "should have 2 filesets");
    Assertions.assertEquals(fileset1.name(), nameIdentifiers1[0].name());
    Assertions.assertEquals(fileset2.name(), nameIdentifiers1[1].name());
  }

  @Test
  public void testListFilesetFiles() throws IOException {
    dropSchema();
    createSchema();

    String filesetName = "test_list_files";
    String storageLocation = storageLocation(filesetName);
    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    Path basePath = new Path(storageLocation);
    fileSystem.mkdirs(basePath);

    String fileName = "test.txt";
    Path filePath = new Path(basePath, fileName);
    String content = "hello";
    try (FSDataOutputStream out = fileSystem.create(filePath, true)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }

    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String actualJson = getFileInfos(filesetIdent, null, null);

    Assertions.assertTrue(
        actualJson.contains(String.format("\"name\":\"%s\"", fileName)),
        String.format("Response JSON should contain \"name\":\"%s\"", fileName));
    Assertions.assertTrue(
        actualJson.contains("\"isDir\":false"), "Response JSON should contain \"isDir\":false");
    long actualSize = content.getBytes(StandardCharsets.UTF_8).length;
    Assertions.assertTrue(
        actualJson.contains(String.format("\"size\":%d", actualSize)),
        String.format("Response JSON should contain \"size\":%d", actualSize));
    long lastMod = fileSystem.getFileStatus(filePath).getModificationTime();
    Assertions.assertTrue(
        actualJson.contains(String.format("\"lastModified\":%d", lastMod)),
        String.format("Response JSON should contain \"lastModified\":%d", lastMod));
    String suffix = "/" + filesetName + "/";
    String regex = "\"path\"\\s*:\\s*\"[^\"]*" + Pattern.quote(suffix) + "\"";
    Pattern suffixChecker = Pattern.compile(regex);
    Assertions.assertTrue(
        suffixChecker.matcher(actualJson).find(),
        () ->
            String.format("Response JSON should contain a path value ending with \"%s\"", suffix));
  }

  @Test
  public void testListFilesetFilesWithSubPath() throws IOException {
    dropSchema();
    createSchema();

    String filesetName = "test_list_files_with_subpath";
    String storageLocation = storageLocation(filesetName);
    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    Path basePath = new Path(storageLocation);
    String subDirName = "subdir";
    Path subDir = new Path(basePath, subDirName);
    fileSystem.mkdirs(subDir);

    String fileName = "test.txt";
    Path filePath = new Path(subDir, fileName);
    String content = "hello";
    try (FSDataOutputStream out = fileSystem.create(filePath, true)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }

    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String actualJson = getFileInfos(filesetIdent, "subdir", null);

    Assertions.assertTrue(
        actualJson.contains(String.format("\"name\":\"%s\"", fileName)),
        String.format("Response JSON should contain \"name\":\"%s\"", fileName));
    Assertions.assertTrue(
        actualJson.contains("\"isDir\":false"), "Response JSON should contain \"isDir\":false");
    long actualSize = content.getBytes(StandardCharsets.UTF_8).length;
    Assertions.assertTrue(
        actualJson.contains(String.format("\"size\":%d", actualSize)),
        String.format("Response JSON should contain \"size\":%d", actualSize));
    long lastMod = fileSystem.getFileStatus(filePath).getModificationTime();
    Assertions.assertTrue(
        actualJson.contains(String.format("\"lastModified\":%d", lastMod)),
        String.format("Response JSON should contain \"lastModified\":%d", lastMod));
    String suffix = "/" + filesetName + "/" + subDirName;
    String regex = "\"path\"\\s*:\\s*\"[^\"]*" + Pattern.quote(suffix) + "\"";
    Pattern suffixChecker = Pattern.compile(regex);
    Assertions.assertTrue(
        suffixChecker.matcher(actualJson).find(),
        () ->
            String.format("Response JSON should contain a path value ending with \"%s\"", suffix));
  }

  @Test
  public void testListFilesetFilesWithLocationName() throws IOException {
    dropSchema();
    createSchema();

    String filesetName = "test_list_files_with_location_name";
    String locA = storageLocation(filesetName + "_locA");
    String locB = storageLocation(filesetName + "_locB");
    Map<String, String> storageLocations =
        ImmutableMap.of(
            "locA", locA,
            "locB", locB);
    Map<String, String> props = ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, "locA");

    createMultipleLocationsFileset(filesetName, "comment", MANAGED, storageLocations, props);
    assertFilesetExists(filesetName);

    Path basePathA = new Path(locA);
    fileSystem.mkdirs(basePathA);
    String fileNameA = "testA.txt";
    Path filePathA = new Path(basePathA, fileNameA);
    String contentA = "hello";
    try (FSDataOutputStream out = fileSystem.create(filePathA, true)) {
      out.write(contentA.getBytes(StandardCharsets.UTF_8));
    }

    Path basePathB = new Path(locB);
    fileSystem.mkdirs(basePathB);
    String fileNameB = "testB.txt";
    Path filePathB = new Path(basePathB, fileNameB);
    String contentB = "hello hello";
    try (FSDataOutputStream out = fileSystem.create(filePathB, true)) {
      out.write(contentB.getBytes(StandardCharsets.UTF_8));
    }

    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    // get file info from locA without locationName, and from locB with locationName
    String actualJsonA = getFileInfos(filesetIdent, null, null);
    String actualJsonB = getFileInfos(filesetIdent, null, "locB");

    // verify locA files
    Assertions.assertTrue(
        actualJsonA.contains(String.format("\"name\":\"%s\"", fileNameA)),
        String.format("Response JSON should contain \"name\":\"%s\"", fileNameA));
    long actualSizeA = contentA.getBytes(StandardCharsets.UTF_8).length;
    Assertions.assertTrue(
        actualJsonA.contains(String.format("\"size\":%d", actualSizeA)),
        String.format("Response JSON should contain \"size\":%d", actualSizeA));

    // verify locB files
    Assertions.assertTrue(
        actualJsonB.contains(String.format("\"name\":\"%s\"", fileNameB)),
        String.format("Response JSON should contain \"name\":\"%s\"", fileNameB));
    long actualSizeB = contentA.getBytes(StandardCharsets.UTF_8).length;
    Assertions.assertTrue(
        actualJsonA.contains(String.format("\"size\":%d", actualSizeB)),
        String.format("Response JSON should contain \"size\":%d", actualSizeB));
  }

  @Test
  public void testListFilesetFilesWithNonExistentPath() throws IOException {
    dropSchema();
    createSchema();

    String filesetName = "test_list_files_with_non_existent_path";
    String storageLocation = storageLocation(filesetName);
    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String invalidPath = "nonexistent";

    String actualJson = getFileInfos(filesetIdent, invalidPath, null);
    Assertions.assertTrue(
        actualJson.contains("does not exist in fileset"),
        String.format(
            "Response JSON should contain \"does not exist in fileset\", but was: %s", actualJson));
  }

  @Test
  public void testRenameFileset() throws IOException {
    // create fileset
    String filesetName = "test_rename_fileset";
    String storageLocation = storageLocation(filesetName);

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // rename fileset
    String newFilesetName = "test_rename_fileset_new";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.rename(newFilesetName));

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals(newFilesetName, newFileset.name(), "fileset name should be updated");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(2, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v1", newFileset.properties().get("k1"), "properties should not be change");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, newFileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testFilesetUpdateComment() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_comment";
    String storageLocation = storageLocation(filesetName);

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset comment
    String newComment = "new_comment";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName),
                FilesetChange.updateComment(newComment));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals(newComment, newFileset.comment(), "comment should be updated");
    Assertions.assertEquals(MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(2, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v1", newFileset.properties().get("k1"), "properties should not be change");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, newFileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testFilesetSetProperties() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_properties";
    String storageLocation = storageLocation(filesetName);

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.setProperty("k1", "v2"));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(2, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v2", newFileset.properties().get("k1"), "properties should be updated");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, newFileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testFilesetRemoveProperties() throws IOException {
    // create fileset
    String filesetName = "test_remove_fileset_properties";
    String storageLocation = storageLocation(filesetName);

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.removeProperty("k1"));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(1, newFileset.properties().size(), "properties should be removed");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, newFileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testFilesetRemoveComment() throws IOException {
    // create fileset
    String filesetName = "test_remove_fileset_comment";
    String storageLocation = storageLocation(filesetName);

    createFileset(filesetName, "comment", MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // remove fileset comment
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.updateComment(null));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertNull(newFileset.comment(), "comment should be removed");
    Assertions.assertEquals(MANAGED, newFileset.type(), "type should not be changed");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be changed");
    Assertions.assertEquals(2, newFileset.properties().size(), "properties should not be changed");
    Assertions.assertEquals(
        "v1", newFileset.properties().get("k1"), "properties should not be changed");
    Assertions.assertEquals(
        LOCATION_NAME_UNKNOWN, newFileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME));
  }

  @Test
  public void testDropCatalogWithEmptySchema() {
    String catalogName =
        GravitinoITUtils.genRandomName("test_drop_catalog_with_empty_schema_catalog");
    // Create a catalog without specifying location.
    Catalog filesetCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, provider, "comment", ImmutableMap.of());

    // Create a schema without specifying location.
    String schemaName =
        GravitinoITUtils.genRandomName("test_drop_catalog_with_empty_schema_schema");
    filesetCatalog.asSchemas().createSchema(schemaName, "comment", ImmutableMap.of());

    // Drop the empty schema.
    boolean dropped = filesetCatalog.asSchemas().dropSchema(schemaName, true);
    Assertions.assertTrue(dropped, "schema should be dropped");
    Assertions.assertFalse(
        filesetCatalog.asSchemas().schemaExists(schemaName), "schema should not be exists");

    // Drop the catalog.
    dropped = metalake.dropCatalog(catalogName, true);
    Assertions.assertTrue(dropped, "catalog should be dropped");
    Assertions.assertFalse(metalake.catalogExists(catalogName), "catalog should not be exists");
  }

  @Test
  public void testGetFileLocation() {
    String filesetName = GravitinoITUtils.genRandomName("fileset");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    Fileset expectedFileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                filesetIdent,
                "fileset comment",
                MANAGED,
                generateLocation(filesetName),
                Maps.newHashMap());
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    // test without caller context
    try {
      String actualFileLocation =
          catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test1.par");

      Assertions.assertEquals(expectedFileset.storageLocation() + "/test1.par", actualFileLocation);
    } finally {
      CallerContext.CallerContextHolder.remove();
    }

    // test with caller context
    try {
      Map<String, String> context = new HashMap<>();
      context.put(
          FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
          InternalClientType.HADOOP_GVFS.name());
      context.put(
          FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
          FilesetDataOperation.CREATE.name());
      CallerContext callerContext = CallerContext.builder().withContext(context).build();
      CallerContext.CallerContextHolder.set(callerContext);

      String actualFileLocation =
          catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test2.par");

      Assertions.assertEquals(expectedFileset.storageLocation() + "/test2.par", actualFileLocation);
    } finally {
      CallerContext.CallerContextHolder.remove();
    }
  }

  @Test
  public void testGetFileLocationWithMultipleLocations() {
    String filesetName = GravitinoITUtils.genRandomName("fileset");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    String locationName1 = "location1";
    String locationName2 = "location2";
    Map<String, String> storageLocations =
        ImmutableMap.of(
            locationName1,
            storageLocation(filesetName + "_location1"),
            locationName2,
            storageLocation(filesetName + "_location2"));
    Fileset expectedFileset =
        catalog
            .asFilesetCatalog()
            .createMultipleLocationFileset(
                filesetIdent,
                "fileset comment",
                MANAGED,
                storageLocations,
                ImmutableMap.of(PROPERTY_DEFAULT_LOCATION_NAME, locationName1));
    Assertions.assertTrue(catalog.asFilesetCatalog().filesetExists(filesetIdent));
    // test without caller context
    String actualFileLocation1 =
        catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test1.par", locationName1);
    Assertions.assertEquals(
        expectedFileset.storageLocations().get(locationName1) + "/test1.par", actualFileLocation1);

    String actualFileLocation2 =
        catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test2.par", locationName2);
    Assertions.assertEquals(
        expectedFileset.storageLocations().get(locationName2) + "/test2.par", actualFileLocation2);

    // test with caller context
    try {
      Map<String, String> context = new HashMap<>();
      context.put(
          FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
          InternalClientType.HADOOP_GVFS.name());
      context.put(
          FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
          FilesetDataOperation.CREATE.name());
      CallerContext callerContext = CallerContext.builder().withContext(context).build();
      CallerContext.CallerContextHolder.set(callerContext);

      actualFileLocation1 =
          catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test1.par", locationName1);
      Assertions.assertEquals(
          expectedFileset.storageLocations().get(locationName1) + "/test1.par",
          actualFileLocation1);

      actualFileLocation2 =
          catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test2.par", locationName2);
      Assertions.assertEquals(
          expectedFileset.storageLocations().get(locationName2) + "/test2.par",
          actualFileLocation2);
    } finally {
      CallerContext.CallerContextHolder.remove();
    }
  }

  @Test
  public void testGetFileLocationWithInvalidAuditHeaders() {
    try {
      String filesetName = GravitinoITUtils.genRandomName("fileset");
      NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
      Assertions.assertFalse(catalog.asFilesetCatalog().filesetExists(filesetIdent));
      Fileset expectedFileset =
          catalog
              .asFilesetCatalog()
              .createFileset(
                  filesetIdent,
                  "fileset comment",
                  MANAGED,
                  generateLocation(filesetName),
                  Maps.newHashMap());

      Map<String, String> context = new HashMap<>();
      // this is an invalid internal client type, but the server will return normally
      context.put(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE, "test");
      CallerContext callerContext = CallerContext.builder().withContext(context).build();
      CallerContext.CallerContextHolder.set(callerContext);

      String fileLocation = catalog.asFilesetCatalog().getFileLocation(filesetIdent, "/test.par");
      Assertions.assertEquals(expectedFileset.storageLocation() + "/test.par", fileLocation);
    } finally {
      CallerContext.CallerContextHolder.remove();
    }
  }

  @Test
  public void testCreateSchemaAndFilesetWithSpecialLocation() {
    String localCatalogName = GravitinoITUtils.genRandomName("local_catalog");
    String hdfsLocation =
        String.format(
            "hdfs://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
    Map<String, String> catalogProps = ImmutableMap.of("location", hdfsLocation);

    Catalog localCatalog =
        metalake.createCatalog(
            localCatalogName, Catalog.Type.FILESET, provider, "comment", catalogProps);
    Assertions.assertEquals(hdfsLocation, localCatalog.properties().get("location"));

    // Create schema without specifying location.
    Schema localSchema =
        localCatalog
            .asSchemas()
            .createSchema("local_schema", "comment", ImmutableMap.of("key1", "val1"));

    Fileset localFileset =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema.name(), "local_fileset"),
                "fileset comment",
                MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(
        hdfsLocation + "/local_schema/local_fileset", localFileset.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema.name(), true);

    // Create schema with specifying location.
    Map<String, String> schemaProps = ImmutableMap.of("location", hdfsLocation);
    Schema localSchema2 =
        localCatalog.asSchemas().createSchema("local_schema2", "comment", schemaProps);
    Assertions.assertEquals(hdfsLocation, localSchema2.properties().get("location"));

    Fileset localFileset2 =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema2.name(), "local_fileset2"),
                "fileset comment",
                MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(hdfsLocation + "/local_fileset2", localFileset2.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema2.name(), true);

    // Delete catalog
    metalake.dropCatalog(localCatalogName, true);
  }

  protected String generateLocation(String filesetName) {
    return String.format(
        "hdfs://%s:%d/user/hadoop/%s/%s/%s",
        containerSuite.getHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT,
        catalogName,
        schemaName,
        filesetName);
  }

  private Fileset createFileset(
      String filesetName,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties) {
    if (storageLocation != null) {
      Path location = new Path(storageLocation);
      try {
        fileSystem.deleteOnExit(location);
      } catch (IOException e) {
        LOG.warn("Failed to delete location: {}", location, e);
      }
    }

    return catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(schemaName, filesetName), comment, type, storageLocation, properties);
  }

  private Fileset createMultipleLocationsFileset(
      String filesetName,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties) {
    if (storageLocations != null) {
      for (String location : storageLocations.values()) {
        Path path = new Path(location);
        try {
          fileSystem.deleteOnExit(path);
        } catch (IOException e) {
          LOG.warn("Failed to delete location: {}", path, e);
        }
      }
    }

    return catalog
        .asFilesetCatalog()
        .createMultipleLocationFileset(
            NameIdentifier.of(schemaName, filesetName),
            comment,
            type,
            storageLocations,
            properties);
  }

  private void assertFilesetExists(String filesetName) throws IOException {
    Assertions.assertTrue(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should be exists");
    Assertions.assertTrue(
        fileSystem.exists(new Path(storageLocation(filesetName))),
        "storage location should be exists");
  }

  protected String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      defaultBaseLocation =
          String.format(
              "hdfs://%s:%d/user/hadoop/%s.db",
              containerSuite.getHiveContainer().getContainerIpAddress(),
              HiveContainer.HDFS_DEFAULTFS_PORT,
              schemaName.toLowerCase());
    }
    return defaultBaseLocation;
  }

  private String storageLocation(String filesetName) {
    return defaultBaseLocation() + "/" + filesetName;
  }
}
