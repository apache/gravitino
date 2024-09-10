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
package org.apache.gravitino.catalog.hadoop.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HadoopCatalogCommonIT extends AbstractIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopCatalogCommonIT.class);
  private static final String PROVIDER = "hadoop";
  private static String metalakeName;
  private static String catalogName;
  private static String schemaName;

  protected static GravitinoMetalake metalake;
  protected static Catalog catalog;
  protected static FileSystem fs;

  protected abstract void startContainer();

  protected abstract Configuration hadoopConf();

  protected abstract String defaultBaseLocation();

  protected abstract String metalakeName();

  protected abstract String catalogName();

  protected abstract String schemaName();

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    assertEquals(0, gravitinoMetalakes.length);

    metalakeName = metalakeName();
    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    String[] catalogs = metalake.listCatalogs();
    assertEquals(0, catalogs.length);

    catalogName = catalogName();
    Map<String, String> catalogProperties = catalogProperties();
    metalake.createCatalog(
        catalogName, Catalog.Type.FILESET, PROVIDER, "comment", catalogProperties);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected Map<String, String> catalogProperties() {
    return Collections.emptyMap();
  }

  private void createSchema() {
    String[] schemas = catalog.asSchemas().listSchemas();
    assertEquals(0, schemas.length);

    schemaName = schemaName();
    Map<String, String> schemaProperties = schemaProperties();
    catalog.asSchemas().createSchema(schemaName, "comment", schemaProperties);
  }

  protected Map<String, String> schemaProperties() {
    return Collections.emptyMap();
  }

  @BeforeAll
  void setup() throws IOException {
    startContainer();
    fs = FileSystem.get(hadoopConf());

    createMetalake();
    createCatalog();
    createSchema();
  }

  @Test
  void testAlterCatalogLocation() {
    String catalogName = GravitinoITUtils.genRandomName("test_alter_catalog_location");
    String location = defaultBaseLocation();
    String newLocation = location + "/new_location";

    Map<String, String> catalogProperties = ImmutableMap.of("location", location);
    // Create a catalog using location
    Catalog filesetCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, PROVIDER, "comment", catalogProperties);

    assertEquals(location, filesetCatalog.properties().get("location"));

    // Now try to alter the location and change it to `newLocation`.
    Catalog modifiedCatalog =
        metalake.alterCatalog(catalogName, CatalogChange.setProperty("location", newLocation));

    assertEquals(newLocation, modifiedCatalog.properties().get("location"));

    metalake.dropCatalog(catalogName);
  }

  @Test
  void testCreateFileset() throws IOException {
    // create fileset
    String filesetName1 = "test_create_fileset";
    String storageLocation = defaultBaseLocation() + filesetName1;
    assertFalse(
        fs.exists(new Path(storageLocation)), "storage location should not exist before creating");
    Fileset fileset =
        createFileset(
            filesetName1,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName1, storageLocation);
    assertNotNull(fileset, "fileset should be created");
    assertEquals("comment", fileset.comment());
    assertEquals(Fileset.Type.MANAGED, fileset.type());
    assertEquals(storageLocation, fileset.storageLocation());
    assertEquals(1, fileset.properties().size());
    assertEquals("v1", fileset.properties().get("k1"));

    // test create a fileset that already exist
    assertThrows(
        FilesetAlreadyExistsException.class,
        () ->
            createFileset(
                filesetName1,
                "comment",
                Fileset.Type.MANAGED,
                storageLocation,
                ImmutableMap.of("k1", "v1")),
        "Should throw FilesetAlreadyExistsException when fileset already exists");
  }

  @Test
  void testCreateFilesetWithNoType() throws IOException {
    // create fileset with no fileset type
    String filesetName = "test_create_fileset_with_no_type";
    String storageLocation = defaultBaseLocation() + filesetName;
    Fileset fileset2 =
        createFileset(filesetName, "comment", null, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);
    assertEquals(
        Fileset.Type.MANAGED, fileset2.type(), "fileset type should be MANAGED by default");
  }

  @Test
  void testCreateFilesetWithNoName() {
    // create fileset with no fileset name
    assertThrows(
        IllegalArgumentException.class,
        () ->
            createFileset(
                null,
                "comment",
                Fileset.Type.MANAGED,
                defaultBaseLocation() + "null",
                ImmutableMap.of("k1", "v1")),
        "Should throw IllegalArgumentException when fileset name is null");
  }

  @Test
  void testCreateFilesetWithNoStorageLocation() throws IOException {
    // create fileset with null storage location
    String filesetName = "test_create_fileset_with_no_storage_location";
    String storageLocation = defaultBaseLocation() + filesetName;
    assertFalse(
        fs.exists(new Path(storageLocation)), "storage location should not exist before creating");
    Fileset fileset2 = createFileset(filesetName, null, Fileset.Type.MANAGED, null, null);
    assertFilesetExists(filesetName, storageLocation);
    assertNotNull(fileset2, "fileset should be created");
    assertNull(fileset2.comment(), "comment should be null");
    assertEquals(Fileset.Type.MANAGED, fileset2.type(), "type should be MANAGED");
    assertEquals(storageLocation, fileset2.storageLocation(), "storage location should be created");
    assertEquals(Collections.emptyMap(), fileset2.properties(), "properties should be empty");
  }

  @Test
  void testCreateExternalFileset() throws IOException {
    // create external fileset
    String filesetName = "test_external_fileset";
    String storageLocation = defaultBaseLocation() + filesetName;
    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.EXTERNAL,
            storageLocation,
            ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName, storageLocation);
    assertNotNull(fileset, "fileset should be created");
    assertEquals("comment", fileset.comment());
    assertEquals(Fileset.Type.EXTERNAL, fileset.type());
    assertEquals(storageLocation, fileset.storageLocation());
    assertEquals(1, fileset.properties().size());
    assertEquals("v1", fileset.properties().get("k1"));
    assertTrue(fs.exists(new Path(storageLocation)), "storage location should be created");

    // create fileset with no storage location
    String filesetName2 = "test_external_fileset_with_no_storage_location";
    assertThrows(
        IllegalArgumentException.class,
        () ->
            createFileset(
                filesetName2, "comment", Fileset.Type.EXTERNAL, null, ImmutableMap.of("k1", "v1")),
        "Should throw IllegalArgumentException when storage location is null");
  }

  @Test
  void testDropFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_managed_fileset";
    String storageLocation = defaultBaseLocation() + filesetName;
    assertFalse(
        fs.exists(new Path(storageLocation)), "storage location should not exist before creating");
    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // drop fileset
    assertTrue(
        catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, filesetName)),
        "fileset should be dropped");

    // verify fileset is dropped
    assertFalse(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should not exist");
    assertFalse(fs.exists(new Path(storageLocation)), "storage location should not exist");
  }

  @Test
  void testDropExternalFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_external_fileset";
    String storageLocation = defaultBaseLocation() + filesetName;
    createFileset(
        filesetName,
        "comment",
        Fileset.Type.EXTERNAL,
        storageLocation,
        ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // drop fileset
    assertTrue(
        catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(schemaName, filesetName)),
        "fileset should be dropped");

    // verify fileset is dropped
    assertFalse(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should not exist");
    assertTrue(fs.exists(new Path(storageLocation)), "storage location should exist");
  }

  @Test
  void testLoadFileset() throws IOException {
    // create fileset
    String filesetName = "test_load_fileset";
    String storageLocation = defaultBaseLocation() + filesetName;
    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // test load fileset
    Fileset loadFileset =
        catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, filesetName));
    assertEquals(fileset.name(), loadFileset.name(), "fileset should be loaded");
    assertEquals(fileset.comment(), loadFileset.comment(), "comment should be loaded");
    assertEquals(fileset.type(), loadFileset.type(), "type should be loaded");
    assertEquals(
        fileset.storageLocation(),
        loadFileset.storageLocation(),
        "storage location should be loaded");
    assertEquals(fileset.properties(), loadFileset.properties(), "properties should be loaded");

    assertThrows(
        NoSuchFilesetException.class,
        () -> catalog.asFilesetCatalog().loadFileset(NameIdentifier.of(schemaName, "not_exist")),
        "Should throw NoSuchFilesetException when fileset does not exist");
  }

  @Test
  void testListFilesets() throws IOException {
    // clear schema
    catalog.asSchemas().dropSchema(schemaName, true);
    assertFalse(catalog.asSchemas().schemaExists(schemaName));
    createSchema();

    // assert no fileset exists
    NameIdentifier[] nameIdentifiers =
        catalog.asFilesetCatalog().listFilesets(Namespace.of(schemaName));
    assertEquals(0, nameIdentifiers.length, "should have no fileset");

    // create fileset1
    String filesetName1 = "test_list_filesets1";
    String storageLocation1 = defaultBaseLocation() + filesetName1;
    Fileset fileset1 =
        createFileset(
            filesetName1,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation1,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName1, storageLocation1);

    // create fileset2
    String filesetName2 = "test_list_filesets2_on_s3";
    String storageLocation2 = defaultBaseLocation() + filesetName2;
    Fileset fileset2 =
        createFileset(
            filesetName2,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation2,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName2, storageLocation2);

    // list filesets
    NameIdentifier[] nameIdentifiers1 =
        catalog.asFilesetCatalog().listFilesets(Namespace.of(schemaName));
    Arrays.sort(nameIdentifiers1, Comparator.comparing(NameIdentifier::name));
    assertEquals(2, nameIdentifiers1.length, "should have 2 filesets");
    assertEquals(fileset1.name(), nameIdentifiers1[0].name());
    assertEquals(fileset2.name(), nameIdentifiers1[1].name());
  }

  @Test
  void testRenameFileset() throws IOException {
    // create fileset
    String filesetName = "test_rename_fileset";
    String storageLocation = defaultBaseLocation() + filesetName;
    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // rename fileset
    String newFilesetName = "test_rename_fileset_new";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.rename(newFilesetName));

    // verify fileset is updated
    assertNotNull(newFileset, "fileset should be created");
    assertEquals(newFilesetName, newFileset.name(), "fileset name should be updated");
    assertEquals("comment", newFileset.comment(), "comment should not be change");
    assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    assertEquals(1, newFileset.properties().size(), "properties should not be change");
    assertEquals("v1", newFileset.properties().get("k1"), "properties should not be change");
    assertTrue(fs.exists(new Path(storageLocation)), "storage location should exist");
  }

  @Test
  void testFilesetUpdateComment() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_comment";
    String storageLocation = defaultBaseLocation() + filesetName;

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // update fileset comment
    String newComment = "new_comment";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName),
                FilesetChange.updateComment(newComment));
    assertFilesetExists(filesetName, storageLocation);

    // verify fileset is updated
    assertNotNull(newFileset, "fileset should be created");
    assertEquals(newComment, newFileset.comment(), "comment should be updated");
    assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    assertEquals(1, newFileset.properties().size(), "properties should not be change");
    assertEquals("v1", newFileset.properties().get("k1"), "properties should not be change");
  }

  @Test
  void testFilesetRemoveComment() throws IOException {
    // create fileset
    String filesetName = "test_remove_fileset_comment";
    String storageLocation = defaultBaseLocation() + filesetName;

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // remove fileset comment
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.removeComment());
    assertFilesetExists(filesetName, storageLocation);

    // verify fileset is updated
    assertNotNull(newFileset, "fileset should be created");
    assertNull(newFileset.comment(), "comment should be removed");
    assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be changed");
    assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be changed");
    assertEquals(1, newFileset.properties().size(), "properties should not be changed");
    assertEquals("v1", newFileset.properties().get("k1"), "properties should not be changed");
  }

  @Test
  void testFilesetUpdateProperties() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_properties";
    String storageLocation = defaultBaseLocation() + filesetName;

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.setProperty("k1", "v2"));
    assertFilesetExists(filesetName, storageLocation);

    // verify fileset is updated
    assertNotNull(newFileset, "fileset should be created");
    assertEquals("comment", newFileset.comment(), "comment should not be change");
    assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    assertEquals(1, newFileset.properties().size(), "properties should not be change");
    assertEquals("v2", newFileset.properties().get("k1"), "properties should be updated");
  }

  @Test
  void testFilesetRemoveProperties() throws IOException {
    // create fileset
    String filesetName = "test_remove_fileset_properties";
    String storageLocation = defaultBaseLocation() + filesetName;

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName, storageLocation);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(schemaName, filesetName), FilesetChange.removeProperty("k1"));
    assertFilesetExists(filesetName, storageLocation);

    // verify fileset is updated
    assertNotNull(newFileset, "fileset should be created");
    assertEquals("comment", newFileset.comment(), "comment should not be change");
    assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    assertEquals(0, newFileset.properties().size(), "properties should be removed");
  }

  @Test
  void testDropCatalogWithEmptySchema() {
    String catalogName =
        GravitinoITUtils.genRandomName("test_drop_catalog_with_empty_schema_catalog");
    // Create a catalog without specifying location.
    Catalog filesetCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, PROVIDER, "comment", Collections.emptyMap());

    // Create a schema without specifying location.
    String schemaName =
        GravitinoITUtils.genRandomName("test_drop_catalog_with_empty_schema_schema");
    filesetCatalog.asSchemas().createSchema(schemaName, "comment", Collections.emptyMap());

    // Drop the empty schema.
    assertTrue(filesetCatalog.asSchemas().dropSchema(schemaName, true), "schema should be dropped");
    assertFalse(filesetCatalog.asSchemas().schemaExists(schemaName), "schema should not be exists");

    // Drop the catalog.
    assertTrue(metalake.dropCatalog(catalogName), "catalog should be dropped");
    assertFalse(metalake.catalogExists(catalogName), "catalog should not be exists");
  }

  @Test
  void testNameSpec() {
    String illegalName = "/%~?*";
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, illegalName);

    assertThrows(
        NoSuchFilesetException.class, () -> catalog.asFilesetCatalog().loadFileset(nameIdentifier));
  }

  @Test
  void testCreateFilesetWithChinese() throws IOException {
    // create fileset
    String filesetName = "test_create_fileset_with_chinese";
    String storageLocation = defaultBaseLocation() + "中文目录test";
    assertFalse(fs.exists(new Path(storageLocation)), "storage location should not exists");
    Fileset fileset =
        createFileset(
            filesetName,
            "这是中文comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1", "test", "中文测试test", "中文key", "test1"));

    // verify fileset is created
    assertFilesetExists(filesetName, storageLocation);
    assertNotNull(fileset, "fileset should be created");
    assertEquals("这是中文comment", fileset.comment());
    assertEquals(Fileset.Type.MANAGED, fileset.type());
    assertEquals(storageLocation, fileset.storageLocation());
    assertEquals(3, fileset.properties().size());
    assertEquals("v1", fileset.properties().get("k1"));
    assertEquals("中文测试test", fileset.properties().get("test"));
    assertEquals("test1", fileset.properties().get("中文key"));
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
        fs.deleteOnExit(location);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete location: {}", location, e);
      }
    }
    return catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(schemaName, filesetName), comment, type, storageLocation, properties);
  }

  private void assertFilesetExists(String filesetName, String storageLocation) throws IOException {
    assertTrue(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(schemaName, filesetName)),
        "fileset should exist");
    assertTrue(fs.exists(new Path(storageLocation)), "storage location should exist");
  }

  @AfterAll
  void stop() throws IOException {
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName);
    client.dropMetalake(metalakeName);

    if (fs != null) {
      fs.close();
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close CloseableGroup", e);
    }
  }
}
