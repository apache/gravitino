/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.fileset;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class CatalogFilesetIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogFilesetIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public static final String metalakeName =
      GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
  public static final String catalogName =
      GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
  public static final String SCHEMA_PREFIX = "CatalogFilesetIT_schema";
  public static final String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  private static final String provider = "hadoop";
  private static GravitinoMetaLake metalake;
  private static Catalog catalog;
  private static FileSystem hdfs;
  private static String defaultBaseLocation;

  @BeforeAll
  public static void setup() throws IOException {
    containerSuite.startHiveContainer();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    hdfs = FileSystem.get(conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public static void stop() throws IOException {
    client.dropMetalake(NameIdentifier.of(metalakeName));

    if (hdfs != null) {
      hdfs.close();
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
  }

  private static void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    metalake.createCatalog(
        NameIdentifier.of(metalakeName, catalogName),
        Catalog.Type.FILESET,
        provider,
        "comment",
        ImmutableMap.of());

    catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
  }

  private static void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put("location", defaultBaseLocation());
    String comment = "comment";

    catalog.asSchemas().createSchema(ident, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
    Assertions.assertNotNull(loadSchema.properties().get(HiveSchemaPropertiesMetadata.LOCATION));
  }

  private static void dropSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    catalog.asSchemas().dropSchema(ident, true);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(ident));
  }

  @Test
  public void testCreateFileset() throws IOException {
    // create fileset
    String filesetName = "test_create_fileset";
    String storageLocation = storageLocation(filesetName);
    Assertions.assertFalse(
        hdfs.exists(new Path(storageLocation)), "storage location should not exists");
    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName);
    Assertions.assertNotNull(fileset, "fileset should be created");
    Assertions.assertEquals("comment", fileset.comment());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset.type());
    Assertions.assertEquals(storageLocation, fileset.storageLocation());
    Assertions.assertEquals(1, fileset.properties().size());
    Assertions.assertEquals("v1", fileset.properties().get("k1"));

    // test create a fileset that already exist
    Assertions.assertThrows(
        FilesetAlreadyExistsException.class,
        () ->
            createFileset(
                filesetName,
                "comment",
                Fileset.Type.MANAGED,
                storageLocation,
                ImmutableMap.of("k1", "v1")),
        "Should throw FilesetAlreadyExistsException when fileset already exists");

    // create fileset with null storage location
    String filesetName2 = "test_create_fileset_no_storage_location";
    Fileset fileset2 = createFileset(filesetName2, null, Fileset.Type.MANAGED, null, null);
    assertFilesetExists(filesetName2);
    Assertions.assertNotNull(fileset2, "fileset should be created");
    Assertions.assertNull(fileset2.comment(), "comment should be null");
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset2.type(), "type should be MANAGED");
    Assertions.assertEquals(
        storageLocation(filesetName2),
        fileset2.storageLocation(),
        "storage location should be created");
    Assertions.assertEquals(ImmutableMap.of(), fileset2.properties(), "properties should be empty");

    // create fileset with null fileset name
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            createFileset(
                null,
                "comment",
                Fileset.Type.MANAGED,
                storageLocation,
                ImmutableMap.of("k1", "v1")),
        "Should throw IllegalArgumentException when fileset name is null");

    // create fileset with null fileset type
    String filesetName3 = "test_create_fileset_no_type";
    String storageLocation3 = storageLocation(filesetName3);
    Fileset fileset3 =
        createFileset(filesetName3, "comment", null, storageLocation3, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName3);
    Assertions.assertEquals(
        Fileset.Type.MANAGED, fileset3.type(), "fileset type should be MANAGED by default");
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
    Assertions.assertEquals(1, fileset.properties().size());
    Assertions.assertEquals("v1", fileset.properties().get("k1"));
    Assertions.assertTrue(
        hdfs.exists(new Path(storageLocation)), "storage location should be created");

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
  public void testLoadFileset() throws IOException {
    // create fileset
    String filesetName = "test_load_fileset";
    String storageLocation = storageLocation(filesetName);

    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // test load fileset
    Fileset loadFileset =
        catalog
            .asFilesetCatalog()
            .loadFileset(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName));
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
        () ->
            catalog
                .asFilesetCatalog()
                .loadFileset(NameIdentifier.of(metalakeName, catalogName, schemaName, "not_exist")),
        "Should throw NoSuchFilesetException when fileset does not exist");
  }

  @Test
  public void testDropManagedFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_managed_fileset";
    String storageLocation = storageLocation(filesetName);

    Assertions.assertFalse(
        hdfs.exists(new Path(storageLocation)), "storage location should not exists");

    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // drop fileset
    boolean dropped =
        catalog
            .asFilesetCatalog()
            .dropFileset(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName));
    Assertions.assertTrue(dropped, "fileset should be dropped");

    // verify fileset is dropped
    Assertions.assertFalse(
        catalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)),
        "fileset should not be exists");
    Assertions.assertFalse(
        hdfs.exists(new Path(storageLocation)), "storage location should be dropped");
  }

  @Test
  public void testDropExternalFileset() throws IOException {
    // create fileset
    String filesetName = "test_drop_external_fileset";
    String storageLocation = storageLocation(filesetName);

    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.EXTERNAL,
            storageLocation,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // drop fileset
    boolean dropped =
        catalog
            .asFilesetCatalog()
            .dropFileset(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName));
    Assertions.assertTrue(dropped, "fileset should be dropped");

    // verify fileset is dropped
    Assertions.assertFalse(
        catalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)),
        "fileset should not be exists");
    Assertions.assertTrue(
        hdfs.exists(new Path(storageLocation)), "storage location should not be dropped");
  }

  @Test
  public void testListFilesets() throws IOException {
    // clear schema
    dropSchema();
    createSchema();

    // test no fileset exists
    NameIdentifier[] nameIdentifiers =
        catalog
            .asFilesetCatalog()
            .listFilesets(Namespace.ofFileset(metalakeName, catalogName, schemaName));
    Assertions.assertEquals(0, nameIdentifiers.length, "should have no fileset");

    // create fileset1
    String filesetName1 = "test_list_filesets1";
    String storageLocation = storageLocation(filesetName1);

    Fileset fileset1 =
        createFileset(
            filesetName1,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName1);

    // create fileset2
    String filesetName2 = "test_list_filesets2";
    String storageLocation2 = storageLocation(filesetName2);

    Fileset fileset2 =
        createFileset(
            filesetName2,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation2,
            ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName2);

    // list filesets
    NameIdentifier[] nameIdentifiers1 =
        catalog
            .asFilesetCatalog()
            .listFilesets(Namespace.ofFileset(metalakeName, catalogName, schemaName));
    Arrays.sort(nameIdentifiers1, Comparator.comparing(NameIdentifier::name));
    Assertions.assertEquals(2, nameIdentifiers1.length, "should have 2 filesets");
    Assertions.assertEquals(fileset1.name(), nameIdentifiers1[0].name());
    Assertions.assertEquals(fileset2.name(), nameIdentifiers1[1].name());
  }

  @Test
  public void testRenameFileset() throws IOException {
    // create fileset
    String filesetName = "test_rename_fileset";
    String storageLocation = storageLocation(filesetName);

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // rename fileset
    String newFilesetName = "test_rename_fileset_new";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
                FilesetChange.rename(newFilesetName));

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals(newFilesetName, newFileset.name(), "fileset name should be updated");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(1, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v1", newFileset.properties().get("k1"), "properties should not be change");
  }

  @Test
  public void testFilesetUpdateComment() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_comment";
    String storageLocation = storageLocation(filesetName);

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset comment
    String newComment = "new_comment";
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
                FilesetChange.updateComment(newComment));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals(newComment, newFileset.comment(), "comment should be updated");
    Assertions.assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(1, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v1", newFileset.properties().get("k1"), "properties should not be change");
  }

  @Test
  public void testFilesetSetProperties() throws IOException {
    // create fileset
    String filesetName = "test_update_fileset_properties";
    String storageLocation = storageLocation(filesetName);

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
                FilesetChange.setProperty("k1", "v2"));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(1, newFileset.properties().size(), "properties should not be change");
    Assertions.assertEquals(
        "v2", newFileset.properties().get("k1"), "properties should be updated");
  }

  @Test
  public void testFilesetRemoveProperties() throws IOException {
    // create fileset
    String filesetName = "test_remove_fileset_properties";
    String storageLocation = storageLocation(filesetName);

    createFileset(
        filesetName, "comment", Fileset.Type.MANAGED, storageLocation, ImmutableMap.of("k1", "v1"));
    assertFilesetExists(filesetName);

    // update fileset properties
    Fileset newFileset =
        catalog
            .asFilesetCatalog()
            .alterFileset(
                NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
                FilesetChange.removeProperty("k1"));
    assertFilesetExists(filesetName);

    // verify fileset is updated
    Assertions.assertNotNull(newFileset, "fileset should be created");
    Assertions.assertEquals("comment", newFileset.comment(), "comment should not be change");
    Assertions.assertEquals(Fileset.Type.MANAGED, newFileset.type(), "type should not be change");
    Assertions.assertEquals(
        storageLocation, newFileset.storageLocation(), "storage location should not be change");
    Assertions.assertEquals(0, newFileset.properties().size(), "properties should be removed");
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
        hdfs.deleteOnExit(location);
      } catch (IOException e) {
        LOG.warn("Failed to delete location: {}", location, e);
      }
    }

    return catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
            comment,
            type,
            storageLocation,
            properties);
  }

  private void assertFilesetExists(String filesetName) throws IOException {
    Assertions.assertTrue(
        catalog
            .asFilesetCatalog()
            .filesetExists(NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)),
        "fileset should be exists");
    Assertions.assertTrue(
        hdfs.exists(new Path(storageLocation(filesetName))), "storage location should be exists");
  }

  private static String defaultBaseLocation() {
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

  private static String storageLocation(String filesetName) {
    return defaultBaseLocation() + "/" + filesetName;
  }
}
