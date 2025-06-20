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

package org.apache.gravitino.client.integration.test;

import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class CatalogIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("catalog_it_metalake");

  private static GravitinoMetalake metalake;

  private static String hmsUri;

  @BeforeAll
  public void startUp() {
    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));
  }

  @AfterAll
  public void tearDown() {
    client.dropMetalake(metalakeName, true);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  @Test
  public void testTestConnection() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    // test before creation
    Assertions.assertDoesNotThrow(
        () ->
            metalake.testConnection(
                catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties));

    // test creation
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("hive", catalog.provider());
    Assertions.assertEquals("catalog comment", catalog.comment());
    Assertions.assertTrue(catalog.properties().containsKey("metastore.uris"));

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testDropCatalog() throws IOException {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);
    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertEquals(catalogName, catalog.name());

    Assertions.assertThrows(
        CatalogAlreadyExistsException.class,
        () ->
            metalake.createCatalog(
                catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Exception exception =
        Assertions.assertThrows(
            CatalogInUseException.class, () -> metalake.dropCatalog(catalogName));
    Assertions.assertTrue(
        exception.getMessage().contains("please disable it first or use force option"),
        exception.getMessage());

    Assertions.assertDoesNotThrow(() -> metalake.disableCatalog(catalogName));
    Assertions.assertTrue(metalake.dropCatalog(catalogName), "catalog should be dropped");
    Assertions.assertFalse(metalake.dropCatalog(catalogName), "catalog should be non-existent");

    // test drop catalog with managed entity
    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", null, ImmutableMap.of());

    String schemaName = GravitinoITUtils.genRandomName("schema");
    Path schemaDir = Files.createTempDirectory(schemaName);
    catalog
        .asSchemas()
        .createSchema(schemaName, null, ImmutableMap.of("location", schemaDir.toString()));
    Assertions.assertTrue(Files.exists(schemaDir));
    Assertions.assertEquals(
        schemaDir.toString(),
        catalog.asSchemas().loadSchema(schemaName).properties().get("location"));

    String filesetName = GravitinoITUtils.genRandomName("fileset");
    Path filesetDir = Files.createTempDirectory(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(schemaName, filesetName),
            null,
            Fileset.Type.MANAGED,
            filesetDir.toString(),
            null);
    Assertions.assertTrue(Files.exists(filesetDir));
    Assertions.assertEquals(
        "file:" + filesetDir,
        catalog
            .asFilesetCatalog()
            .loadFileset(NameIdentifier.of(schemaName, filesetName))
            .storageLocation());

    Assertions.assertTrue(metalake.dropCatalog(catalogName, true));
    Assertions.assertFalse(metalake.dropCatalog(catalogName));
    Assertions.assertFalse(Files.exists(schemaDir));
    Assertions.assertFalse(Files.exists(filesetDir));
  }

  @Test
  public void testCatalogAvailable() {
    String catalogName = GravitinoITUtils.genRandomName("test_catalog");
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", ImmutableMap.of());
    Assertions.assertEquals("true", catalog.properties().get(PROPERTY_IN_USE));

    // test in-use and can't drop
    Exception exception =
        Assertions.assertThrows(
            CatalogInUseException.class, () -> metalake.dropCatalog(catalogName));
    Assertions.assertTrue(
        exception.getMessage().contains("please disable it first or use force option"),
        exception.getMessage());

    // test disable and enable again
    Assertions.assertDoesNotThrow(() -> metalake.disableCatalog(catalogName));
    Catalog loadedCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals("false", loadedCatalog.properties().get(PROPERTY_IN_USE));

    Assertions.assertDoesNotThrow(() -> metalake.enableCatalog(catalogName));
    loadedCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals("true", loadedCatalog.properties().get(PROPERTY_IN_USE));

    Assertions.assertDoesNotThrow(() -> metalake.disableCatalog(catalogName));
    exception =
        Assertions.assertThrows(
            CatalogNotInUseException.class,
            () -> metalake.alterCatalog(catalogName, CatalogChange.updateComment("new comment")));
    Assertions.assertTrue(
        exception.getMessage().contains("please enable it first"), exception.getMessage());

    // test schema operations under non-in-use catalog
    SupportsSchemas schemaOps = loadedCatalog.asSchemas();
    Assertions.assertThrows(CatalogNotInUseException.class, schemaOps::listSchemas);
    Assertions.assertThrows(
        CatalogNotInUseException.class, () -> schemaOps.createSchema("dummy", null, null));
    Assertions.assertThrows(CatalogNotInUseException.class, () -> schemaOps.loadSchema("dummy"));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () -> schemaOps.alterSchema("dummy", SchemaChange.removeProperty("dummy")));
    Assertions.assertThrows(
        CatalogNotInUseException.class, () -> schemaOps.dropSchema("dummy", false));

    // test fileset operations under non-in-use catalog
    FilesetCatalog filesetOps = loadedCatalog.asFilesetCatalog();
    Assertions.assertThrows(
        CatalogNotInUseException.class, () -> filesetOps.listFilesets(Namespace.of("dummy")));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () -> filesetOps.loadFileset(NameIdentifier.of("dummy", "dummy")));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () ->
            filesetOps.createFileset(NameIdentifier.of("dummy", "dummy"), null, null, null, null));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () -> filesetOps.dropFileset(NameIdentifier.of("dummy", "dummy")));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () -> filesetOps.getFileLocation(NameIdentifier.of("dummy", "dummy"), "dummy"));
    Assertions.assertThrows(
        CatalogNotInUseException.class,
        () ->
            filesetOps.alterFileset(
                NameIdentifier.of("dummy", "dummy"), FilesetChange.updateComment(null)));

    Assertions.assertTrue(metalake.dropCatalog(catalogName), "catalog should be dropped");
    Assertions.assertFalse(metalake.dropCatalog(catalogName), "catalog should be non-existent");
  }

  @Test
  public void testCreateCatalogWithoutProperties() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", null);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.FILESET, catalog.type());
    Assertions.assertEquals("fileset", catalog.provider());
    Assertions.assertEquals("catalog comment", catalog.comment());
    Assertions.assertEquals("true", catalog.properties().get(PROPERTY_IN_USE));
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);

    // test cloud related properties
    ImmutableMap<String, String> illegalProps = ImmutableMap.of("cloud.name", "myCloud");
    // test before creation
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.testConnection(
                    catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", illegalProps));
    Assertions.assertTrue(exception.getMessage().contains("Invalid value [myCloud]"));

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", illegalProps));
    Assertions.assertTrue(exception.getMessage().contains("Invalid value [myCloud]"));

    ImmutableMap<String, String> props =
        ImmutableMap.of("cloud.name", "aws", "cloud.region-code", "us-west-2");
    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", props);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertFalse(catalog.properties().isEmpty());
    Assertions.assertEquals("aws", catalog.properties().get("cloud.name"));
    Assertions.assertEquals("us-west-2", catalog.properties().get("cloud.region-code"));
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
  }

  @Test
  public void testCreateCatalogWithChinese() {
    String catalogName = GravitinoITUtils.genRandomName("catalogz");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, "hive", "这是中文comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("hive", catalog.provider());
    Assertions.assertEquals("这是中文comment", catalog.comment());
    Assertions.assertTrue(catalog.properties().containsKey("metastore.uris"));

    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
  }

  @Test
  public void testListCatalogsInfo() {
    String relCatalogName = GravitinoITUtils.genRandomName("rel_catalog_");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    metalake.createCatalog(
        relCatalogName, Catalog.Type.RELATIONAL, "hive", "relational catalog comment", properties);
    Catalog relCatalog = metalake.loadCatalog(relCatalogName);

    String fileCatalogName = GravitinoITUtils.genRandomName("file_catalog_");
    metalake.createCatalog(
        fileCatalogName,
        Catalog.Type.FILESET,
        "hadoop",
        "file catalog comment",
        Collections.emptyMap());
    Catalog fileCatalog = metalake.loadCatalog(fileCatalogName);

    Catalog[] catalogs = metalake.listCatalogsInfo();
    for (Catalog catalog : catalogs) {
      if (catalog.name().equals(relCatalogName)) {
        assertCatalogEquals(relCatalog, catalog);
      } else if (catalog.name().equals(fileCatalogName)) {
        assertCatalogEquals(fileCatalog, catalog);
      }
    }
    Assertions.assertTrue(ArrayUtils.contains(catalogs, relCatalog));
    Assertions.assertTrue(ArrayUtils.contains(catalogs, fileCatalog));

    metalake.disableCatalog(relCatalogName);
    metalake.dropCatalog(relCatalogName);

    metalake.disableCatalog(fileCatalogName);
    metalake.dropCatalog(fileCatalogName);
  }

  private void assertCatalogEquals(Catalog catalog1, Catalog catalog2) {
    Assertions.assertEquals(catalog1.name(), catalog2.name());
    Assertions.assertEquals(catalog1.type(), catalog2.type());
    Assertions.assertEquals(catalog1.provider(), catalog2.provider());
    Assertions.assertEquals(catalog1.comment(), catalog2.comment());
  }

  @Test
  @DisabledIfSystemProperty(named = "testMode", matches = "embedded")
  public void testCreateCatalogWithPackage() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome);
    String packagePath = String.join(File.separator, gravitinoHome, "catalogs", "hive");
    properties.put("package", packagePath);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("hive", catalog.provider());
    Assertions.assertEquals("catalog comment", catalog.comment());
    Assertions.assertTrue(catalog.properties().containsKey("package"));

    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);

    // Test using invalid package path
    String catalogName1 = GravitinoITUtils.genRandomName("catalog");
    properties.put("package", "/tmp/none_exist_path_to_package");
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogName1, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties));
    Assertions.assertTrue(
        exception.getMessage().contains("Invalid package path: /tmp/none_exist_path_to_package"));
  }

  @Test
  void testUpdateCatalogWithNullableComment() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog catalog =
        metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, "hive", null, properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertNull(catalog.comment());

    Catalog updatedCatalog =
        metalake.alterCatalog(catalogName, CatalogChange.updateComment("new catalog comment"));
    Assertions.assertEquals("new catalog comment", updatedCatalog.comment());

    Catalog updateNullComment =
        metalake.alterCatalog(catalogName, CatalogChange.updateComment(null));
    Assertions.assertNull(updateNullComment.comment());

    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
  }

  @Test
  public void testAlterCatalogProperties() {
    String cloudName = "aws";
    String alterCloudName = "azure";
    String regionCode = "us-east-1";
    String alterRegionCode = "us-west-2";

    String catalogName = GravitinoITUtils.genRandomName("test_catalog");
    ImmutableMap<String, String> props =
        ImmutableMap.of(Catalog.CLOUD_NAME, cloudName, Catalog.CLOUD_REGION_CODE, regionCode);
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", props);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertFalse(catalog.properties().isEmpty());
    Assertions.assertEquals(cloudName, catalog.properties().get(Catalog.CLOUD_NAME));
    Assertions.assertEquals(regionCode, catalog.properties().get(Catalog.CLOUD_REGION_CODE));

    Catalog alteredCatalog =
        metalake.alterCatalog(
            catalogName,
            CatalogChange.setProperty(Catalog.CLOUD_NAME, alterCloudName),
            CatalogChange.setProperty(Catalog.CLOUD_REGION_CODE, alterRegionCode));

    Assertions.assertEquals(alterCloudName, alteredCatalog.properties().get(Catalog.CLOUD_NAME));
    Assertions.assertEquals(
        alterRegionCode, alteredCatalog.properties().get(Catalog.CLOUD_REGION_CODE));
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
  }
}
