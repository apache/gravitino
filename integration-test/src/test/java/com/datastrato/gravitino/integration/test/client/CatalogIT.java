/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class CatalogIT extends AbstractIT {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("catalog_it_metalake");

  private static GravitinoMetalake metalake;

  private static String hmsUri;

  @BeforeAll
  public static void startUp() {
    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    NameIdentifier ident = NameIdentifier.of(metalakeName);
    Assertions.assertFalse(client.metalakeExists(ident));
    metalake = client.createMetalake(ident, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(ident));
  }

  @AfterAll
  public static void tearDown() {
    client.dropMetalake(NameIdentifier.of(metalakeName));

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
  public void testCreateCatalog() {
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogIdent));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog catalog =
        metalake.createCatalog(
            catalogIdent, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogIdent));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("hive", catalog.provider());
    Assertions.assertEquals("catalog comment", catalog.comment());
    Assertions.assertTrue(catalog.properties().containsKey("metastore.uris"));

    metalake.dropCatalog(catalogIdent);
  }

  @Test
  public void testListCatalogsInfo() {
    String relCatalogName = GravitinoITUtils.genRandomName("rel_catalog_");
    NameIdentifier relCatalogIdent = NameIdentifier.of(metalakeName, relCatalogName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog relCatalog =
        metalake.createCatalog(
            relCatalogIdent,
            Catalog.Type.RELATIONAL,
            "hive",
            "relational catalog comment",
            properties);

    String fileCatalogName = GravitinoITUtils.genRandomName("file_catalog_");
    NameIdentifier fileCatalogIdent = NameIdentifier.of(metalakeName, fileCatalogName);
    Catalog fileCatalog =
        metalake.createCatalog(
            fileCatalogIdent,
            Catalog.Type.FILESET,
            "hadoop",
            "file catalog comment",
            Collections.emptyMap());

    Catalog[] catalogs = metalake.listCatalogsInfo(relCatalogIdent.namespace());
    for (Catalog catalog : catalogs) {
      if (catalog.name().equals(relCatalogName)) {
        assertCatalogEquals(relCatalog, catalog);
      } else if (catalog.name().equals(fileCatalogName)) {
        assertCatalogEquals(fileCatalog, catalog);
      }
    }
    // TODO: uncomment this after fixing hidden properties
    // Assertions.assertTrue(ArrayUtils.contains(catalogs, relCatalog));
    // Assertions.assertTrue(ArrayUtils.contains(catalogs, fileCatalog));
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
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogIdent));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome);
    String packagePath = String.join(File.separator, gravitinoHome, "catalogs", "hive");
    properties.put("package", packagePath);

    Catalog catalog =
        metalake.createCatalog(
            catalogIdent, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogIdent));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("hive", catalog.provider());
    Assertions.assertEquals("catalog comment", catalog.comment());
    Assertions.assertTrue(catalog.properties().containsKey("package"));

    metalake.dropCatalog(catalogIdent);

    // Test using invalid package path
    String catalogName1 = GravitinoITUtils.genRandomName("catalog");
    NameIdentifier catalogIdent1 = NameIdentifier.of(metalakeName, catalogName1);
    properties.put("package", "/tmp/none_exist_path_to_package");
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogIdent1, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties));
    Assertions.assertTrue(
        exception.getMessage().contains("Invalid package path: /tmp/none_exist_path_to_package"));
  }
}
