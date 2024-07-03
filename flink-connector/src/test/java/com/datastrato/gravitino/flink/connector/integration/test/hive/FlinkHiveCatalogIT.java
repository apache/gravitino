/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import com.datastrato.gravitino.flink.connector.integration.test.FlinkCommonIT;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class FlinkHiveCatalogIT extends FlinkCommonIT {

  private static com.datastrato.gravitino.Catalog hiveCatalog;

  @BeforeAll
  static void hiveStartUp() {
    hiveCatalog =
        metalake.createCatalog(
            "test_flink_hive_schema_catalog",
            com.datastrato.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of("metastore.uris", hiveMetastoreUri));
  }

  @AfterAll
  static void hiveStop() {
    metalake.dropCatalog("test_flink_hive_schema_catalog");
  }

  @Test
  public void testCreateGravitinoHiveCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.
    String catalogName = "gravitino_hive";
    Configuration configuration = new Configuration();
    configuration.set(
        CommonCatalogOptions.CATALOG_TYPE, GravitinoHiveCatalogFactoryOptions.IDENTIFIER);
    configuration.set(HiveCatalogFactoryOptions.HIVE_CONF_DIR, "src/test/resources/flink-tests");
    configuration.set(
        GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS, "thrift://127.0.0.1:9084");
    CatalogDescriptor catalogDescriptor = CatalogDescriptor.of(catalogName, configuration);
    tableEnv.createCatalog(catalogName, catalogDescriptor);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the catalog properties.
    com.datastrato.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals("thrift://127.0.0.1:9084", properties.get(METASTORE_URIS));
    Map<String, String> flinkProperties =
        gravitinoCatalog.properties().entrySet().stream()
            .filter(e -> e.getKey().startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(2, flinkProperties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        flinkProperties.get(flinkByPass(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key())));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
        flinkProperties.get(flinkByPass(CommonCatalogOptions.CATALOG_TYPE.key())));

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, catalog.get());

    // List catalogs.
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    Assertions.assertEquals(
        DEFAULT_CATALOG,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be default_catalog in flink");

    // Change the current catalog to the new created catalog.
    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    // Drop the catalog. Only support drop catalog by SQL.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testCreateGravitinoHiveCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.
    String catalogName = "gravitino_hive_sql";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-hive', "
                + "'hive-conf-dir'='src/test/resources/flink-tests',"
                + "'hive.metastore.uris'='thrift://127.0.0.1:9084',"
                + "'unknown.key'='unknown.value'"
                + ")",
            catalogName));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the properties of the created catalog.
    com.datastrato.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals("thrift://127.0.0.1:9084", properties.get(METASTORE_URIS));
    Map<String, String> flinkProperties =
        properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(3, flinkProperties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        flinkProperties.get(flinkByPass(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key())));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
        flinkProperties.get(flinkByPass(CommonCatalogOptions.CATALOG_TYPE.key())));
    Assertions.assertEquals(
        "unknown.value",
        flinkProperties.get(flinkByPass("unknown.key")),
        "The unknown.key will not cause failure and will be saved in Gravitino.");

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, catalog.get());

    // List catalogs.
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    // Use SQL to list catalogs.
    TableResult result = tableEnv.executeSql("show catalogs");
    Assertions.assertEquals(
        numCatalogs + 1, Lists.newArrayList(result.collect()).size(), "Should have 2 catalogs");

    Assertions.assertEquals(
        DEFAULT_CATALOG,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be default_catalog in flink");

    // Change the current catalog to the new created catalog.
    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    // Drop the catalog. Only support using SQL to drop catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testCreateGravitinoHiveCatalogRequireOptions() {
    tableEnv.useCatalog(DEFAULT_CATALOG);

    // Failed to create the catalog for missing the required options.
    String catalogName = "gravitino_hive_sql2";
    Assertions.assertThrows(
        ValidationException.class,
        () -> {
          tableEnv.executeSql(
              String.format(
                  "create catalog %s with ("
                      + "'type'='gravitino-hive', "
                      + "'hive-conf-dir'='src/test/resources/flink-tests'"
                      + ")",
                  catalogName));
        },
        "The hive.metastore.uris is required.");

    Assertions.assertFalse(metalake.catalogExists(catalogName));
  }

  @Test
  public void testGetCatalogFromGravitino() {
    // list catalogs.
    int numCatalogs = tableEnv.listCatalogs().length;

    // create a new catalog.
    String catalogName = "hive_catalog_in_gravitino";
    com.datastrato.gravitino.Catalog gravitinoCatalog =
        metalake.createCatalog(
            catalogName,
            com.datastrato.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of(
                "flink.bypass.hive-conf-dir",
                "src/test/resources/flink-tests",
                "flink.bypass.hive.test",
                "hive.config",
                "metastore.uris",
                "thrift://127.0.0.1:9084"));
    Assertions.assertNotNull(gravitinoCatalog);
    Assertions.assertEquals(catalogName, gravitinoCatalog.name());
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs + 1, tableEnv.listCatalogs().length, "Should create a new catalog");

    // get the catalog from gravitino.
    Optional<Catalog> flinkHiveCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(flinkHiveCatalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, flinkHiveCatalog.get());
    GravitinoHiveCatalog gravitinoHiveCatalog = (GravitinoHiveCatalog) flinkHiveCatalog.get();
    HiveConf hiveConf = gravitinoHiveCatalog.getHiveConf();
    Assertions.assertTrue(hiveConf.size() > 0, "Should have hive conf");
    Assertions.assertEquals("hive.config", hiveConf.get("hive.test"));
    Assertions.assertEquals(
        "thrift://127.0.0.1:9084", hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    // drop the catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Override
  protected com.datastrato.gravitino.Catalog currentCatalog() {
    return hiveCatalog;
  }
}
