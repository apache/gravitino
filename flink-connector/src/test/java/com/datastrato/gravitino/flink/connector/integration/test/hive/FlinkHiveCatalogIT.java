/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import com.datastrato.gravitino.flink.connector.integration.test.FlinkEnvIT;
import com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlinkHiveCatalogIT extends FlinkEnvIT {

  private static final String DEFAULT_CATALOG = "default_catalog";

  @Test
  public void testCreateGravitinoHiveCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);

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
    Assertions.assertEquals(3, catalogs.length, "Should create a new catalog");
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
    Assertions.assertEquals(3, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    // Use SQL to list catalogs.
    TableResult result = tableEnv.executeSql("show catalogs");
    Assertions.assertEquals(
        3, Lists.newArrayList(result.collect()).size(), "Should have 2 catalogs");

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
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(2, catalogs.length, "Only have 2 catalog");

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
    Assertions.assertEquals(3, tableEnv.listCatalogs().length, "Should create a new catalog");

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
        2, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testCreateSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    com.datastrato.gravitino.Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    String schema = "test_create_schema";
    try {
      TableResult tableResult =
          tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema));
      TestUtils.assertTableResult(tableResult, ResultKind.SUCCESS);
      catalog.asSchemas().schemaExists(schema);
    } finally {
      catalog.asSchemas().dropSchema(schema, true);
      Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
    }
  }

  @Test
  public void testGetSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    com.datastrato.gravitino.Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    String schema = "test_get_schema";
    String comment = "test comment";
    String propertyKey = "key1";
    String propertyValue = "value1";
    String location = warehouse + "/" + schema;

    try {
      TestUtils.assertTableResult(
          tableEnv.executeSql(
              String.format(
                  "CREATE DATABASE IF NOT EXISTS %s COMMENT '%s' WITH ('%s'='%s', '%s'='%s')",
                  schema, comment, propertyKey, propertyValue, "location", location)),
          ResultKind.SUCCESS);
      TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

      catalog.asSchemas().schemaExists(schema);
      Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
      Assertions.assertEquals(schema, loadedSchema.name());
      Assertions.assertEquals(comment, loadedSchema.comment());
      Assertions.assertEquals(2, loadedSchema.properties().size());
      Assertions.assertEquals(propertyValue, loadedSchema.properties().get(propertyKey));
      Assertions.assertEquals(
          location, loadedSchema.properties().get(HiveSchemaPropertiesMetadata.LOCATION));
    } finally {
      catalog.asSchemas().dropSchema(schema, true);
      Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
    }
  }

  @Test
  public void testListSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    com.datastrato.gravitino.Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
    String schema = "test_list_schema";
    String schema2 = "test_list_schema2";
    String schema3 = "test_list_schema3";

    try {
      TestUtils.assertTableResult(
          tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema)),
          ResultKind.SUCCESS);

      TestUtils.assertTableResult(
          tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema2)),
          ResultKind.SUCCESS);

      TestUtils.assertTableResult(
          tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema3)),
          ResultKind.SUCCESS);
      TestUtils.assertTableResult(
          tableEnv.executeSql("SHOW DATABASES"),
          ResultKind.SUCCESS_WITH_CONTENT,
          Row.of("default"),
          Row.of(schema),
          Row.of(schema2),
          Row.of(schema3));

      String[] schemas = catalog.asSchemas().listSchemas();
      Assertions.assertEquals(4, schemas.length);
      Assertions.assertEquals("default", schemas[0]);
      Assertions.assertEquals(schema, schemas[1]);
      Assertions.assertEquals(schema2, schemas[2]);
      Assertions.assertEquals(schema3, schemas[3]);
    } finally {
      catalog.asSchemas().dropSchema(schema, true);
      catalog.asSchemas().dropSchema(schema2, true);
      catalog.asSchemas().dropSchema(schema3, true);
      Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
    }
  }

  @Test
  public void testAlterSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    com.datastrato.gravitino.Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    String schema = "test_alter_schema";

    try {
      TestUtils.assertTableResult(
          tableEnv.executeSql(
              String.format(
                  "CREATE DATABASE IF NOT EXISTS %s "
                      + "COMMENT 'test comment'"
                      + "WITH ('key1' = 'value1', 'key2'='value2')",
                  schema)),
          ResultKind.SUCCESS);

      Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
      Assertions.assertEquals(schema, loadedSchema.name());
      Assertions.assertEquals("test comment", loadedSchema.comment());
      Assertions.assertEquals(3, loadedSchema.properties().size());
      Assertions.assertEquals("value1", loadedSchema.properties().get("key1"));
      Assertions.assertEquals("value2", loadedSchema.properties().get("key2"));
      Assertions.assertNotNull(loadedSchema.properties().get("location"));

      TestUtils.assertTableResult(
          tableEnv.executeSql(
              String.format("ALTER DATABASE %s SET ('key1'='new-value', 'key3'='value3')", schema)),
          ResultKind.SUCCESS);
      Schema reloadedSchema = catalog.asSchemas().loadSchema(schema);
      Assertions.assertEquals(schema, reloadedSchema.name());
      Assertions.assertEquals("test comment", reloadedSchema.comment());
      Assertions.assertEquals(4, reloadedSchema.properties().size());
      Assertions.assertEquals("new-value", reloadedSchema.properties().get("key1"));
      Assertions.assertEquals("value3", reloadedSchema.properties().get("key3"));
    } finally {
      catalog.asSchemas().dropSchema(schema, true);
    }
  }
}
