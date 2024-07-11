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
package org.apache.gravitino.flink.connector.integration.test.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.assertColumns;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.types.Row;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlinkHiveCatalogIT extends FlinkCommonIT {
  private static final String DEFAULT_HIVE_CATALOG = "test_flink_hive_schema_catalog";

  private static org.apache.gravitino.Catalog hiveCatalog;

  @BeforeAll
  static void hiveStartUp() {
    initDefaultHiveCatalog();
  }

  @AfterAll
  static void hiveStop() {
    Preconditions.checkNotNull(metalake);
    metalake.dropCatalog(DEFAULT_HIVE_CATALOG);
  }

  protected static void initDefaultHiveCatalog() {
    Preconditions.checkNotNull(metalake);
    hiveCatalog =
        metalake.createCatalog(
            DEFAULT_HIVE_CATALOG,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of("metastore.uris", hiveMetastoreUri));
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
    configuration.set(GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS, hiveMetastoreUri);
    CatalogDescriptor catalogDescriptor = CatalogDescriptor.of(catalogName, configuration);
    tableEnv.createCatalog(catalogName, catalogDescriptor);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the catalog properties.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(METASTORE_URIS));
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
                + "'hive.metastore.uris'='%s',"
                + "'unknown.key'='unknown.value'"
                + ")",
            catalogName, hiveMetastoreUri));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the properties of the created catalog.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(METASTORE_URIS));
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
    org.apache.gravitino.Catalog gravitinoCatalog =
        metalake.createCatalog(
            catalogName,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of(
                "flink.bypass.hive-conf-dir",
                "src/test/resources/flink-tests",
                "flink.bypass.hive.test",
                "hive.config",
                "metastore.uris",
                hiveMetastoreUri));
    Assertions.assertNotNull(gravitinoCatalog);
    Assertions.assertEquals(catalogName, gravitinoCatalog.name());
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs + 1, tableEnv.listCatalogs().length, "Should create a new catalog");

    // get the catalog from Gravitino.
    Optional<Catalog> flinkHiveCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(flinkHiveCatalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, flinkHiveCatalog.get());
    GravitinoHiveCatalog gravitinoHiveCatalog = (GravitinoHiveCatalog) flinkHiveCatalog.get();
    HiveConf hiveConf = gravitinoHiveCatalog.getHiveConf();
    Assertions.assertTrue(hiveConf.size() > 0, "Should have hive conf");
    Assertions.assertEquals("hive.config", hiveConf.get("hive.test"));
    Assertions.assertEquals(
        hiveMetastoreUri, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    // drop the catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testHivePartitionTable() {
    String databaseName = "test_create_hive_partition_table_db";
    String tableName = "test_create_hive_partition_table";
    String comment = "test comment";
    String key = "test key";
    String value = "test value";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type')"
                      + " COMMENT '%s' "
                      + " PARTITIONED BY (string_type, double_type)"
                      + " WITH ("
                      + "'%s' = '%s')",
                  tableName, comment, key, value);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertNotNull(table);
          Assertions.assertEquals(comment, table.comment());
          Assertions.assertEquals(value, table.properties().get(key));
          Column[] columns =
              new Column[] {
                Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
                Column.of("double_type", Types.DoubleType.get(), "double_type")
              };
          assertColumns(columns, table.columns());
          Transform[] partitions =
              new Transform[] {
                Transforms.identity("string_type"), Transforms.identity("double_type")
              };
          Assertions.assertArrayEquals(partitions, table.partitioning());

          // load flink catalog
          try {
            Catalog flinkCatalog = tableEnv.getCatalog(currentCatalog().name()).get();
            CatalogBaseTable flinkTable =
                flinkCatalog.getTable(ObjectPath.fromString(databaseName + "." + tableName));
            ResolvedCatalogTable resolvedCatalogTable = (ResolvedCatalogTable) flinkTable;
            Assertions.assertTrue(resolvedCatalogTable.isPartitioned());
            Assertions.assertArrayEquals(
                new String[] {"string_type", "double_type"},
                resolvedCatalogTable.getPartitionKeys().toArray());
          } catch (Exception e) {
            Assertions.fail("Table should be exist", e);
          }

          // write and read datas
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES ('A', 1.0), ('B', 2.0)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of("A", 1.0),
              Row.of("B", 2.0));
          try {
            Assertions.assertTrue(
                hdfs.exists(
                    new Path(
                        table.properties().get("location"), "/string_type=A/double_type=1.0")));
            Assertions.assertTrue(
                hdfs.exists(
                    new Path(
                        table.properties().get("location"), "/string_type=B/double_type=2.0")));
          } catch (IOException e) {
            Assertions.fail("The partition directory should be exist.", e);
          }
        },
        true);
  }

  @Override
  protected org.apache.gravitino.Catalog currentCatalog() {
    return hiveCatalog;
  }
}
