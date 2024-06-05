/*
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
package com.datastrato.gravitino.flink.connector.integration.test.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils.assertColumns;
import static com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import com.datastrato.gravitino.flink.connector.integration.test.FlinkCommonIT;
import com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import jline.internal.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class FlinkHiveCatalogIT extends FlinkCommonIT {
  private static final String DEFAULT_HIVE_CATALOG = "test_flink_hive_schema_catalog";
  private static final String DEFAULT_HIVE_SCHEMA = "test_flink_hive_schema";

  private static com.datastrato.gravitino.Catalog hiveCatalog;

  @BeforeAll
  static void hiveStartUp() {
    initDefaultHiveCatalog();
    initDefaultHiveSchema();
  }

  @AfterAll
  static void hiveStop() {
    hiveCatalog.asSchemas().dropSchema(DEFAULT_HIVE_SCHEMA, true);
    metalake.dropCatalog(DEFAULT_HIVE_CATALOG);
  }

  protected static void initDefaultHiveCatalog() {
    Preconditions.checkNotNull(metalake);
    hiveCatalog =
        metalake.createCatalog(
            DEFAULT_HIVE_CATALOG,
            com.datastrato.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of("metastore.uris", hiveMetastoreUri));
  }

  protected static void initDefaultHiveSchema() {
    Preconditions.checkNotNull(metalake);
    Preconditions.checkNotNull(hiveCatalog);
    if (!hiveCatalog.asSchemas().schemaExists(DEFAULT_HIVE_SCHEMA)) {
      hiveCatalog.asSchemas().createSchema(DEFAULT_HIVE_SCHEMA, null, ImmutableMap.of());
    }
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

    // get the catalog from Gravitino.
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

  @Test
  public void testCreateNoPartitionTable() {
    String tableName, comment;
    tableName = "test_create_no_partition_table";
    comment = "test comment";
    String key = "test key";
    String value = "test value";

    // 1. The NOT NULL constraint for column is only supported since Hive 3.0,
    // but the current Gravitino Hive catalog only supports Hive 2.x.
    // 2. Hive doesn't support Time and Timestamp with timezone type.
    // 3. Flink SQL only support to create Interval Month and Second(3).
    doWithSchema(
        hiveCatalog,
        DEFAULT_HIVE_SCHEMA,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type'"
                      + " COMMENT '%s' WITH ("
                      + "'%s' = '%s')",
                  tableName, comment, key, value);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Table table =
              catalog
                  .asTableCatalog()
                  .loadTable(
                      NameIdentifier.of(
                          metalake.name(), DEFAULT_HIVE_CATALOG, DEFAULT_HIVE_SCHEMA, tableName));
          Assertions.assertNotNull(table);
          Assertions.assertEquals(comment, table.comment());
          Assertions.assertEquals(value, table.properties().get(key));
          Column[] columns =
              new Column[] {
                Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
                Column.of("double_type", Types.DoubleType.get(), "double_type")
              };
          assertColumns(columns, table.columns());
          Assertions.assertArrayEquals(EMPTY_TRANSFORM, table.partitioning());
        });
  }

  @Test
  public void testListTables() {
    com.datastrato.gravitino.Catalog currentCatalog = metalake.loadCatalog(DEFAULT_HIVE_CATALOG);
    String newSchema = "test_list_table_catalog";
    try {
      Column[] columns = new Column[] {Column.of("user_id", Types.IntegerType.get(), "USER_ID")};
      doWithSchema(
          currentCatalog,
          newSchema,
          catalog -> {
            catalog
                .asTableCatalog()
                .createTable(
                    NameIdentifier.of(
                        metalake.name(), DEFAULT_HIVE_CATALOG, newSchema, "test_table1"),
                    columns,
                    "comment1",
                    ImmutableMap.of());
            catalog
                .asTableCatalog()
                .createTable(
                    NameIdentifier.of(
                        metalake.name(), DEFAULT_HIVE_CATALOG, newSchema, "test_table2"),
                    columns,
                    "comment2",
                    ImmutableMap.of());
            TableResult result = sql("SHOW TABLES");
            TestUtils.assertTableResult(
                result,
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of("test_table1"),
                Row.of("test_table2"));
          });
    } finally {
      currentCatalog.asSchemas().dropSchema(newSchema, true);
    }
  }

  @Test
  public void testDropTable() {
    doWithSchema(
        metalake.loadCatalog(DEFAULT_HIVE_CATALOG),
        DEFAULT_HIVE_SCHEMA,
        catalog -> {
          String tableName = "test_drop_table";
          Column[] columns =
              new Column[] {Column.of("user_id", Types.IntegerType.get(), "USER_ID")};
          NameIdentifier identifier =
              NameIdentifier.of(
                  metalake.name(), DEFAULT_HIVE_CATALOG, DEFAULT_HIVE_SCHEMA, tableName);
          catalog.asTableCatalog().createTable(identifier, columns, "comment1", ImmutableMap.of());
          Assertions.assertTrue(catalog.asTableCatalog().tableExists(identifier));

          TableResult result = sql("DROP TABLE %s", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Assertions.assertFalse(catalog.asTableCatalog().tableExists(identifier));
        });
  }

  @Test
  public void testGetTable() {
    Column[] columns =
        new Column[] {
          Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
          Column.of("double_type", Types.DoubleType.get(), "double_type")
        };

    doWithSchema(
        metalake.loadCatalog(DEFAULT_HIVE_CATALOG),
        DEFAULT_HIVE_SCHEMA,
        catalog -> {
          String tableName = "test_desc_table";
          String comment = "comment1";
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(
                      metalake.name(),
                      DEFAULT_HIVE_CATALOG,
                      DEFAULT_HIVE_SCHEMA,
                      "test_desc_table"),
                  columns,
                  comment,
                  ImmutableMap.of("k1", "v1"));

          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(DEFAULT_HIVE_CATALOG);
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(DEFAULT_HIVE_SCHEMA, tableName));
            Assertions.assertNotNull(table);
            Assertions.assertEquals(CatalogBaseTable.TableKind.TABLE, table.getTableKind());
            Assertions.assertEquals(comment, table.getComment());

            org.apache.flink.table.catalog.Column[] expected =
                new org.apache.flink.table.catalog.Column[] {
                  org.apache.flink.table.catalog.Column.physical("string_type", DataTypes.STRING())
                      .withComment("string_type"),
                  org.apache.flink.table.catalog.Column.physical("double_type", DataTypes.DOUBLE())
                      .withComment("double_type")
                };
            org.apache.flink.table.catalog.Column[] actual =
                toFlinkPhysicalColumn(table.getUnresolvedSchema().getColumns());
            Assertions.assertArrayEquals(expected, actual);

            CatalogTable catalogTable = (CatalogTable) table;
            Assertions.assertFalse(catalogTable.isPartitioned());
          } catch (TableNotExistException e) {
            Assertions.fail(e);
          }
        });
  }

  @Override
  protected com.datastrato.gravitino.Catalog currentCatalog() {
    return hiveCatalog;
  }
}
