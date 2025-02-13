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

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.assertColumns;
import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class FlinkIcebergCatalogIT extends FlinkCommonIT {

  private static final String DEFAULT_ICEBERG_CATALOG = "flink_iceberg_catalog";

  private static org.apache.gravitino.Catalog icebergCatalog;

  @Override
  protected boolean supportsPrimaryKey() {
    return false;
  }

  @BeforeAll
  public void before() {
    Preconditions.checkNotNull(metalake);
    icebergCatalog =
        metalake.createCatalog(
            DEFAULT_ICEBERG_CATALOG,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            getCatalogConfigs());
  }

  protected abstract Map<String, String> getCatalogConfigs();

  @Test
  public void testCreateGravitinoIcebergCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.
    String catalogName = "gravitino_iceberg_catalog";
    Configuration configuration = Configuration.fromMap(getCatalogConfigs());
    configuration.set(
        CommonCatalogOptions.CATALOG_TYPE, GravitinoIcebergCatalogFactoryOptions.IDENTIFIER);

    CatalogDescriptor catalogDescriptor = CatalogDescriptor.of(catalogName, configuration);
    tableEnv.createCatalog(catalogName, catalogDescriptor);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the catalog properties.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(IcebergConstants.URI));

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalog.get());

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

    Optional<org.apache.flink.table.catalog.Catalog> droppedCatalog =
        tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testCreateGravitinoIcebergUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.
    String catalogName = "gravitino_iceberg_using_sql";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='%s', "
                + "'catalog-backend'='%s',"
                + "'uri'='%s',"
                + "'warehouse'='%s'"
                + ")",
            catalogName,
            GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
            getCatalogBackend(),
            hiveMetastoreUri,
            warehouse));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the properties of the created catalog.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(IcebergConstants.URI));

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalog.get());

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

    Optional<org.apache.flink.table.catalog.Catalog> droppedCatalog =
        tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testGetCatalogFromGravitino() {
    // list catalogs.
    int numCatalogs = tableEnv.listCatalogs().length;

    // create a new catalog.
    String catalogName = "iceberg_catalog_in_gravitino";
    org.apache.gravitino.Catalog gravitinoCatalog =
        metalake.createCatalog(
            catalogName,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            getCatalogConfigs());
    Assertions.assertNotNull(gravitinoCatalog);
    Assertions.assertEquals(catalogName, gravitinoCatalog.name());
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs + 1, tableEnv.listCatalogs().length, "Should create a new catalog");

    // get the catalog from Gravitino.
    Optional<org.apache.flink.table.catalog.Catalog> flinkIcebergCatalog =
        tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(flinkIcebergCatalog.isPresent());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, flinkIcebergCatalog.get());

    // drop the catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testIcebergTableWithPartition() {
    String databaseName = "test_iceberg_table_partition";
    String tableName = "iceberg_table_with_partition";
    String key = "test key";
    String value = "test value";

    doWithSchema(
        icebergCatalog,
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s ("
                      + " id BIGINT COMMENT 'unique id',"
                      + " data STRING NOT NULL"
                      + " ) PARTITIONED BY (data)"
                      + " WITH ("
                      + "'%s' = '%s')",
                  tableName, key, value);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertNotNull(table);
          Assertions.assertEquals(value, table.properties().get(key));
          Column[] columns =
              new Column[] {
                Column.of("id", Types.LongType.get(), "unique id", true, false, null),
                Column.of("data", Types.StringType.get(), null, false, false, null)
              };
          assertColumns(columns, table.columns());
          Transform[] partitions = new Transform[] {Transforms.identity("data")};
          Assertions.assertArrayEquals(partitions, table.partitioning());

          // load flink catalog
          try {
            org.apache.flink.table.catalog.Catalog flinkCatalog =
                tableEnv.getCatalog(currentCatalog().name()).get();
            CatalogBaseTable flinkTable =
                flinkCatalog.getTable(ObjectPath.fromString(databaseName + "." + tableName));
            DefaultCatalogTable catalogTable = (DefaultCatalogTable) flinkTable;
            Assertions.assertTrue(catalogTable.isPartitioned());
            Assertions.assertArrayEquals(
                new String[] {"data"}, catalogTable.getPartitionKeys().toArray());
          } catch (Exception e) {
            Assertions.fail("Table should be exist", e);
          }

          // write and read.
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s ORDER BY data", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(1, "A"),
              Row.of(2, "B"));
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testCreateIcebergTable() {
    String databaseName = "test_create_iceberg_table";
    String tableName = "iceberg_table";
    String comment = "test table comment";
    String key = "test key";
    String value = "test value";

    doWithSchema(
        metalake.loadCatalog(DEFAULT_ICEBERG_CATALOG),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s ("
                      + " string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type',"
                      + " int_type INT COMMENT 'int_type',"
                      + " varchar_type VARCHAR COMMENT 'varchar_type',"
                      + " boolean_type BOOLEAN COMMENT 'boolean_type',"
                      + " binary_type VARBINARY(10) COMMENT 'binary_type',"
                      + " decimal_type DECIMAL(10, 2) COMMENT 'decimal_type',"
                      + " bigint_type BIGINT COMMENT 'bigint_type',"
                      + " float_type FLOAT COMMENT 'float_type',"
                      + " date_type DATE COMMENT 'date_type',"
                      + " timestamp_type TIMESTAMP COMMENT 'timestamp_type',"
                      + " array_type ARRAY<INT> COMMENT 'array_type',"
                      + " map_type MAP<INT, STRING> COMMENT 'map_type',"
                      + " struct_type ROW<k1 INT, k2 String>"
                      + " ) COMMENT '%s' WITH ("
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
                Column.of("double_type", Types.DoubleType.get(), "double_type"),
                Column.of("int_type", Types.IntegerType.get(), "int_type"),
                Column.of("varchar_type", Types.StringType.get(), "varchar_type"),
                Column.of("boolean_type", Types.BooleanType.get(), "boolean_type"),
                Column.of("binary_type", Types.BinaryType.get(), "binary_type"),
                Column.of("decimal_type", Types.DecimalType.of(10, 2), "decimal_type"),
                Column.of("bigint_type", Types.LongType.get(), "bigint_type"),
                Column.of("float_type", Types.FloatType.get(), "float_type"),
                Column.of("date_type", Types.DateType.get(), "date_type"),
                Column.of(
                    "timestamp_type", Types.TimestampType.withoutTimeZone(), "timestamp_type"),
                Column.of(
                    "array_type", Types.ListType.of(Types.IntegerType.get(), true), "array_type"),
                Column.of(
                    "map_type",
                    Types.MapType.of(Types.IntegerType.get(), Types.StringType.get(), true),
                    "map_type"),
                Column.of(
                    "struct_type",
                    Types.StructType.of(
                        Types.StructType.Field.nullableField("k1", Types.IntegerType.get()),
                        Types.StructType.Field.nullableField("k2", Types.StringType.get())),
                    null)
              };
          assertColumns(columns, table.columns());
          Assertions.assertArrayEquals(EMPTY_TRANSFORM, table.partitioning());
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testGetIcebergTable() {
    Column[] columns =
        new Column[] {
          Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
          Column.of("double_type", Types.DoubleType.get(), "double_type"),
          Column.of("int_type", Types.IntegerType.get(), "int_type"),
          Column.of("varchar_type", Types.StringType.get(), "varchar_type"),
          Column.of("boolean_type", Types.BooleanType.get(), "boolean_type"),
          Column.of("binary_type", Types.BinaryType.get(), "binary_type"),
          Column.of("decimal_type", Types.DecimalType.of(10, 2), "decimal_type"),
          Column.of("bigint_type", Types.LongType.get(), "bigint_type"),
          Column.of("float_type", Types.FloatType.get(), "float_type"),
          Column.of("date_type", Types.DateType.get(), "date_type"),
          Column.of("timestamp_type", Types.TimestampType.withoutTimeZone(), "timestamp_type"),
          Column.of("array_type", Types.ListType.of(Types.IntegerType.get(), true), "array_type"),
          Column.of(
              "map_type",
              Types.MapType.of(Types.IntegerType.get(), Types.StringType.get(), true),
              "map_type"),
          Column.of(
              "struct_type",
              Types.StructType.of(
                  Types.StructType.Field.nullableField("k1", Types.IntegerType.get()),
                  Types.StructType.Field.nullableField("k2", Types.StringType.get())),
              null)
        };

    String databaseName = "test_get_iceberg_table";
    doWithSchema(
        metalake.loadCatalog(DEFAULT_ICEBERG_CATALOG),
        databaseName,
        catalog -> {
          String tableName = "test_desc_table";
          String comment = "comment1";
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(databaseName, "test_desc_table"),
                  columns,
                  comment,
                  ImmutableMap.of("k1", "v1"));

          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(DEFAULT_ICEBERG_CATALOG);
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(databaseName, tableName));
            Assertions.assertNotNull(table);
            Assertions.assertEquals(CatalogBaseTable.TableKind.TABLE, table.getTableKind());
            Assertions.assertEquals(comment, table.getComment());

            org.apache.flink.table.catalog.Column[] expected =
                new org.apache.flink.table.catalog.Column[] {
                  org.apache.flink.table.catalog.Column.physical("string_type", DataTypes.STRING())
                      .withComment("string_type"),
                  org.apache.flink.table.catalog.Column.physical("double_type", DataTypes.DOUBLE())
                      .withComment("double_type"),
                  org.apache.flink.table.catalog.Column.physical("int_type", DataTypes.INT())
                      .withComment("int_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "varchar_type", DataTypes.VARCHAR(Integer.MAX_VALUE))
                      .withComment("varchar_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "boolean_type", DataTypes.BOOLEAN())
                      .withComment("boolean_type"),
                  org.apache.flink.table.catalog.Column.physical("binary_type", DataTypes.BYTES())
                      .withComment("binary_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "decimal_type", DataTypes.DECIMAL(10, 2))
                      .withComment("decimal_type"),
                  org.apache.flink.table.catalog.Column.physical("bigint_type", DataTypes.BIGINT())
                      .withComment("bigint_type"),
                  org.apache.flink.table.catalog.Column.physical("float_type", DataTypes.FLOAT())
                      .withComment("float_type"),
                  org.apache.flink.table.catalog.Column.physical("date_type", DataTypes.DATE())
                      .withComment("date_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "timestamp_type", DataTypes.TIMESTAMP())
                      .withComment("timestamp_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "array_type", DataTypes.ARRAY(DataTypes.INT()))
                      .withComment("array_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "map_type", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                      .withComment("map_type"),
                  org.apache.flink.table.catalog.Column.physical(
                      "struct_type",
                      DataTypes.ROW(
                          DataTypes.FIELD("k1", DataTypes.INT()),
                          DataTypes.FIELD("k2", DataTypes.STRING())))
                };
            org.apache.flink.table.catalog.Column[] actual =
                toFlinkPhysicalColumn(table.getUnresolvedSchema().getColumns());
            Assertions.assertArrayEquals(expected, actual);

            CatalogTable catalogTable = (CatalogTable) table;
            Assertions.assertFalse(catalogTable.isPartitioned());
          } catch (TableNotExistException e) {
            Assertions.fail(e);
          }
        },
        true,
        supportDropCascade());
  }

  @Override
  protected Catalog currentCatalog() {
    return icebergCatalog;
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected boolean supportGetSchemaWithoutCommentAndOption() {
    return false;
  }

  @Override
  protected boolean supportDropCascade() {
    return false;
  }

  protected abstract String getCatalogBackend();
}
