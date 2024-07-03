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

package com.datastrato.gravitino.flink.connector.integration.test;

import static com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils.assertColumns;
import static com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class FlinkCommonIT extends FlinkEnvIT {

  protected abstract Catalog currentCatalog();

  @Test
  public void testCreateSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_create_schema";
          try {
            TableResult tableResult = sql("CREATE DATABASE IF NOT EXISTS %s", schema);
            TestUtils.assertTableResult(tableResult, ResultKind.SUCCESS);
            catalog.asSchemas().schemaExists(schema);
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testGetSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_get_schema";
          String comment = "test comment";
          String propertyKey = "key1";
          String propertyValue = "value1";
          String location = warehouse + "/" + schema;

          try {
            TestUtils.assertTableResult(
                sql(
                    "CREATE DATABASE IF NOT EXISTS %s COMMENT '%s' WITH ('%s'='%s', '%s'='%s')",
                    schema, comment, propertyKey, propertyValue, "location", location),
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
        });
  }

  @Test
  public void testListSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          String schema = "test_list_schema";
          String schema2 = "test_list_schema2";
          String schema3 = "test_list_schema3";

          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema2), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema3), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("SHOW DATABASES"),
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
        });
  }

  @Test
  public void testAlterSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_alter_schema";
          try {
            TestUtils.assertTableResult(
                sql(
                    "CREATE DATABASE IF NOT EXISTS %s "
                        + "COMMENT 'test comment'"
                        + "WITH ('key1' = 'value1', 'key2'='value2')",
                    schema),
                ResultKind.SUCCESS);

            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
            Assertions.assertEquals("test comment", loadedSchema.comment());
            Assertions.assertEquals(3, loadedSchema.properties().size());
            Assertions.assertEquals("value1", loadedSchema.properties().get("key1"));
            Assertions.assertEquals("value2", loadedSchema.properties().get("key2"));
            Assertions.assertNotNull(loadedSchema.properties().get("location"));

            TestUtils.assertTableResult(
                sql("ALTER DATABASE %s SET ('key1'='new-value', 'key3'='value3')", schema),
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
        });
  }

  @Test
  public void testCreateSimpleTable() {
    String databaseName = "test_create_no_partition_table_db";
    String tableName = "test_create_no_partition_table";
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
                      + " COMMENT '%s' WITH ("
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
          Assertions.assertArrayEquals(EMPTY_TRANSFORM, table.partitioning());

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES ('A', 1.0), ('B', 2.0)", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of("A", 1.0),
              Row.of("B", 2.0));
        },
        true);
  }

  @Test
  public void testListTables() {
    String newSchema = "test_list_table_catalog";
    Column[] columns = new Column[] {Column.of("user_id", Types.IntegerType.get(), "USER_ID")};
    doWithSchema(
        currentCatalog(),
        newSchema,
        catalog -> {
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(newSchema, "test_table1"),
                  columns,
                  "comment1",
                  ImmutableMap.of());
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(newSchema, "test_table2"),
                  columns,
                  "comment2",
                  ImmutableMap.of());
          TableResult result = sql("SHOW TABLES");
          TestUtils.assertTableResult(
              result,
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of("test_table1"),
              Row.of("test_table2"));
        },
        true);
  }

  @Test
  public void testDropTable() {
    String databaseName = "test_drop_table_db";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          String tableName = "test_drop_table";
          Column[] columns =
              new Column[] {Column.of("user_id", Types.IntegerType.get(), "USER_ID")};
          NameIdentifier identifier = NameIdentifier.of(databaseName, tableName);
          catalog.asTableCatalog().createTable(identifier, columns, "comment1", ImmutableMap.of());
          Assertions.assertTrue(catalog.asTableCatalog().tableExists(identifier));

          TableResult result = sql("DROP TABLE %s", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Assertions.assertFalse(catalog.asTableCatalog().tableExists(identifier));
        },
        true);
  }

  @Test
  public void testGetSimpleTable() {
    String databaseName = "test_get_simple_table";
    Column[] columns =
        new Column[] {
          Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
          Column.of("double_type", Types.DoubleType.get(), "double_type")
        };

    doWithSchema(
        currentCatalog(),
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
              tableEnv.getCatalog(catalog.name());
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
        },
        true);
  }
}
