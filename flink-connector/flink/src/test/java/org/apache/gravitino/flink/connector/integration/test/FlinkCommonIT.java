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

package org.apache.gravitino.flink.connector.integration.test;

import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.assertColumns;
import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public abstract class FlinkCommonIT extends FlinkEnvIT {

  protected abstract Catalog currentCatalog();

  protected boolean supportTableOperation() {
    return true;
  }

  protected boolean supportColumnOperation() {
    return true;
  }

  protected boolean supportSchemaOperationWithCommentAndOptions() {
    return true;
  }

  protected boolean supportGetSchemaWithoutCommentAndOption() {
    return true;
  }

  protected abstract String getProvider();

  protected abstract boolean supportDropCascade();

  protected boolean supportsPrimaryKey() {
    return true;
  }

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
            catalog.asSchemas().dropSchema(schema, supportDropCascade());
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  @EnabledIf("supportGetSchemaWithoutCommentAndOption")
  public void testGetSchemaWithoutCommentAndOption() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_get_schema";
          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

            catalog.asSchemas().schemaExists(schema);
            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  @EnabledIf("supportSchemaOperationWithCommentAndOptions")
  public void testGetSchemaWithCommentAndOptions() {
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
            Assertions.assertEquals(propertyValue, loadedSchema.properties().get(propertyKey));
            Assertions.assertEquals(
                location, loadedSchema.properties().get(HiveConstants.LOCATION));
          } finally {
            catalog.asSchemas().dropSchema(schema, supportDropCascade());
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testListSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
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
            Arrays.sort(schemas);
            Assertions.assertEquals(4, schemas.length);
            Assertions.assertEquals("default", schemas[0]);
            Assertions.assertEquals(schema, schemas[1]);
            Assertions.assertEquals(schema2, schemas[2]);
            Assertions.assertEquals(schema3, schemas[3]);
          } finally {
            catalog.asSchemas().dropSchema(schema, supportDropCascade());
            catalog.asSchemas().dropSchema(schema2, supportDropCascade());
            catalog.asSchemas().dropSchema(schema3, supportDropCascade());
            Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          }
        });
  }

  @Test
  @EnabledIf("supportSchemaOperationWithCommentAndOptions")
  public void testAlterSchemaWithCommentAndOptions() {
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
            Assertions.assertEquals("value1", loadedSchema.properties().get("key1"));
            Assertions.assertEquals("value2", loadedSchema.properties().get("key2"));
            Assertions.assertNotNull(loadedSchema.properties().get("location"));

            TestUtils.assertTableResult(
                sql("ALTER DATABASE %s SET ('key1'='new-value', 'key3'='value3')", schema),
                ResultKind.SUCCESS);
            Schema reloadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, reloadedSchema.name());
            Assertions.assertEquals("test comment", reloadedSchema.comment());
            Assertions.assertEquals("new-value", reloadedSchema.properties().get("key1"));
            Assertions.assertEquals("value3", reloadedSchema.properties().get("key3"));
          } finally {
            catalog.asSchemas().dropSchema(schema, supportDropCascade());
          }
        });
  }

  @Test
  @EnabledIf("supportTableOperation")
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
              sql("INSERT INTO %s VALUES ('A', 1.0), ('B', 2.0)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of("A", 1.0),
              Row.of("B", 2.0));
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportsPrimaryKey")
  public void testCreateTableWithPrimaryKey() {
    String databaseName = "test_create_table_with_primary_key_db";
    String tableName = "test_create_primary_key_table";
    String comment = "test comment";
    String key = "test key";
    String value = "test value";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          sql(
              "CREATE TABLE %s "
                  + "(aa int, "
                  + " bb int,"
                  + " cc int,"
                  + "  PRIMARY KEY (aa,bb) NOT ENFORCED"
                  + ")"
                  + " COMMENT '%s' WITH ("
                  + "'%s' = '%s')",
              tableName, comment, key, value);
          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertEquals(1, table.index().length);
          Index index = table.index()[0];
          Assertions.assertEquals("aa", index.fieldNames()[0][0]);
          Assertions.assertEquals("bb", index.fieldNames()[1][0]);

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES(1,2,3)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1));
          TestUtils.assertTableResult(
              sql("SELECT count(*) num FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(1));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName), ResultKind.SUCCESS_WITH_CONTENT, Row.of(1, 2, 3));

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES(1,2,4)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1));
          TestUtils.assertTableResult(
              sql("SELECT count(*) num FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(1));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName), ResultKind.SUCCESS_WITH_CONTENT, Row.of(1, 2, 4));

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES(1,3,4)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1));
          TestUtils.assertTableResult(
              sql("SELECT count(*) num FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(2));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(1, 2, 4),
              Row.of(1, 3, 4));

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES(2,2,4)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1));
          TestUtils.assertTableResult(
              sql("SELECT count(*) num FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(3));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(1, 2, 4),
              Row.of(1, 3, 4),
              Row.of(2, 2, 4));
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportTableOperation")
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
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportTableOperation")
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
          sql("DROP TABLE IF EXISTS %s", tableName);
          Assertions.assertFalse(catalog.asTableCatalog().tableExists(identifier));
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportTableOperation")
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
            fail(e);
          }
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportColumnOperation")
  public void testRenameColumn() {
    String databaseName = "test_rename_column_db";
    String tableName = "test_rename_column";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id INT COMMENT 'USER_ID', "
                      + " order_amount DOUBLE COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'",
                  tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          result = sql("ALTER TABLE %s RENAME user_id TO user_id_new", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Column[] actual =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected =
              new Column[] {
                Column.of("user_id_new", Types.IntegerType.get(), "USER_ID"),
                Column.of("order_amount", Types.DoubleType.get(), "ORDER_AMOUNT")
              };
          assertColumns(expected, actual);
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportColumnOperation")
  public void testAlterTableComment() {
    String databaseName = "test_alter_table_comment_database";
    String tableName = "test_alter_table_comment";
    String newComment = "new_table_comment";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(currentCatalog().name());
          if (flinkCatalog.isPresent()) {
            org.apache.flink.table.catalog.Catalog currentFlinkCatalog = flinkCatalog.get();
            ObjectPath currentTablePath = new ObjectPath(databaseName, tableName);
            try {
              // use java api to create a new table
              org.apache.flink.table.api.Schema schema =
                  org.apache.flink.table.api.Schema.newBuilder()
                      .column("test", DataTypes.INT())
                      .build();
              CatalogTable newTable =
                  CatalogTable.of(schema, "test comment", ImmutableList.of(), ImmutableMap.of());
              List<org.apache.flink.table.catalog.Column> columns = Lists.newArrayList();
              columns.add(org.apache.flink.table.catalog.Column.physical("test", DataTypes.INT()));
              ResolvedSchema resolvedSchema = new ResolvedSchema(columns, new ArrayList<>(), null);
              currentFlinkCatalog.createTable(
                  currentTablePath, new ResolvedCatalogTable(newTable, resolvedSchema), false);
              CatalogTable table = (CatalogTable) currentFlinkCatalog.getTable(currentTablePath);

              // alter table comment
              currentFlinkCatalog.alterTable(
                  currentTablePath,
                  CatalogTable.of(
                      table.getUnresolvedSchema(),
                      newComment,
                      table.getPartitionKeys(),
                      table.getOptions()),
                  false);

              CatalogTable loadedTable =
                  (CatalogTable) currentFlinkCatalog.getTable(currentTablePath);
              Assertions.assertEquals(newComment, loadedTable.getComment());
              Table gravitinoTable =
                  currentCatalog()
                      .asTableCatalog()
                      .loadTable(NameIdentifier.of(databaseName, tableName));
              Assertions.assertEquals(newComment, gravitinoTable.comment());

            } catch (DatabaseNotExistException
                | TableAlreadyExistException
                | TableNotExistException e) {
              fail(e);
            }
          } else {
            fail("Catalog doesn't exist");
          }
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportColumnOperation")
  public void testAlterTableAddColumn() {
    String databaseName = "test_alter_table_add_column_db";
    String tableName = "test_alter_table_add_column";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id INT COMMENT 'USER_ID', "
                      + " order_amount INT COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'",
                  tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          result = sql("ALTER TABLE %s ADD new_column_2 INT AFTER order_amount", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Column[] actual =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected =
              new Column[] {
                Column.of("user_id", Types.IntegerType.get(), "USER_ID"),
                Column.of("order_amount", Types.IntegerType.get(), "ORDER_AMOUNT"),
                Column.of("new_column_2", Types.IntegerType.get(), null),
              };
          assertColumns(expected, actual);
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportColumnOperation")
  public void testAlterTableDropColumn() {
    String databaseName = "test_alter_table_drop_column_db";
    String tableName = "test_alter_table_drop_column";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id INT COMMENT 'USER_ID', "
                      + " order_amount INT COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'",
                  tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          result = sql("ALTER TABLE %s DROP user_id", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Column[] actual =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected =
              new Column[] {Column.of("order_amount", Types.IntegerType.get(), "ORDER_AMOUNT")};
          assertColumns(expected, actual);
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportColumnOperation")
  public void testAlterColumnTypeAndChangeOrder() {
    String databaseName = "test_alter_table_alter_column_db";
    String tableName = "test_alter_table_rename_column";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id BIGINT COMMENT 'USER_ID', "
                      + " order_amount INT COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'"
                      + " WITH ("
                      + "'%s' = '%s')",
                  tableName, "test key", "test value");
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          result =
              sql("ALTER TABLE %s MODIFY order_amount BIGINT COMMENT 'new comment2'", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          result =
              sql(
                  "ALTER TABLE %s MODIFY user_id BIGINT COMMENT 'new comment' AFTER order_amount",
                  tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Column[] actual =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected =
              new Column[] {
                Column.of("order_amount", Types.LongType.get(), "new comment2"),
                Column.of("user_id", Types.LongType.get(), "new comment")
              };
          assertColumns(expected, actual);
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportTableOperation")
  public void testRenameTable() {
    String databaseName = "test_rename_table_db";
    String tableName = "test_rename_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id INT COMMENT 'USER_ID', "
                      + " order_amount INT COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'",
                  tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          String newTableName = "new_rename_table_name";
          result = sql("ALTER TABLE %s RENAME TO %s", tableName, newTableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Assertions.assertFalse(
              catalog.asTableCatalog().tableExists(NameIdentifier.of(databaseName, tableName)));
          Assertions.assertTrue(
              catalog.asTableCatalog().tableExists(NameIdentifier.of(databaseName, newTableName)));
        },
        true,
        supportDropCascade());
  }

  @Test
  @EnabledIf("supportTableOperation")
  public void testAlterTableProperties() {
    String databaseName = "test_alter_table_properties_db";
    String tableName = "test_alter_table_properties";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(user_id INT COMMENT 'USER_ID', "
                      + " order_amount INT COMMENT 'ORDER_AMOUNT')"
                      + " COMMENT 'test comment'"
                      + " WITH ("
                      + "'%s' = '%s')",
                  tableName, "key", "value");
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          result = sql("ALTER TABLE %s SET ('key2' = 'value2', 'key' = 'value1')", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);
          Map<String, String> properties =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .properties();

          Assertions.assertEquals("value1", properties.get("key"));
          Assertions.assertEquals("value2", properties.get("key2"));
          result = sql("ALTER TABLE %s RESET ('key2')", tableName);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          properties =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .properties();
          Assertions.assertEquals("value1", properties.get("key"));
          Assertions.assertNull(properties.get("key2"));
        },
        true,
        supportDropCascade());
  }
}
