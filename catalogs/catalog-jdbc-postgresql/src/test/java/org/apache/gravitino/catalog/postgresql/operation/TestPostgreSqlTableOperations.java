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
package org.apache.gravitino.catalog.postgresql.operation;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

@Tag("gravitino-docker-test")
public class TestPostgreSqlTableOperations extends TestPostgreSql {

  private static Type VARCHAR = Types.VarCharType.of(255);
  private static Type INT = Types.IntegerType.get();

  @Test
  public void testOperationTable() {
    String tableName = RandomNameUtils.genRandomName("op_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // rename table
    String newName = "new_table";
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.rename(TEST_DB_NAME, tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(TEST_DB_NAME, newName));

    // alter table
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName("col_5")
            .withType(VARCHAR)
            .withComment("new_add")
            .withNullable(true)
            .build();
    JdbcColumn newColumn1 =
        JdbcColumn.builder()
            .withName("col_6")
            .withType(Types.BooleanType.get())
            .withComment("new_add")
            .withDefaultValue(Literals.of("true", Types.BooleanType.get()))
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()}, newColumn.dataType(), newColumn.comment()),
        TableChange.addColumn(
            new String[] {newColumn1.name()},
            newColumn1.dataType(),
            newColumn1.comment(),
            newColumn1.defaultValue()),
        TableChange.updateColumnComment(new String[] {columns.get(0).name()}, "test_new_comment"),
        TableChange.updateColumnType(
            new String[] {columns.get(1).name()}, Types.DecimalType.of(10, 2)),
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(3).name()}, Literals.of("new hello world", VARCHAR)),
        TableChange.deleteColumn(new String[] {columns.get(2).name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    List<JdbcColumn> alterColumns = new ArrayList<JdbcColumn>();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.renameColumn(new String[] {"col_1"}, "col_1_new"),
        TableChange.updateColumnType(new String[] {"col_2"}, Types.DoubleType.get()));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1_new")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // alter column Nullability
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.updateColumnNullability(new String[] {"col_2"}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1_new")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(true)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.deleteColumn(new String[] {newColumn.name()}, true),
        TableChange.deleteColumn(new String[] {newColumn1.name()}, true));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.remove(newColumn);
    alterColumns.remove(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    TableChange deleteColumn = TableChange.deleteColumn(new String[] {newColumn.name()}, false);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME, newName, deleteColumn));
    Assertions.assertEquals(
        "Delete column does not exist: " + newColumn.name(), illegalArgumentException.getMessage());

    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                TEST_DB_NAME,
                newName,
                TableChange.deleteColumn(new String[] {newColumn.name()}, true)));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
    Assertions.assertFalse(TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = RandomNameUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.BooleanType.get())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.ShortType.get())
            .withNullable(false)
            .build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_5")
            .withType(Types.FloatType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_6")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_7")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_8")
            .withType(Types.TimeType.of(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_10")
            .withType(Types.TimestampType.withTimeZone(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_11")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder().withName("col_12").withType(VARCHAR).withNullable(false).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_13")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_14")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_15")
            .withType(Types.BinaryType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_16")
            .withType(Types.ListType.of(Types.IntegerType.get(), false))
            .withNullable(true)
            .build());

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        Transforms.EMPTY_TRANSFORM,
        load);
  }

  @Test
  public void testCreateMultipleTable() throws SQLException {
    String testDbName = RandomNameUtils.genRandomName("test_db_");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + testDbName);
    }
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    String jdbcUrl =
        StringUtils.substring(CONTAINER.getJdbcUrl(), 0, CONTAINER.getJdbcUrl().lastIndexOf("/"));
    properties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl + "/" + testDbName);
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DataSource dataSource = DataSourceUtils.createDataSource(properties);
    PostgreSqlSchemaOperations postgreSqlSchemaOperations = new PostgreSqlSchemaOperations();
    Map<String, String> config =
        new HashMap<String, String>() {
          {
            put(JdbcConfig.JDBC_DATABASE.getKey(), testDbName);
          }
        };
    postgreSqlSchemaOperations.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, config);
    postgreSqlSchemaOperations.create(TEST_DB_NAME, null, null);

    PostgreSqlTableOperations postgreSqlTableOperations = new PostgreSqlTableOperations();

    postgreSqlTableOperations.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        config);
    postgreSqlTableOperations.setDatabaseOperation(postgreSqlSchemaOperations);

    String table_1 = "table_multiple_1";
    postgreSqlTableOperations.create(
        TEST_DB_NAME,
        table_1,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    List<String> tableNames = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tableNames.contains(table_1));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load(TEST_DB_NAME, table_1));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load("other_schema", table_1));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load("other_schema", table_1));

    String table_2 = "table_multiple_2";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table_2,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    tableNames = postgreSqlTableOperations.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tableNames.contains(table_2));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load(TEST_DB_NAME, table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load("other_schema", table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load("other_schema", table_2));
    postgreSqlTableOperations.drop(TEST_DB_NAME, table_1);

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> {
          postgreSqlTableOperations.load(TEST_DB_NAME, table_1);
        });

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, table_2);
    Assertions.assertEquals(table_2, load.name());
  }

  @Test
  public void testCreateAutoIncrementTable() {
    String tableName = RandomNameUtils.genRandomName("increment_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());
    Map<String, String> properties = new HashMap<>();
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    columns.clear();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.ShortType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());

    // Testing does not support auto increment column types
    String randomName = RandomNameUtils.genRandomName("increment_table_");
    JdbcColumn[] jdbcColumns = columns.toArray(new JdbcColumn[0]);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              TABLE_OPERATIONS.create(
                  TEST_DB_NAME,
                  randomName,
                  jdbcColumns,
                  tableComment,
                  properties,
                  null,
                  Distributions.NONE,
                  Indexes.EMPTY_INDEXES);
            });

    Assertions.assertTrue(
        StringUtils.contains(illegalArgumentException.getMessage(), "Unsupported auto-increment"));
  }

  @Test
  public void testCreateIndexTable() {
    String tableName = RandomNameUtils.genRandomName("index_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("id-1")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("name")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("city")
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes =
        new Index[] {
          Indexes.primary("test_pk", new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}})
        };

    // Test create table index success.
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create table index failed.
    JdbcColumn[] jdbcColumns = columns.toArray(new JdbcColumn[0]);
    Index[] primaryIndex =
        new Index[] {
          Indexes.primary("no_exist_pk", new String[][] {{"no_exist_1"}}),
          Indexes.unique("no_exist_key", new String[][] {{"no_exist_2"}, {"no_exist_3"}})
        };
    GravitinoRuntimeException gravitinoRuntimeException =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> {
              TABLE_OPERATIONS.create(
                  TEST_DB_NAME,
                  tableName,
                  jdbcColumns,
                  tableComment,
                  properties,
                  null,
                  Distributions.NONE,
                  primaryIndex);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            gravitinoRuntimeException.getMessage(),
            "column \"no_exist_1\" named in key does not exist"));
  }

  @Test
  public void testAppendIndexesSql() {
    // Test append index sql success.
    Index[] indexes =
        new Index[] {
          Indexes.primary("test_pk", new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}})
        };
    StringBuilder successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + "CONSTRAINT \"test_pk\" PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + "CONSTRAINT \"u1_key\" UNIQUE (\"col_2\", \"col_3\"),\n"
            + "CONSTRAINT \"u2_key\" UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql not have name.
    indexes =
        new Index[] {
          Indexes.primary(null, new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique(null, new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique(null, new String[][] {{"col_3"}, {"col_4"}})
        };
    successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + " PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + " UNIQUE (\"col_2\", \"col_3\"),\n"
            + " UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql failed.
    Index primary = Indexes.primary("test_pk", new String[][] {{"col_1", "col_2"}});
    Index unique1 = Indexes.unique("u1_key", new String[][] {{"col_2", "col_3"}});
    Index unique2 = Indexes.unique("u2_key", new String[][] {{"col_3", "col_4"}});
    StringBuilder stringBuilder = new StringBuilder();

    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PostgreSqlTableOperations.appendIndexesSql(
                    new Index[] {primary, unique1, unique2}, stringBuilder));
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in PostgreSQL"));
  }

  @Test
  public void testOperationIndexDefinition() {
    String tableName = "t1";
    // Test add index definition success.
    TableChange.AddIndex addIndex =
        new TableChange.AddIndex(
            Index.IndexType.PRIMARY_KEY, "test_pk", new String[][] {{"col_1"}});
    String result = PostgreSqlTableOperations.addIndexDefinition(tableName, addIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" ADD CONSTRAINT \"test_pk\" PRIMARY KEY (\"col_1\");", result);

    addIndex =
        new TableChange.AddIndex(
            Index.IndexType.UNIQUE_KEY, "test_uk", new String[][] {{"col_1"}, {"col_2"}});
    result = PostgreSqlTableOperations.addIndexDefinition(tableName, addIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" ADD CONSTRAINT \"test_uk\" UNIQUE (\"col_1\", \"col_2\");", result);

    // Test delete index definition.
    TableChange.DeleteIndex deleteIndex = new TableChange.DeleteIndex("test_pk", false);
    result = PostgreSqlTableOperations.deleteIndexDefinition(tableName, deleteIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" DROP CONSTRAINT \"test_pk\";\n" + "DROP INDEX \"test_pk\";", result);

    deleteIndex = new TableChange.DeleteIndex("test_2_pk", true);
    result = PostgreSqlTableOperations.deleteIndexDefinition(tableName, deleteIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" DROP CONSTRAINT \"test_2_pk\";\n"
            + "DROP INDEX IF EXISTS \"test_2_pk\";",
        result);
  }

  @Test
  void testUpdateColumnAutoIncrementDefinition() {
    TableChange.UpdateColumnAutoIncrement updateColumnAutoIncrement =
        new TableChange.UpdateColumnAutoIncrement(new String[] {"col_1"}, true);
    String sql =
        PostgreSqlTableOperations.updateColumnAutoIncrementDefinition(
            updateColumnAutoIncrement, "table_test");
    Assertions.assertEquals(
        "ALTER TABLE \"table_test\" ALTER COLUMN  \"col_1\" ADD GENERATED BY DEFAULT AS IDENTITY;",
        sql);

    updateColumnAutoIncrement =
        new TableChange.UpdateColumnAutoIncrement(new String[] {"col_2"}, false);
    sql =
        PostgreSqlTableOperations.updateColumnAutoIncrementDefinition(
            updateColumnAutoIncrement, "table_test");
    Assertions.assertEquals(
        "ALTER TABLE \"table_test\" ALTER COLUMN  \"col_2\" DROP IDENTITY;", sql);
  }

  @Test
  public void testCalculateDatetimePrecision() {
    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("DATE", 10, -1),
        "DATE type should return 0 precision");

    Assertions.assertEquals(
        0,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIME", 10, 0),
        "TIME type should return 0 precision");

    Assertions.assertEquals(
        3,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMETZ", 23, 3),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        6,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMESTAMP", 26, 6),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        1,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMESTAMPTZ", 19, 1),
        "Lower case type name should work");

    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("VARCHAR", 50, 0),
        "Non-datetime type should return 0 precision");
  }
}
