/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import com.datastrato.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations;
import com.datastrato.gravitino.catalog.postgresql.operation.PostgreSqlTableOperations;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

@Tag("gravitino-docker-it")
public class TestPostgreSqlTableOperations extends TestPostgreSqlAbstractIT {

  private static Type VARCHAR = Types.VarCharType.of(255);
  private static Type INT = Types.IntegerType.get();

  @Test
  public void testOperationTable() {
    String tableName = GravitinoITUtils.genRandomName("op_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(VARCHAR)
            .withComment("test_comment_col1")
            .withNullable(true)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());
    columns.add(
        new JdbcColumn.Builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue("hello world")
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
        null);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, load);

    // rename table
    String newName = "new_table";
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.rename(TEST_DB_NAME, tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(TEST_DB_NAME, newName));

    // alter table
    JdbcColumn newColumn =
        new JdbcColumn.Builder()
            .withName("col_5")
            .withType(VARCHAR)
            .withComment("new_add")
            .withNullable(true)
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()}, newColumn.dataType(), newColumn.comment()),
        TableChange.updateColumnComment(new String[] {columns.get(0).name()}, "test_new_comment"),
        TableChange.updateColumnType(
            new String[] {columns.get(1).name()}, Types.DecimalType.of(10, 2)),
        TableChange.deleteColumn(new String[] {columns.get(2).name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    List<JdbcColumn> alterColumns = new ArrayList<JdbcColumn>();
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(VARCHAR)
            .withComment("test_new_comment")
            .withNullable(true)
            .build());
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(columns.get(3));
    alterColumns.add(newColumn);
    assertionsTableInfo(newName, tableComment, alterColumns, properties, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.renameColumn(new String[] {"col_1"}, "col_1_new"),
        TableChange.updateColumnType(new String[] {"col_2"}, Types.DoubleType.get()));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_1_new")
            .withType(VARCHAR)
            .withComment("test_new_comment")
            .withNullable(true)
            .build());
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(columns.get(3));
    alterColumns.add(newColumn);
    assertionsTableInfo(newName, tableComment, alterColumns, properties, load);

    // alter column Nullability
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.updateColumnNullability(new String[] {"col_1_new"}, false),
        TableChange.updateColumnNullability(new String[] {"col_2"}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_1_new")
            .withType(VARCHAR)
            .withComment("test_new_comment")
            .withNullable(false)
            .build());
    alterColumns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(true)
            .withComment("set test key")
            .build());
    alterColumns.add(columns.get(3));
    alterColumns.add(newColumn);
    assertionsTableInfo(newName, tableComment, alterColumns, properties, load);

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.deleteColumn(new String[] {newColumn.name()}, true));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.remove(newColumn);
    assertionsTableInfo(newName, tableComment, alterColumns, properties, load);

    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    TEST_DB_NAME,
                    newName,
                    TableChange.deleteColumn(new String[] {newColumn.name()}, false)));
    Assertions.assertEquals(
        "Delete column does not exist: " + newColumn.name(), illegalArgumentException.getMessage());

    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                TEST_DB_NAME,
                newName,
                TableChange.deleteColumn(new String[] {newColumn.name()}, true)));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.purge(TEST_DB_NAME, newName));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.purge(TEST_DB_NAME, newName));
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(Types.BooleanType.get())
            .withNullable(true)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.ShortType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_5")
            .withType(Types.FloatType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_6")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_7")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_8")
            .withType(Types.TimeType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_10")
            .withType(Types.TimestampType.withTimeZone())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_11")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder().withName("col_12").withType(VARCHAR).withNullable(false).build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_13")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_14")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_15")
            .withType(Types.BinaryType.get())
            .withNullable(false)
            .build());

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, Collections.emptyMap(), load);
  }

  @Test
  public void testCreateMultipleTable() throws SQLException {
    String testDbName = GravitinoITUtils.genRandomName("test_db_");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + testDbName);
    }
    HashMap<String, String> properties = Maps.newHashMap();
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
        dataSource, JDBC_EXCEPTION_CONVERTER, new PostgreSqlTypeConverter(), config);

    String table_1 = "table_multiple_1";
    postgreSqlTableOperations.create(
        TEST_DB_NAME,
        table_1,
        new JdbcColumn[] {
          new JdbcColumn.Builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null);

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
          new JdbcColumn.Builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null);
    tableNames = postgreSqlTableOperations.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tableNames.contains(table_2));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load(TEST_DB_NAME, table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load("other_schema", table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load("other_schema", table_2));
    postgreSqlTableOperations.purge(TEST_DB_NAME, table_1);

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> {
          postgreSqlTableOperations.load(TEST_DB_NAME, table_1);
        });

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, table_2);
    Assertions.assertEquals(table_2, load.name());
  }
}
