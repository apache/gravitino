/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import static com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations.AUTO_INCREMENT;
import static com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations.PRIMARY_KEY;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMysqlTableOperations extends TestMysqlAbstractIT {

  private static Type VARCHAR = Types.VarCharType.of(255);
  private static Type INT = Types.IntegerType.get();

  @Test
  public void testOperationTable() {
    String tableName = RandomUtils.nextInt(10000) + "_op_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(VARCHAR)
            .withComment("test_comment")
            .withNullable(true)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set primary key")
            .withProperties(
                new ArrayList<String>() {
                  {
                    add(AUTO_INCREMENT);
                    add(PRIMARY_KEY);
                  }
                })
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(INT)
            .withProperties(
                new ArrayList<String>() {
                  {
                    add("UNIQUE KEY");
                  }
                })
            .withNullable(true)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue("hello world")
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();
    // TODO #804 Properties will be unified in the future.
    //    properties.put("ENGINE", "InnoDB");
    //    properties.put(AUTO_INCREMENT, "10");
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
            new String[] {newColumn.name()},
            newColumn.dataType(),
            newColumn.comment(),
            TableChange.ColumnPosition.after("col_1")));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    List<JdbcColumn> alterColumns =
        new ArrayList<JdbcColumn>() {
          {
            add(columns.get(0));
            add(newColumn);
            add(columns.get(1));
            add(columns.get(2));
            add(columns.get(3));
          }
        };
    assertionsTableInfo(newName, tableComment, alterColumns, properties, load);

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    assertionsTableInfo(newName, tableComment, columns, properties, load);

    GravitinoRuntimeException gravitinoRuntimeException =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    TEST_DB_NAME,
                    newName,
                    TableChange.deleteColumn(new String[] {newColumn.name()}, true)));

    Assertions.assertTrue(
        gravitinoRuntimeException
            .getMessage()
            .contains("Can't DROP 'col_5'; check that column/key exists"));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.purge(TEST_DB_NAME, newName));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.purge(TEST_DB_NAME, newName));
  }

  @Test
  public void testAlterTable() {
    String tableName = RandomUtils.nextInt(10000) + "_al_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(INT)
            .withComment("id")
            .withNullable(false)
            .build();
    columns.add(col_1);
    JdbcColumn col_2 =
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(VARCHAR)
            .withComment("name")
            .withDefaultValue("hello world")
            .withNullable(false)
            .build();
    columns.add(col_2);
    Map<String, String> properties = new HashMap<>();

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null);
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, VARCHAR));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    // After modifying the type, some attributes of the corresponding column are not supported.
    columns.clear();
    properties.remove(AUTO_INCREMENT);
    col_1 =
        new JdbcColumn.Builder()
            .withName(col_1.name())
            .withType(VARCHAR)
            .withComment(col_1.comment())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.getDefaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    assertionsTableInfo(tableName, tableComment, columns, properties, load);

    String newComment = "new_comment";
    // update table comment and column comment
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, INT),
        TableChange.updateColumnComment(new String[] {col_2.name()}, newComment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();
    col_1 =
        new JdbcColumn.Builder()
            .withName(col_1.name())
            .withType(INT)
            .withComment(col_1.comment())
            .withProperties(col_1.getProperties())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.getDefaultValue())
            .build();
    col_2 =
        new JdbcColumn.Builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(newComment)
            .withProperties(col_2.getProperties())
            .withNullable(col_2.nullable())
            .withDefaultValue(col_2.getDefaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    assertionsTableInfo(tableName, tableComment, columns, properties, load);

    String newColName_1 = "new_col_1";
    String newColName_2 = "new_col_2";
    // rename column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.renameColumn(new String[] {col_1.name()}, newColName_1),
        TableChange.renameColumn(new String[] {col_2.name()}, newColName_2));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();
    col_1 =
        new JdbcColumn.Builder()
            .withName(newColName_1)
            .withType(col_1.dataType())
            .withComment(col_1.comment())
            .withProperties(col_1.getProperties())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.getDefaultValue())
            .build();
    col_2 =
        new JdbcColumn.Builder()
            .withName(newColName_2)
            .withType(col_2.dataType())
            .withComment(col_2.comment())
            .withProperties(col_2.getProperties())
            .withNullable(col_2.nullable())
            .withDefaultValue(col_2.getDefaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    assertionsTableInfo(tableName, tableComment, columns, properties, load);

    newComment = "txt3";
    String newCol2Comment = "xxx";
    // update column position 、comment and add column、set table properties
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {newColName_1}, TableChange.ColumnPosition.after(newColName_2)),
        TableChange.updateComment(newComment),
        TableChange.addColumn(new String[] {"col_3"}, VARCHAR, "txt3"),
        TableChange.updateColumnComment(new String[] {newColName_2}, newCol2Comment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();

    columns.add(
        new JdbcColumn.Builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(newCol2Comment)
            .withProperties(col_2.getProperties())
            .withDefaultValue(col_2.getDefaultValue())
            .withNullable(col_2.nullable())
            .build());
    columns.add(col_1);
    JdbcColumn col_3 =
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withNullable(true)
            .withComment("txt3")
            .build();
    columns.add(
        new JdbcColumn.Builder().withName("col_3").withType(VARCHAR).withComment("txt3").build());
    assertionsTableInfo(tableName, newComment, columns, properties, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnPosition(new String[] {columns.get(0).name()}, null),
        TableChange.updateColumnNullability(new String[] {col_3.name()}, !col_3.nullable()));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    col_2 = columns.remove(0);
    columns.clear();

    columns.add(col_1);
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("txt3")
            .build());
    columns.add(col_2);

    assertionsTableInfo(tableName, newComment, columns, properties, load);
  }

  @Test
  public void testCreateAndLoadTable() {
    String tableName = RandomUtils.nextInt(10000) + "_cl_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(Types.DecimalType.of(10, 2))
            .withComment("test_decimal")
            .withNullable(false)
            .withDefaultValue("0.00")
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.LongType.get())
            .withNullable(false)
            .withDefaultValue("0")
            .withComment("long type")
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(Types.TimestampType.withoutTimeZone())
            // MySQL 5.7 doesn't support nullable timestamp
            .withNullable(false)
            .withComment("timestamp")
            .withDefaultValue("2013-01-01 00:00:00")
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(Types.DateType.get())
            .withNullable(true)
            .withComment("date")
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

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, loaded);
  }

  @Test
  public void testCreateMultipleTables() {
    String test_table_1 = "test_table_1";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        test_table_1,
        new JdbcColumn[] {
          new JdbcColumn.Builder()
              .withName("col_1")
              .withType(Types.DecimalType.of(10, 2))
              .withComment("test_decimal")
              .withNullable(false)
              .withDefaultValue("0.00")
              .build()
        },
        "test_comment",
        null,
        null);

    String testDb = "test_db_2";

    DATABASE_OPERATIONS.create(testDb, null, null);
    List<String> tables = TABLE_OPERATIONS.listTables(testDb);
    Assertions.assertFalse(tables.contains(test_table_1));

    String test_table_2 = "test_table_2";
    TABLE_OPERATIONS.create(
        testDb,
        test_table_2,
        new JdbcColumn[] {
          new JdbcColumn.Builder()
              .withName("col_1")
              .withType(Types.DecimalType.of(10, 2))
              .withComment("test_decimal")
              .withNullable(false)
              .withDefaultValue("0.00")
              .build()
        },
        "test_comment",
        null,
        null);

    tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tables.contains(test_table_2));
  }
}
