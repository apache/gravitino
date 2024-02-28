/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.MYSQL_AUTO_INCREMENT_OFFSET_KEY;
import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.MYSQL_ENGINE_KEY;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Decimal;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class MysqlTableOperationsIT extends TestMysqlAbstractIT {

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
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(INT)
            .withNullable(true)
            .withDefaultValue(Literals.NULL)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();
    properties.put(MYSQL_AUTO_INCREMENT_OFFSET_KEY, "10");

    Index[] indexes = new Index[] {Indexes.unique("test", new String[][] {{"col_1"}, {"col_2"}})};
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        indexes);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

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
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()},
            newColumn.dataType(),
            newColumn.comment(),
            TableChange.ColumnPosition.after("col_1")),
        TableChange.setProperty(MYSQL_ENGINE_KEY, "InnoDB"));
    properties.put(MYSQL_ENGINE_KEY, "InnoDB");
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
    assertionsTableInfo(newName, tableComment, alterColumns, properties, indexes, load);

    // Detect unsupported properties
    TableChange setProperty = TableChange.setProperty(MYSQL_ENGINE_KEY, "ABC");
    GravitinoRuntimeException gravitinoRuntimeException =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME, newName, setProperty));
    Assertions.assertTrue(
        StringUtils.contains(
            gravitinoRuntimeException.getMessage(), "Unknown storage engine 'ABC'"));

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    assertionsTableInfo(newName, tableComment, columns, properties, indexes, load);

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

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
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
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build();
    columns.add(col_2);
    JdbcColumn col_3 =
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withComment("name")
            .withDefaultValue(Literals.NULL)
            .build();
    //  `col_1` int NOT NULL COMMENT 'id' ,
    //  `col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'name' ,
    //  `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    columns.add(col_3);
    Map<String, String> properties = new HashMap<>();

    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("uk_2", new String[][] {{"col_1"}, {"col_2"}})
        };
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        indexes);
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, VARCHAR));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    // After modifying the type, some attributes of the corresponding column are not supported.
    columns.clear();
    col_1 =
        new JdbcColumn.Builder()
            .withName(col_1.name())
            .withType(VARCHAR)
            .withComment(col_1.comment())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    String newComment = "new_comment";
    // update table comment and column comment
    //  `col_1` int NOT NULL COMMENT 'id' ,
    //  `col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment' ,
    //  `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
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
            .withAutoIncrement(col_1.autoIncrement())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    col_2 =
        new JdbcColumn.Builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(newComment)
            .withAutoIncrement(col_2.autoIncrement())
            .withNullable(col_2.nullable())
            .withDefaultValue(col_2.defaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    String newColName_1 = "new_col_1";
    String newColName_2 = "new_col_2";
    // rename column
    // update table comment and column comment
    //  `new_col_1` int NOT NULL COMMENT 'id' ,
    //  `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment' ,
    //  `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
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
            .withAutoIncrement(col_1.autoIncrement())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    col_2 =
        new JdbcColumn.Builder()
            .withName(newColName_2)
            .withType(col_2.dataType())
            .withComment(col_2.comment())
            .withAutoIncrement(col_2.autoIncrement())
            .withNullable(col_2.nullable())
            .withDefaultValue(col_2.defaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    newComment = "txt3";
    String newCol2Comment = "xxx";
    // update column position 、comment and add column、set table properties
    //  `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'xxx' ,
    //  `new_col_1` int NOT NULL COMMENT 'id' ,
    //  `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    //  `col_4` varchar(255) NOT NULL COMMENT 'txt4' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {newColName_1}, TableChange.ColumnPosition.after(newColName_2)),
        TableChange.updateComment(newComment),
        TableChange.addColumn(new String[] {"col_4"}, VARCHAR, "txt4", false),
        TableChange.updateColumnComment(new String[] {newColName_2}, newCol2Comment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();

    columns.add(
        new JdbcColumn.Builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(newCol2Comment)
            .withAutoIncrement(col_2.autoIncrement())
            .withDefaultValue(col_2.defaultValue())
            .withNullable(col_2.nullable())
            .build());
    columns.add(col_1);
    columns.add(col_3);
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withComment("txt4")
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .withNullable(false)
            .build());
    assertionsTableInfo(tableName, newComment, columns, properties, indexes, load);

    //  `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'xxx' ,
    //  `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    //  `col_4` varchar(255) NOT NULL COMMENT 'txt4' ,
    //  `new_col_1` int NOT NULL COMMENT 'id' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnPosition(new String[] {columns.get(1).name()}, null),
        TableChange.updateColumnNullability(
            new String[] {columns.get(3).name()}, !columns.get(3).nullable()));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    col_1 = columns.remove(1);
    JdbcColumn col3 = columns.remove(1);
    JdbcColumn col_4 = columns.remove(1);
    columns.clear();

    columns.add(
        new JdbcColumn.Builder()
            .withName("new_col_2")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("xxx")
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .build());
    columns.add(col3);
    columns.add(
        new JdbcColumn.Builder()
            .withName(col_4.name())
            .withType(col_4.dataType())
            .withNullable(!col_4.nullable())
            .withComment(col_4.comment())
            .withDefaultValue(col_4.defaultValue())
            .build());
    columns.add(col_1);

    assertionsTableInfo(tableName, newComment, columns, properties, indexes, load);

    TableChange updateColumn =
        TableChange.updateColumnNullability(new String[] {col3.name()}, !col3.nullable());
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME, tableName, updateColumn));
    Assertions.assertTrue(
        exception.getMessage().contains("with null default value cannot be changed to not null"));
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
            .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.00", 10, 2)))
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.LongType.get())
            .withNullable(false)
            .withDefaultValue(Literals.longLiteral(0L))
            .withComment("long type")
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(Types.TimestampType.withoutTimeZone())
            // MySQL 5.7 doesn't support nullable timestamp
            .withNullable(false)
            .withComment("timestamp")
            .withDefaultValue(Literals.timestampLiteral(LocalDateTime.parse("2013-01-01T00:00:00")))
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(Types.DateType.get())
            .withNullable(true)
            .withComment("date")
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}}),
          Indexes.unique("uk_col_4", new String[][] {{"col_4"}})
        };
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        indexes);

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, loaded);
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_1")
            .withType(Types.ByteType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(Types.ShortType.get())
            .withNullable(true)
            .build());
    columns.add(
        new JdbcColumn.Builder().withName("col_3").withType(INT).withNullable(false).build());
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
        new JdbcColumn.Builder().withName("col_10").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_11").withType(VARCHAR).withNullable(false).build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_12")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_13")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_14")
            .withType(Types.BinaryType.get())
            .withNullable(false)
            .build());
    columns.add(
        new JdbcColumn.Builder()
            .withName("col_15")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null,
        Indexes.EMPTY_INDEXES);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(tableName, tableComment, columns, Collections.emptyMap(), null, load);
  }

  @Test
  public void testCreateNotSupportTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    List<Type> notSupportType =
        Arrays.asList(
            Types.BooleanType.get(),
            Types.FixedType.of(10),
            Types.IntervalDayType.get(),
            Types.IntervalYearType.get(),
            Types.TimestampType.withTimeZone(),
            Types.UUIDType.get(),
            Types.ListType.of(Types.DateType.get(), true),
            Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true),
            Types.UnionType.of(Types.IntegerType.get()),
            Types.StructType.of(
                Types.StructType.Field.notNullField("col_1", Types.IntegerType.get())));

    for (Type type : notSupportType) {
      columns.clear();
      columns.add(
          new JdbcColumn.Builder().withName("col_1").withType(type).withNullable(false).build());

      JdbcColumn[] jdbcCols = columns.toArray(new JdbcColumn[0]);
      Map<String, String> emptyMap = Collections.emptyMap();
      IllegalArgumentException illegalArgumentException =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> {
                TABLE_OPERATIONS.create(
                    TEST_DB_NAME,
                    tableName,
                    jdbcCols,
                    tableComment,
                    emptyMap,
                    null,
                    Indexes.EMPTY_INDEXES);
              });
      Assertions.assertTrue(
          illegalArgumentException
              .getMessage()
              .contains(
                  String.format(
                      "Couldn't convert Gravitino type %s to MySQL type", type.simpleString())));
    }
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
              .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.00")))
              .build()
        },
        "test_comment",
        null,
        null,
        Indexes.EMPTY_INDEXES);

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
              .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.00")))
              .build()
        },
        "test_comment",
        null,
        null,
        Indexes.EMPTY_INDEXES);

    tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tables.contains(test_table_2));
  }

  @Test
  public void testLoadTableDefaultProperties() {
    String test_table_1 = GravitinoITUtils.genRandomName("properties_table_");
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        test_table_1,
        new JdbcColumn[] {
          new JdbcColumn.Builder()
              .withName("col_1")
              .withType(Types.DecimalType.of(10, 2))
              .withComment("test_decimal")
              .withNullable(false)
              .build()
        },
        "test_comment",
        null,
        null,
        Indexes.EMPTY_INDEXES);
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, test_table_1);
    Assertions.assertEquals("InnoDB", load.properties().get(MYSQL_ENGINE_KEY));
  }

  @Test
  public void testAutoIncrement() {
    String tableName = "test_increment_table_1";
    String comment = "test_comment";
    Map<String, String> properties =
        new HashMap<String, String>() {
          {
            put(MYSQL_AUTO_INCREMENT_OFFSET_KEY, "10");
          }
        };
    JdbcColumn[] columns = {
      new JdbcColumn.Builder()
          .withName("col_1")
          .withType(Types.LongType.get())
          .withComment("id")
          .withAutoIncrement(true)
          .withNullable(false)
          .build(),
      new JdbcColumn.Builder()
          .withName("col_2")
          .withType(Types.VarCharType.of(255))
          .withComment("city")
          .withNullable(false)
          .build(),
      new JdbcColumn.Builder()
          .withName("col_3")
          .withType(Types.VarCharType.of(255))
          .withComment("name")
          .withNullable(false)
          .build()
    };
    // Test create increment key for unique index.
    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}}),
          Indexes.unique("uk_1", new String[][] {{"col_1"}})
        };
    TABLE_OPERATIONS.create(TEST_DB_NAME, tableName, columns, comment, properties, null, indexes);

    JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        table);
    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create increment key for primary index.
    indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}}),
          Indexes.unique("uk_2", new String[][] {{"col_2"}})
        };
    TABLE_OPERATIONS.create(TEST_DB_NAME, tableName, columns, comment, properties, null, indexes);

    table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        table);
    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create increment key for col_1 + col_3 uk.
    indexes = new Index[] {Indexes.unique("uk_2_3", new String[][] {{"col_1"}, {"col_3"}})};
    TABLE_OPERATIONS.create(TEST_DB_NAME, tableName, columns, comment, properties, null, indexes);

    table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        table);
    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create auto increment fail
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.create(
                    TEST_DB_NAME,
                    tableName,
                    columns,
                    comment,
                    properties,
                    null,
                    Indexes.EMPTY_INDEXES));
    Assertions.assertTrue(
        StringUtils.contains(
            exception.getMessage(),
            "Incorrect table definition; there can be only one auto column and it must be defined as a key"));

    // Test create many auto increment col
    JdbcColumn[] newColumns = {
      columns[0],
      columns[1],
      columns[2],
      new JdbcColumn.Builder()
          .withName("col_4")
          .withType(Types.IntegerType.get())
          .withComment("test_id")
          .withAutoIncrement(true)
          .withNullable(false)
          .build()
    };

    final Index[] primaryIndex =
        new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_4"}})};
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.create(
                    TEST_DB_NAME, tableName, newColumns, comment, properties, null, primaryIndex));
    Assertions.assertTrue(
        StringUtils.contains(
            exception.getMessage(),
            "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: [col_1,col_4]"));
  }
}
