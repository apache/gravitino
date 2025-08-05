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
package org.apache.gravitino.catalog.oceanbase.operation;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestOceanBaseTableOperations extends TestOceanBase {
  private static final Type VARCHAR = Types.VarCharType.of(255);
  private static final Type INT = Types.IntegerType.get();

  @BeforeAll
  public static void setUp() {
    DATABASE_OPERATIONS.create(TEST_DB_NAME, null, new HashMap<>());
  }

  @Test
  public void testOperationTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16).toLowerCase() + "_op_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(VARCHAR)
            .withComment("test_comment")
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set primary key")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_3")
            .withType(INT)
            .withNullable(true)
            .withDefaultValue(Literals.NULL)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes = new Index[] {Indexes.unique("test", new String[][] {{"col_1"}, {"col_2"}})};
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

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
            .withDefaultValue(Literals.of("hello test", VARCHAR))
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()},
            newColumn.dataType(),
            newColumn.comment(),
            TableChange.ColumnPosition.after("col_1"),
            newColumn.defaultValue()));
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
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    assertionsTableInfo(
        newName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

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
    Assertions.assertTrue(TABLE_OPERATIONS.drop(TEST_DB_NAME, newName), "table should be dropped");
    Assertions.assertFalse(
        TABLE_OPERATIONS.drop(TEST_DB_NAME, newName), "table should be non-existent");
  }

  @Test
  public void testAlterTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16).toLowerCase() + "_al_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(INT)
            .withComment("id")
            .withNullable(false)
            .build();
    columns.add(col_1);
    JdbcColumn col_2 =
        JdbcColumn.builder()
            .withName("col_2")
            .withType(VARCHAR)
            .withComment("name")
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build();
    columns.add(col_2);
    JdbcColumn col_3 =
        JdbcColumn.builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withComment("name")
            .withDefaultValue(Literals.NULL)
            .build();
    // `col_1` int NOT NULL COMMENT 'id' ,
    // `col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'name' ,
    // `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
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
        Distributions.NONE,
        indexes);
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, VARCHAR));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    // After modifying the type, some attributes of the corresponding column are not
    // supported.
    columns.clear();
    col_1 =
        JdbcColumn.builder()
            .withName(col_1.name())
            .withType(VARCHAR)
            .withComment(col_1.comment())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    String newComment = "new_comment";
    // update table comment and column comment
    // `col_1` int NOT NULL COMMENT 'id' ,
    // `col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment' ,
    // `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, INT),
        TableChange.updateColumnComment(new String[] {col_2.name()}, newComment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();
    col_1 =
        JdbcColumn.builder()
            .withName(col_1.name())
            .withType(INT)
            .withComment(col_1.comment())
            .withAutoIncrement(col_1.autoIncrement())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    col_2 =
        JdbcColumn.builder()
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
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    String newColName_1 = "new_col_1";
    String newColName_2 = "new_col_2";
    // rename column
    // update table comment and column comment
    // `new_col_1` int NOT NULL COMMENT 'id' ,
    // `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment'
    // ,
    // `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.renameColumn(new String[] {col_1.name()}, newColName_1),
        TableChange.renameColumn(new String[] {col_2.name()}, newColName_2));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();
    col_1 =
        JdbcColumn.builder()
            .withName(newColName_1)
            .withType(col_1.dataType())
            .withComment(col_1.comment())
            .withAutoIncrement(col_1.autoIncrement())
            .withNullable(col_1.nullable())
            .withDefaultValue(col_1.defaultValue())
            .build();
    col_2 =
        JdbcColumn.builder()
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
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    newComment = "txt3";
    String newCol2Comment = "xxx";
    // update column position add column、set table properties
    // `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'xxx' ,
    // `new_col_1` int NOT NULL COMMENT 'id' ,
    // `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    // `col_4` varchar(255) NOT NULL COMMENT 'txt4' ,
    // `col_5` varchar(255) COMMENT 'hello world' DEFAULT 'hello world' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {newColName_1}, TableChange.ColumnPosition.after(newColName_2)),
        TableChange.addColumn(new String[] {"col_4"}, VARCHAR, "txt4", false),
        TableChange.updateColumnComment(new String[] {newColName_2}, newCol2Comment),
        TableChange.addColumn(
            new String[] {"col_5"}, VARCHAR, "txt5", Literals.of("hello world", VARCHAR)));
    TABLE_OPERATIONS.alterTable(TEST_DB_NAME, tableName, TableChange.updateComment(newComment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    columns.clear();

    columns.add(
        JdbcColumn.builder()
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
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withComment("txt4")
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_5")
            .withType(VARCHAR)
            .withComment("txt5")
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(true)
            .build());
    assertionsTableInfo(
        tableName, newComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    // `new_col_2` varchar(255) NOT NULL DEFAULT 'hello world' COMMENT 'xxx' ,
    // `col_3` varchar(255) NULL DEFAULT NULL COMMENT 'name' ,
    // `col_4` varchar(255) NULL COMMENT 'txt4' ,
    // `col_5` varchar(255) COMMENT 'hello world' DEFAULT 'hello world' ,
    // `new_col_1` int NOT NULL COMMENT 'id' ,
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
    JdbcColumn col_5 = columns.remove(1);
    columns.clear();

    columns.add(
        JdbcColumn.builder()
            .withName("new_col_2")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("xxx")
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .build());
    columns.add(col3);
    columns.add(
        JdbcColumn.builder()
            .withName(col_4.name())
            .withType(col_4.dataType())
            .withNullable(!col_4.nullable())
            .withComment(col_4.comment())
            .withDefaultValue(col_4.defaultValue())
            .build());
    columns.add(col_5);
    columns.add(col_1);

    assertionsTableInfo(
        tableName, newComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

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
  public void testAlterTableUpdateColumnDefaultValue() {
    String tableName = RandomNameUtils.genRandomName("properties_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.DecimalType.of(10, 2))
            .withComment("test_decimal")
            .withNullable(false)
            .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.00", 10, 2)))
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.LongType.get())
            .withNullable(false)
            .withDefaultValue(Literals.longLiteral(0L))
            .withComment("long type")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_3")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .withNullable(false)
            .withComment("timestamp")
            .withDefaultValue(Literals.timestampLiteral(LocalDateTime.parse("2013-01-01T00:00:00")))
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withComment("varchar")
            .withDefaultValue(Literals.of("hello", Types.VarCharType.of(255)))
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
        Distributions.NONE,
        indexes);

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, loaded);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(0).name()},
            Literals.decimalLiteral(Decimal.of("1.23", 10, 2))),
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(1).name()}, Literals.longLiteral(1L)),
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(2).name()},
            Literals.timestampLiteral(LocalDateTime.parse("2024-04-01T00:00:00"))),
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(3).name()}, Literals.of("world", Types.VarCharType.of(255))));

    loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    Assertions.assertEquals(
        Literals.decimalLiteral(Decimal.of("1.234", 10, 2)), loaded.columns()[0].defaultValue());
    Assertions.assertEquals(Literals.longLiteral(1L), loaded.columns()[1].defaultValue());
    Assertions.assertEquals(
        Literals.timestampLiteral(LocalDateTime.parse("2024-04-01T00:00:00")),
        loaded.columns()[2].defaultValue());
    Assertions.assertEquals(
        Literals.of("world", Types.VarCharType.of(255)), loaded.columns()[3].defaultValue());
  }

  @Test
  public void testCreateAndLoadTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16).toLowerCase() + "_cl_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.DecimalType.of(10, 2))
            .withComment("test_decimal")
            .withNullable(false)
            .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.00", 10, 2)))
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.LongType.get())
            .withNullable(false)
            .withDefaultValue(Literals.longLiteral(0L))
            .withComment("long type")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_3")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .withNullable(false)
            .withComment("timestamp")
            .withDefaultValue(Literals.timestampLiteral(LocalDateTime.parse("2013-01-01T00:00:00")))
            .build());
    columns.add(
        JdbcColumn.builder()
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
        Distributions.NONE,
        indexes);

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, loaded);
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = RandomNameUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.ByteType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.ShortType.get())
            .withNullable(true)
            .build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(INT).withNullable(false).build());
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
            .withType(Types.TimeType.of(0))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder().withName("col_10").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(
        JdbcColumn.builder().withName("col_11").withType(VARCHAR).withNullable(false).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_12")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_13")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_14")
            .withType(Types.BinaryType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
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
  public void testCreateNotSupportTypeTable() {
    String tableName = RandomNameUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    List<Type> notSupportType =
        Arrays.asList(
            Types.FixedType.of(10),
            Types.IntervalDayType.get(),
            Types.IntervalYearType.get(),
            Types.UUIDType.get(),
            Types.ListType.of(Types.DateType.get(), true),
            Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true),
            Types.UnionType.of(Types.IntegerType.get()),
            Types.StructType.of(
                Types.StructType.Field.notNullField("col_1", Types.IntegerType.get())));

    for (Type type : notSupportType) {
      columns.clear();
      columns.add(
          JdbcColumn.builder().withName("col_1").withType(type).withNullable(false).build());

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
                    Distributions.NONE,
                    Indexes.EMPTY_INDEXES);
              });
      System.out.println(illegalArgumentException.getMessage());
      Assertions.assertTrue(
          illegalArgumentException
              .getMessage()
              .contains(
                  String.format(
                      "Couldn't convert Gravitino type %s to OceanBase type",
                      type.simpleString())));
    }
  }

  @Test
  public void testCreateMultipleTables() {
    String test_table_1 = "test_table_1";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        test_table_1,
        new JdbcColumn[] {
          JdbcColumn.builder()
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
        Distributions.NONE,
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
          JdbcColumn.builder()
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
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tables.contains(test_table_2));
  }

  @Test
  public void testAutoIncrement() {
    String tableName = "test_increment_table_1";
    String comment = "test_comment";
    Map<String, String> properties =
        new HashMap<String, String>() {
          {
            put("AUTO_INCREMENT", "10");
          }
        };
    JdbcColumn[] columns = {
      JdbcColumn.builder()
          .withName("col_1")
          .withType(Types.LongType.get())
          .withComment("id")
          .withAutoIncrement(true)
          .withNullable(false)
          .build(),
      JdbcColumn.builder()
          .withName("col_2")
          .withType(Types.VarCharType.of(255))
          .withComment("city")
          .withNullable(false)
          .build(),
      JdbcColumn.builder()
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
    TABLE_OPERATIONS.create(
        TEST_DB_NAME, tableName, columns, comment, properties, null, Distributions.NONE, indexes);

    JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);
    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create increment key for primary index.
    indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}}),
          Indexes.unique("uk_2", new String[][] {{"col_2"}})
        };
    TABLE_OPERATIONS.create(
        TEST_DB_NAME, tableName, columns, comment, properties, null, Distributions.NONE, indexes);

    table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);
    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create increment key for col_1 + col_3 uk.
    indexes = new Index[] {Indexes.unique("uk_2_3", new String[][] {{"col_1"}, {"col_3"}})};
    TABLE_OPERATIONS.create(
        TEST_DB_NAME, tableName, columns, comment, properties, null, Distributions.NONE, indexes);

    table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        comment,
        Arrays.stream(columns).collect(Collectors.toList()),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
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
                    Distributions.NONE,
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
      JdbcColumn.builder()
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
                    TEST_DB_NAME,
                    tableName,
                    newColumns,
                    comment,
                    properties,
                    null,
                    Distributions.NONE,
                    primaryIndex));
    Assertions.assertTrue(
        StringUtils.contains(
            exception.getMessage(),
            "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: [col_1,col_4]"));
  }

  @Test
  public void testAppendIndexesBuilder() {
    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}, {"col_1"}}),
          Indexes.unique("uk_col_4", new String[][] {{"col_4"}}),
          Indexes.unique("uk_col_5", new String[][] {{"col_4"}, {"col_5"}}),
          Indexes.unique("uk_col_6", new String[][] {{"col_4"}, {"col_5"}, {"col_6"}})
        };
    StringBuilder sql = new StringBuilder();
    OceanBaseTableOperations.appendIndexesSql(indexes, sql);
    String expectedStr =
        ",\n"
            + "CONSTRAINT PRIMARY KEY (`col_2`, `col_1`),\n"
            + "CONSTRAINT `uk_col_4` UNIQUE (`col_4`),\n"
            + "CONSTRAINT `uk_col_5` UNIQUE (`col_4`, `col_5`),\n"
            + "CONSTRAINT `uk_col_6` UNIQUE (`col_4`, `col_5`, `col_6`)";
    Assertions.assertEquals(expectedStr, sql.toString());

    indexes =
        new Index[] {
          Indexes.unique("uk_1", new String[][] {{"col_4"}}),
          Indexes.unique("uk_2", new String[][] {{"col_4"}, {"col_3"}}),
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_2"}, {"col_1"}, {"col_3"}}),
          Indexes.unique("uk_3", new String[][] {{"col_4"}, {"col_5"}, {"col_6"}, {"col_7"}})
        };
    sql = new StringBuilder();
    OceanBaseTableOperations.appendIndexesSql(indexes, sql);
    expectedStr =
        ",\n"
            + "CONSTRAINT `uk_1` UNIQUE (`col_4`),\n"
            + "CONSTRAINT `uk_2` UNIQUE (`col_4`, `col_3`),\n"
            + "CONSTRAINT PRIMARY KEY (`col_2`, `col_1`, `col_3`),\n"
            + "CONSTRAINT `uk_3` UNIQUE (`col_4`, `col_5`, `col_6`, `col_7`)";
    Assertions.assertEquals(expectedStr, sql.toString());
  }

  @Test
  public void testOperationIndexDefinition() {
    TableChange.AddIndex failIndex =
        new TableChange.AddIndex(Index.IndexType.PRIMARY_KEY, "pk_1", new String[][] {{"col_1"}});
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> OceanBaseTableOperations.addIndexDefinition(failIndex));
    Assertions.assertTrue(
        illegalArgumentException
            .getMessage()
            .contains("Primary key name must be PRIMARY in OceanBase"));

    TableChange.AddIndex successIndex =
        new TableChange.AddIndex(
            Index.IndexType.UNIQUE_KEY, "uk_1", new String[][] {{"col_1"}, {"col_2"}});
    String sql = OceanBaseTableOperations.addIndexDefinition(successIndex);
    Assertions.assertEquals("ADD UNIQUE INDEX `uk_1` (`col_1`, `col_2`)", sql);

    successIndex =
        new TableChange.AddIndex(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"col_1"}, {"col_2"}});
    sql = OceanBaseTableOperations.addIndexDefinition(successIndex);
    Assertions.assertEquals("ADD PRIMARY KEY  (`col_1`, `col_2`)", sql);

    String tableName = RandomStringUtils.randomAlphabetic(16).toLowerCase() + "_oi_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(INT)
            .withNullable(false)
            .withComment("set primary key")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(VARCHAR)
            .withComment("test_comment")
            .withNullable(true)
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes = new Index[] {Indexes.unique("uk_2", new String[][] {{"col_1"}})};
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes);

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);

    // If ifExists is set to true then the code should not throw an exception if the index doesn't
    // exist.
    TableChange.DeleteIndex deleteIndex = new TableChange.DeleteIndex("uk_1", true);
    sql = OceanBaseTableOperations.deleteIndexDefinition(load, deleteIndex);
    Assertions.assertEquals("DROP INDEX `uk_1`", sql);

    // The index existence check should only verify existence when ifExists is false, preventing
    // failures when dropping non-existent indexes.
    TableChange.DeleteIndex deleteIndex2 = new TableChange.DeleteIndex("uk_1", false);
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> OceanBaseTableOperations.deleteIndexDefinition(load, deleteIndex2));
    Assertions.assertEquals("Index does not exist", thrown.getMessage());

    TableChange.DeleteIndex deleteIndex3 = new TableChange.DeleteIndex("uk_2", false);
    sql = OceanBaseTableOperations.deleteIndexDefinition(load, deleteIndex3);
    Assertions.assertEquals("DROP INDEX `uk_2`", sql);
  }

  @Test
  public void testCalculateDatetimePrecision() {
    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("DATE", 10, 0),
        "DATE type should return 0 precision");

    Assertions.assertEquals(
        1,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIME", 10, 0),
        "TIME type should return 0 precision");

    Assertions.assertEquals(
        3,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMESTAMP", 23, 0),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        6,
        TABLE_OPERATIONS.calculateDatetimePrecision("DATETIME", 26, 0),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        9,
        TABLE_OPERATIONS.calculateDatetimePrecision("timestamp", 29, 0),
        "Lower case type name should work");

    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("VARCHAR", 50, 0),
        "Non-datetime type should return 0 precision");
  }

  @Test
  public void testCalculateDatetimePrecisionWithUnsupportedDriverVersion() {
    OceanBaseTableOperations operationsWithOldDriver =
        new OceanBaseTableOperations() {
          @Override
          protected String getMySQLDriverVersion() {
            return "mysql-connector-java-8.0.11 (Revision: a0ca826f5cdf51a98356fdfb1bf251eb042f80bf)";
          }

          @Override
          public boolean isMySQLDriverVersionSupported(String driverVersion) {
            return false;
          }
        };

    Assertions.assertNull(
        operationsWithOldDriver.calculateDatetimePrecision("TIMESTAMP", 26, 0),
        "TIMESTAMP type should return null for unsupported driver version");

    Assertions.assertNull(
        operationsWithOldDriver.calculateDatetimePrecision("DATETIME", 26, 0),
        "DATETIME type should return null for unsupported driver version");

    Assertions.assertNull(
        operationsWithOldDriver.calculateDatetimePrecision("TIME", 16, 0),
        "TIME type should return null for unsupported driver version");
  }

  @Test
  public void testCalculateDatetimePrecisionWithOceanBaseDriver() {
    OceanBaseTableOperations operationsWithOceanBaseDriver =
        new OceanBaseTableOperations() {
          @Override
          protected String getMySQLDriverVersion() {
            return "com.oceanbase.jdbc.Driver-1.0.0";
          }

          @Override
          public boolean isMySQLDriverVersionSupported(String driverVersion) {
            return true;
          }
        };

    Assertions.assertEquals(
        3,
        operationsWithOceanBaseDriver.calculateDatetimePrecision("TIMESTAMP", 23, 0),
        "TIMESTAMP type should return precision for OceanBase driver");

    Assertions.assertEquals(
        6,
        operationsWithOceanBaseDriver.calculateDatetimePrecision("DATETIME", 26, 0),
        "DATETIME type should return precision for OceanBase driver");

    Assertions.assertEquals(
        1,
        operationsWithOceanBaseDriver.calculateDatetimePrecision("TIME", 10, 0),
        "TIME type should return precision for OceanBase driver");
  }
}
