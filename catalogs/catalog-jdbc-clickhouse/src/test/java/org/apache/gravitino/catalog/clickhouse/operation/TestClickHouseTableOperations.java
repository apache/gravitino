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
package org.apache.gravitino.catalog.clickhouse.operation;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.converter.ClickHouseUtils.getSortOrders;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestClickHouseTableOperations extends TestClickHouse {
  private static final Type STRING = Types.StringType.get();
  private static final Type INT = Types.IntegerType.get();
  private static final Type LONG = Types.LongType.get();

  @Test
  public void testOperationTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16) + "_op_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(STRING)
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
            .withType(STRING)
            .withDefaultValue(Literals.of("hello world", STRING))
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes = new Index[] {};
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes,
        getSortOrders("col_2"));

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME.toString());
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    // rename table
    String newName = "new_table";
    Assertions.assertDoesNotThrow(
        () -> TABLE_OPERATIONS.rename(TEST_DB_NAME.toString(), tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), newName));

    // alter table
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName("col_5")
            .withType(STRING)
            .withComment("new_add")
            .withNullable(false) //
            //            .withDefaultValue(Literals.of("hello test", STRING))
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()},
            newColumn.dataType(),
            newColumn.comment(),
            TableChange.ColumnPosition.after("col_1"),
            newColumn.nullable(),
            newColumn.autoIncrement(),
            newColumn.defaultValue())
        //        ,
        //        TableChange.setProperty(CLICKHOUSE_ENGINE_KEY, "InnoDB"));
        //    properties.put(CLICKHOUSE_ENGINE_KEY, "InnoDB"
        );
    load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), newName);
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

    // Detect unsupported properties
    TableChange setProperty = TableChange.setProperty(CLICKHOUSE_ENGINE_KEY, "ABC");
    UnsupportedOperationException gravitinoRuntimeException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME.toString(), newName, setProperty));
    Assertions.assertTrue(
        StringUtils.contains(
            gravitinoRuntimeException.getMessage(),
            "alter table properties in ck is not supported"));

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        newName,
        TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), newName);
    assertionsTableInfo(
        newName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    TableChange deleteColumn = TableChange.deleteColumn(new String[] {newColumn.name()}, false);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME.toString(), newName, deleteColumn));
    Assertions.assertEquals(
        "Delete column does not exist: " + newColumn.name(), illegalArgumentException.getMessage());
    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                TEST_DB_NAME.toString(),
                newName,
                TableChange.deleteColumn(new String[] {newColumn.name()}, true)));

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        newName,
        TableChange.deleteColumn(new String[] {newColumn.name()}, true));
    Assertions.assertTrue(
        TABLE_OPERATIONS.drop(TEST_DB_NAME.toString(), newName), "table should be dropped");

    GravitinoRuntimeException exception =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> TABLE_OPERATIONS.drop(TEST_DB_NAME.toString(), newName));
    Assertions.assertTrue(StringUtils.contains(exception.getMessage(), "does not exist"));
  }

  @Test
  public void testAlterTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16) + "_al_table";
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(INT)
            .withComment("id")
            .withNullable(false)
            .withDefaultValue(Literals.integerLiteral(0))
            .build();
    columns.add(col_1);
    JdbcColumn col_2 =
        JdbcColumn.builder()
            .withName("col_2")
            .withType(STRING)
            .withComment("name")
            .withDefaultValue(Literals.of("hello world", STRING))
            .withNullable(false)
            .build();
    columns.add(col_2);
    JdbcColumn col_3 =
        JdbcColumn.builder()
            .withName("col_3")
            .withType(STRING)
            .withComment("name")
            .withDefaultValue(Literals.NULL)
            .build();
    // `col_1` int NOT NULL COMMENT 'id' ,
    // `col_2` STRING(255) NOT NULL DEFAULT 'hello world' COMMENT 'name' ,
    // `col_3` STRING(255) NULL DEFAULT NULL COMMENT 'name' ,
    columns.add(col_3);
    Map<String, String> properties = new HashMap<>();

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        null,
        getSortOrders("col_2"));
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, LONG));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);

    // After modifying the type, some attributes of the corresponding column are not
    // supported.
    columns.clear();
    col_1 =
        JdbcColumn.builder()
            .withName(col_1.name())
            .withType(LONG)
            .withComment(col_1.comment())
            .withNullable(col_1.nullable())
            .withDefaultValue(Literals.longLiteral(0L))
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    String newComment = "new_comment";
    // update table comment and column comment
    // `col_1` int NOT NULL COMMENT 'id' ,
    // `col_2` STRING(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment' ,
    // `col_3` STRING(255) NULL DEFAULT NULL COMMENT 'name' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        tableName,
        TableChange.updateColumnType(new String[] {col_1.name()}, INT),
        TableChange.updateColumnComment(new String[] {col_2.name()}, newComment));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);

    columns.clear();
    col_1 =
        JdbcColumn.builder()
            .withName(col_1.name())
            .withType(INT)
            .withComment(col_1.comment())
            .withAutoIncrement(col_1.autoIncrement())
            .withNullable(col_1.nullable())
            .withDefaultValue(Literals.integerLiteral(0))
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
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    String newColName_1 = "new_col_1";
    // rename column
    // update table comment and column comment
    // `new_col_1` int NOT NULL COMMENT 'id' ,
    // `new_col_2` STRING(255) NOT NULL DEFAULT 'hello world' COMMENT 'new_comment'
    // ,
    // `col_3` STRING(255) NULL DEFAULT NULL COMMENT 'name' ,
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
        tableName,
        TableChange.renameColumn(new String[] {col_1.name()}, newColName_1));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);

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
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);
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
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(false)
            .withComment("timestamp")
            .withDefaultValue(Literals.timestampLiteral(LocalDateTime.parse("2013-01-01T00:00:00")))
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(Types.StringType.get())
            .withNullable(false)
            .withComment("STRING")
            .withDefaultValue(Literals.of("hello", Types.StringType.get()))
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes = new Index[0];
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes,
        getSortOrders("col_2"));

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, loaded);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME.toString(),
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
            new String[] {columns.get(3).name()}, Literals.of("world", Types.StringType.get())));

    loaded = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
    Assertions.assertEquals(
        Literals.decimalLiteral(Decimal.of("1.234", 10, 2)), loaded.columns()[0].defaultValue());
    Assertions.assertEquals(Literals.longLiteral(1L), loaded.columns()[1].defaultValue());
    Assertions.assertEquals(
        Literals.timestampLiteral(LocalDateTime.parse("2024-04-01T00:00:00")),
        loaded.columns()[2].defaultValue());
    Assertions.assertEquals(
        Literals.of("world", Types.StringType.get()), loaded.columns()[3].defaultValue());
  }

  @Test
  public void testCreateAndLoadTable() {
    String tableName = RandomStringUtils.randomAlphabetic(16) + "_cl_table";
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
            .withType(Types.TimestampType.withoutTimeZone())
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
            .withDefaultValue(DEFAULT_VALUE_NOT_SET)
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes = new Index[0];
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes,
        getSortOrders("col_1"));

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
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
    //    columns.add(
    //        JdbcColumn.builder()
    //            .withName("col_8")
    //            .withType(Types.TimeType.get())
    //            .withNullable(false)
    //            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder().withName("col_10").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(
        JdbcColumn.builder().withName("col_11").withType(STRING).withNullable(false).build());
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
    //    columns.add(
    //        JdbcColumn.builder()
    //            .withName("col_14")
    //            .withType(Types.BinaryType.get())
    //            .withNullable(false)
    //            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_15")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES,
        getSortOrders("col_1"));

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);
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
                    TEST_DB_NAME.toString(),
                    tableName,
                    jdbcCols,
                    tableComment,
                    emptyMap,
                    null,
                    Distributions.NONE,
                    Indexes.EMPTY_INDEXES,
                    getSortOrders("col_1"));
              });
      Assertions.assertTrue(
          illegalArgumentException
              .getMessage()
              .contains(
                  String.format(
                      "Couldn't convert Gravitino type %s to ClickHouse type",
                      type.simpleString())));
    }
  }

  @Test
  public void testCreateMultipleTables() {
    String test_table_1 = "test_table_1";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
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
        Indexes.EMPTY_INDEXES,
        getSortOrders("col_1"));

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
        Indexes.EMPTY_INDEXES,
        getSortOrders("col_1"));

    tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME.toString());
    Assertions.assertFalse(tables.contains(test_table_2));
  }

  @Test
  public void testLoadTableDefaultProperties() {
    String test_table_1 = RandomNameUtils.genRandomName("properties_table_");
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        test_table_1,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_1")
              .withType(Types.DecimalType.of(10, 2))
              .withComment("test_decimal")
              .withNullable(false)
              .withDefaultValue(Literals.decimalLiteral(Decimal.of("0.0")))
              .build()
        },
        "test_comment",
        null,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES,
        getSortOrders("col_1"));
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), test_table_1);
    Assertions.assertEquals("MergeTree", load.properties().get(CLICKHOUSE_ENGINE_KEY));
  }
}
