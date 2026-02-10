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
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseUtils.getSortOrders;
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
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.clickhouse.ClickHouseUtils;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
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
  public void testCreateAndAlterTable() {
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

    // alter table add column
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName("col_5")
            .withType(STRING)
            .withComment("new_add")
            .withNullable(false) //
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
            newColumn.defaultValue()));
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
            "Alter table properties in ClickHouse is not supported"));

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
        "Delete column '%s' does not exist.".formatted(newColumn.name()),
        illegalArgumentException.getMessage());
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

    // Update column type
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
            .withType(Types.TimestampType.withoutTimeZone(0))
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
        Literals.decimalLiteral(Decimal.of("1.23", 10, 2)), loaded.columns()[0].defaultValue());
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
  public void testTypeConversionAgainstCluster() {
    String tableName = RandomStringUtils.randomAlphabetic(16) + "_type_conv";

    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("c_int8")
            .withType(Types.ByteType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_uint8")
            .withType(Types.ByteType.unsigned())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_int16")
            .withType(Types.ShortType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_uint16")
            .withType(Types.ShortType.unsigned())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_int32")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_uint32")
            .withType(Types.IntegerType.unsigned())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_decimal")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_uint64")
            .withType(Types.LongType.unsigned())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_float32")
            .withType(Types.FloatType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_float64")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_fixed")
            .withType(Types.FixedCharType.of(3))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_string")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_varchar")
            .withType(Types.VarCharType.of(5))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_date")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_ts")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_dt64")
            .withType(Types.ExternalType.of("DateTime64(3)"))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_bool")
            .withType(Types.BooleanType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_uuid")
            .withType(Types.UUIDType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("c_ipv4")
            .withType(Types.ExternalType.of("IPv4"))
            .withNullable(false)
            .build());

    Map<String, String> properties = new HashMap<>();
    Index[] indexes =
        new Index[] {
          Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"c_int8"}})
        };

    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        tableName,
        columns.toArray(new JdbcColumn[0]),
        "type conversion",
        properties,
        null,
        Distributions.NONE,
        indexes,
        getSortOrders("c_int8"));

    JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), tableName);

    Assertions.assertEquals(Types.ByteType.get(), loaded.columns()[0].dataType());
    Assertions.assertEquals(Types.ByteType.unsigned(), loaded.columns()[1].dataType());
    Assertions.assertEquals(Types.ShortType.get(), loaded.columns()[2].dataType());
    Assertions.assertEquals(Types.ShortType.unsigned(), loaded.columns()[3].dataType());
    Assertions.assertEquals(Types.IntegerType.get(), loaded.columns()[4].dataType());
    Assertions.assertEquals(Types.IntegerType.unsigned(), loaded.columns()[5].dataType());
    Assertions.assertEquals(Types.DecimalType.of(10, 2), loaded.columns()[6].dataType());
    Assertions.assertEquals(Types.LongType.unsigned(), loaded.columns()[7].dataType());
    Assertions.assertEquals(Types.FloatType.get(), loaded.columns()[8].dataType());
    Assertions.assertEquals(Types.DoubleType.get(), loaded.columns()[9].dataType());
    Assertions.assertEquals(Types.FixedCharType.of(3), loaded.columns()[10].dataType());
    Assertions.assertEquals(Types.StringType.get(), loaded.columns()[11].dataType());
    Assertions.assertEquals(Types.StringType.get(), loaded.columns()[12].dataType());
    Assertions.assertEquals(Types.DateType.get(), loaded.columns()[13].dataType());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(0), loaded.columns()[14].dataType());
    Assertions.assertEquals(
        Types.ExternalType.of("DateTime64(3)"), loaded.columns()[15].dataType());
    Assertions.assertEquals(Types.BooleanType.get(), loaded.columns()[16].dataType());
    Assertions.assertEquals(Types.UUIDType.get(), loaded.columns()[17].dataType());
    Assertions.assertEquals(Types.ExternalType.of("IPv4"), loaded.columns()[18].dataType());

    Assertions.assertTrue(TABLE_OPERATIONS.drop(TEST_DB_NAME.toString(), tableName));
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
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone(0))
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
    String testTable1 = "test_table_1";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        testTable1,
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
    Assertions.assertFalse(tables.contains(testTable1));

    String testTable2 = "test_table_2";
    TABLE_OPERATIONS.create(
        testDb,
        testTable2,
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
    Assertions.assertFalse(tables.contains(testTable2));
  }

  @Test
  public void testLoadTableDefaultProperties() {
    String testTable1 = RandomNameUtils.genRandomName("properties_table_");
    TABLE_OPERATIONS.create(
        TEST_DB_NAME.toString(),
        testTable1,
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
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME.toString(), testTable1);
    Assertions.assertEquals("MergeTree", load.properties().get(CLICKHOUSE_ENGINE_KEY));
  }

  @Test
  public void testGenerateCreateTableSqlBranchCoverage() {
    TestableClickHouseTableOperations ops = new TestableClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());

    JdbcColumn col =
        JdbcColumn.builder()
            .withName("c1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Index[] indexes =
        new Index[] {
          Indexes.of(
              Index.IndexType.PRIMARY_KEY,
              Indexes.DEFAULT_PRIMARY_KEY_NAME,
              new String[][] {{"c1"}})
        };

    // partitioning not supported
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.buildCreateSql(
                "t1",
                new JdbcColumn[] {col},
                null,
                new HashMap<>(),
                new Transform[] {Transforms.identity("p")},
                Distributions.NONE,
                indexes,
                ClickHouseUtils.getSortOrders("c1")));

    // distribution not NONE
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "t1",
                new JdbcColumn[] {col},
                null,
                new HashMap<>(),
                new Transform[0],
                Distributions.hash(4, NamedReference.field("c1")),
                indexes,
                ClickHouseUtils.getSortOrders("c1")));

    // MergeTree requires ORDER BY
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "t1",
                new JdbcColumn[] {col},
                null,
                new HashMap<>(),
                new Transform[0],
                Distributions.NONE,
                indexes,
                new SortOrder[0]));

    // MergeTree only supports single sortOrders
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.buildCreateSql(
                "t1",
                new JdbcColumn[] {col},
                null,
                new HashMap<>(),
                new Transform[0],
                Distributions.NONE,
                indexes,
                new SortOrder[] {
                  ClickHouseUtils.getSortOrders("c1")[0], ClickHouseUtils.getSortOrders("c1")[0]
                }));

    // Engine without ORDER BY support should fail when sortOrders provided
    Map<String, String> logEngineProps = new HashMap<>();
    logEngineProps.put(
        TableConstants.ENGINE_UPPER, ClickHouseTablePropertiesMetadata.ENGINE.LOG.getValue());
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.buildCreateSql(
                "t1",
                new JdbcColumn[] {col},
                null,
                logEngineProps,
                new Transform[0],
                Distributions.NONE,
                indexes,
                ClickHouseUtils.getSortOrders("c1")));

    // Settings and comment retained
    Map<String, String> props = new HashMap<>();
    props.put(
        TableConstants.ENGINE_UPPER, ClickHouseTablePropertiesMetadata.ENGINE.MERGETREE.getValue());
    props.put(ClickHouseConstants.TableConstants.SETTINGS_PREFIX + "max_threads", "8");
    String sql =
        ops.buildCreateSql(
            "t1",
            new JdbcColumn[] {col},
            "co'mment",
            props,
            new Transform[0],
            Distributions.NONE,
            indexes,
            ClickHouseUtils.getSortOrders("c1"));
    Assertions.assertTrue(sql.contains("ENGINE = MergeTree"));
    Assertions.assertTrue(sql.contains("SETTINGS max_threads = 8"));
    Assertions.assertTrue(sql.contains("COMMENT 'co''mment'"));
  }

  private static final class TestableClickHouseTableOperations extends ClickHouseTableOperations {
    String buildCreateSql(
        String tableName,
        JdbcColumn[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitioning,
        org.apache.gravitino.rel.expressions.distributions.Distribution distribution,
        Index[] indexes,
        SortOrder[] sortOrders) {
      return generateCreateTableSql(
          tableName, columns, comment, properties, partitioning, distribution, indexes, sortOrders);
    }
  }

  @Test
  public void testGenerateAlterTableSqlCoverage() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());

    JdbcTable table = buildStubTable();
    ops.setTable(table);

    TableChange[] changes =
        new TableChange[] {
          TableChange.addColumn(
              new String[] {"c_new"},
              Types.StringType.get(),
              "new column",
              TableChange.ColumnPosition.after("c1"),
              true,
              false,
              Column.DEFAULT_VALUE_NOT_SET),
          TableChange.updateColumnDefaultValue(
              new String[] {"c2"}, Literals.of("val", Types.StringType.get())),
          TableChange.updateColumnType(new String[] {"c1"}, Types.LongType.get()),
          TableChange.updateColumnComment(new String[] {"c1"}, "c1_comment"),
          TableChange.updateColumnPosition(new String[] {"c1"}, TableChange.ColumnPosition.first()),
          TableChange.deleteColumn(new String[] {"c3"}, false),
          TableChange.updateColumnNullability(new String[] {"c2"}, false),
          TableChange.deleteIndex("idx1", false),
          TableChange.renameColumn(new String[] {"c2"}, "c2_new"),
          TableChange.updateComment("new_table_comment")
        };

    String sql = ops.buildAlterSql("db", "tbl", changes);

    Assertions.assertTrue(sql.contains("ADD COLUMN `c_new` Nullable(String)"));
    Assertions.assertTrue(sql.contains("RENAME COLUMN `c2` TO `c2_new`"));
    Assertions.assertTrue(sql.contains("DEFAULT 'val'"));
    Assertions.assertTrue(sql.contains("Int64"));
    Assertions.assertTrue(sql.contains("COMMENT 'c1_comment'"));
    Assertions.assertTrue(sql.contains("FIRST"));
    Assertions.assertTrue(sql.contains("DROP COLUMN `c3`"));
    Assertions.assertTrue(sql.contains("DROP INDEX `idx1`"));
    Assertions.assertTrue(sql.contains("MODIFY COMMENT 'new_table_comment'"));
    Assertions.assertTrue(sql.startsWith("ALTER TABLE `tbl`"));
  }

  @Test
  public void testAlterTableDeleteColumnIfExistsNoOpReturnsEmpty() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTable());

    String sql =
        ops.buildAlterSql(
            "db",
            "tbl",
            new TableChange[] {TableChange.deleteColumn(new String[] {"missing"}, true)});
    Assertions.assertEquals("", sql);
  }

  @Test
  public void testAlterTableDeleteIndexBranches() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTable());

    String sqlSkip =
        ops.buildAlterSql(
            "db", "tbl", new TableChange[] {TableChange.deleteIndex("missing", true)});
    Assertions.assertEquals("", sqlSkip);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildAlterSql(
                "db", "tbl", new TableChange[] {TableChange.deleteIndex("missing", false)}));
  }

  @Test
  public void testAlterTableNullabilityValidationFails() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTableWithNullDefault());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildAlterSql(
                "db",
                "tbl",
                new TableChange[] {
                  TableChange.updateColumnNullability(new String[] {"c3"}, false)
                }));
  }

  @Test
  public void testAlterTableSetPropertyUnsupported() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTable());

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.buildAlterSql("db", "tbl", new TableChange[] {TableChange.setProperty("k", "v")}));
  }

  @Test
  public void testAlterTableRemovePropertyUnsupported() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTable());

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> ops.buildAlterSql("db", "tbl", new TableChange[] {TableChange.removeProperty("k")}));
  }

  private static JdbcTable buildStubTable() {
    JdbcColumn c1 =
        JdbcColumn.builder()
            .withName("c1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .build();
    JdbcColumn c2 =
        JdbcColumn.builder()
            .withName("c2")
            .withType(Types.StringType.get())
            .withNullable(true)
            .withDefaultValue(Literals.of("x", Types.StringType.get()))
            .build();
    JdbcColumn c3 =
        JdbcColumn.builder()
            .withName("c3")
            .withType(Types.StringType.get())
            .withNullable(true)
            .withDefaultValue(DEFAULT_VALUE_NOT_SET)
            .build();
    return JdbcTable.builder()
        .withName("tbl")
        .withColumns(new JdbcColumn[] {c1, c2, c3})
        .withIndexes(
            new Index[] {
              Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"c1"}}),
              Indexes.unique("idx1", new String[][] {{"c2"}})
            })
        .withComment("table_comment")
        .withTableOperation(null)
        .build();
  }

  private static JdbcTable buildStubTableWithNullDefault() {
    JdbcColumn c1 = JdbcColumn.builder().withName("c1").withType(Types.IntegerType.get()).build();
    JdbcColumn c2 = JdbcColumn.builder().withName("c2").withType(Types.StringType.get()).build();
    JdbcColumn c3 =
        JdbcColumn.builder()
            .withName("c3")
            .withType(Types.StringType.get())
            .withNullable(true)
            .withDefaultValue(Literals.NULL)
            .build();
    return JdbcTable.builder()
        .withName("tbl")
        .withColumns(new JdbcColumn[] {c1, c2, c3})
        .withIndexes(
            new Index[] {
              Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"c1"}})
            })
        .withComment("table_comment")
        .withTableOperation(null)
        .build();
  }

  private static final class StubClickHouseTableOperations extends ClickHouseTableOperations {
    private JdbcTable table;

    void setTable(JdbcTable table) {
      this.table = table;
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      return table;
    }

    String buildAlterSql(String db, String tableName, TableChange[] changes) {
      return generateAlterTableSql(db, tableName, changes);
    }
  }
}
