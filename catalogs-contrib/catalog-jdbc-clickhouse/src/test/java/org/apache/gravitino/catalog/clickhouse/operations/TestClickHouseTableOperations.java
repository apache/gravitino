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
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata;
import org.apache.gravitino.catalog.clickhouse.ClickHouseUtils;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
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
        ClickHouseConstants.CLICKHOUSE_ENGINE_NAME,
        ClickHouseTablePropertiesMetadata.ENGINE.LOG.getValue());
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
        ClickHouseConstants.CLICKHOUSE_ENGINE_NAME,
        ClickHouseTablePropertiesMetadata.ENGINE.MERGETREE.getValue());
    props.put(ClickHouseConstants.SETTINGS_PREFIX + "max_threads", "8");
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
}
