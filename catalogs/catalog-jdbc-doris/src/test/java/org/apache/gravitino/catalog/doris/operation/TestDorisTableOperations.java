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
package org.apache.gravitino.catalog.doris.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-test")
public class TestDorisTableOperations extends TestDoris {
  private static final JdbcTypeConverter TYPE_CONVERTER = new DorisTypeConverter();
  private static final Type VARCHAR_255 = Types.VarCharType.of(255);
  private static final Type VARCHAR_1024 = Types.VarCharType.of(1024);

  private static final Type INT = Types.IntegerType.get();

  private static final Integer DEFAULT_BUCKET_SIZE = 1;

  private static final String databaseName = GravitinoITUtils.genRandomName("doris_test_db");

  // Because the creation of Schema Change is an asynchronous process, we need to wait for a while
  // For more information, you can refer to the comment in
  // DorisTableOperations.generateAlterTableSql().
  private static final long MAX_WAIT_IN_SECONDS = 30;

  private static final long WAIT_INTERVAL_IN_SECONDS = 1;

  @BeforeAll
  public static void startup() {
    TestDoris.startup();
    createDatabase();
  }

  private static void createDatabase() {
    DATABASE_OPERATIONS.create(databaseName, "test_comment", new HashMap<>());
  }

  private static Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    return properties;
  }

  @Test
  void testAllDistribution() {
    Distribution[] distributions =
        new Distribution[] {
          Distributions.even(DEFAULT_BUCKET_SIZE, Expression.EMPTY_EXPRESSION),
          Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1")),
          Distributions.even(10, Expression.EMPTY_EXPRESSION),
          Distributions.hash(0, NamedReference.field("col_1")),
          Distributions.hash(11, NamedReference.field("col_1")),
          Distributions.hash(12, NamedReference.field("col_1"), NamedReference.field("col_2"))
        };

    for (Distribution distribution : distributions) {
      String tableName = GravitinoITUtils.genRandomName("doris_basic_test_table");
      String tableComment = "test_comment";
      List<JdbcColumn> columns = new ArrayList<>();
      JdbcColumn col_1 =
          JdbcColumn.builder().withName("col_1").withType(INT).withComment("id").build();
      columns.add(col_1);
      JdbcColumn col_2 =
          JdbcColumn.builder().withName("col_2").withType(VARCHAR_255).withComment("col_2").build();
      columns.add(col_2);
      JdbcColumn col_3 =
          JdbcColumn.builder().withName("col_3").withType(VARCHAR_255).withComment("col_3").build();
      columns.add(col_3);
      Map<String, String> properties = new HashMap<>();
      Index[] indexes = new Index[] {};

      // create table
      TABLE_OPERATIONS.create(
          databaseName,
          tableName,
          columns.toArray(new JdbcColumn[0]),
          tableComment,
          createProperties(),
          null,
          distribution,
          indexes);
      JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
      assertionsTableInfo(
          tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

      Assertions.assertEquals(distribution.strategy(), load.distribution().strategy());
      Assertions.assertArrayEquals(distribution.expressions(), load.distribution().expressions());
      TABLE_OPERATIONS.drop(databaseName, tableName);
    }
  }

  @Test
  public void testBasicTableOperation() {
    String tableName = GravitinoITUtils.genRandomName("doris_basic_test_table");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        JdbcColumn.builder().withName("col_1").withType(INT).withComment("id").build();
    columns.add(col_1);
    JdbcColumn col_2 =
        JdbcColumn.builder().withName("col_2").withType(VARCHAR_255).withComment("col_2").build();
    columns.add(col_2);
    JdbcColumn col_3 =
        JdbcColumn.builder().withName("col_3").withType(VARCHAR_255).withComment("col_3").build();
    columns.add(col_3);
    Map<String, String> properties = new HashMap<>();

    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};

    // create table
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    assertTrue(listTables.contains(tableName));
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    // rename table
    String newName = GravitinoITUtils.genRandomName("new_table");
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.rename(databaseName, tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(databaseName, newName));

    Assertions.assertTrue(TABLE_OPERATIONS.drop(databaseName, newName), "table should be dropped");

    listTables = TABLE_OPERATIONS.listTables(databaseName);
    Assertions.assertFalse(listTables.contains(newName));

    Assertions.assertFalse(
        TABLE_OPERATIONS.drop(databaseName, newName), "table should be non-existent");
  }

  @Test
  public void testAlterTable() {
    String tableName = GravitinoITUtils.genRandomName("doris_alter_test_table");

    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        JdbcColumn.builder().withName("col_1").withType(INT).withComment("id").build();
    columns.add(col_1);
    JdbcColumn col_2 =
        JdbcColumn.builder().withName("col_2").withType(VARCHAR_255).withComment("col_2").build();
    columns.add(col_2);
    JdbcColumn col_3 =
        JdbcColumn.builder().withName("col_3").withType(VARCHAR_255).withComment("col_3").build();
    columns.add(col_3);
    Map<String, String> properties = new HashMap<>();

    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};

    // create table
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnType(new String[] {col_3.name()}, VARCHAR_1024));

    // After modifying the type, check it
    columns.clear();
    col_3 =
        JdbcColumn.builder()
            .withName(col_3.name())
            .withType(VARCHAR_1024)
            .withComment(col_3.comment())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    String colNewComment = "new_comment";
    // update column comment

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnComment(new String[] {col_2.name()}, colNewComment));
    load = TABLE_OPERATIONS.load(databaseName, tableName);

    columns.clear();
    col_2 =
        JdbcColumn.builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(colNewComment)
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    // add new column
    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.addColumn(new String[] {"col_4"}, VARCHAR_255, "txt4", true));

    columns.clear();
    JdbcColumn col_4 =
        JdbcColumn.builder().withName("col_4").withType(VARCHAR_255).withComment("txt4").build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    columns.add(col_4);
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // change column position
    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {"col_3"}, TableChange.ColumnPosition.after("col_4")));

    columns.clear();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_4);
    columns.add(col_3);
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // drop column if exist
    TABLE_OPERATIONS.alterTable(
        databaseName, tableName, TableChange.deleteColumn(new String[] {"col_4"}, true));
    columns.clear();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // delete column that does not exist
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    databaseName,
                    tableName,
                    TableChange.deleteColumn(new String[] {"col_4"}, false)));

    Assertions.assertEquals(
        "Delete column does not exist: col_4", illegalArgumentException.getMessage());
    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                databaseName, tableName, TableChange.deleteColumn(new String[] {"col_4"}, true)));

    // test add index
    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k2_index", new String[][] {{"col_2"}, {"col_3"}}));

    Index[] newIndexes =
        new Index[] {Indexes.primary("k2_index", new String[][] {{"col_2"}, {"col_3"}})};
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    newIndexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // test delete index
    TABLE_OPERATIONS.alterTable(databaseName, tableName, TableChange.deleteIndex("k2_index", true));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    Transforms.EMPTY_TRANSFORM,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("all_type_table");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(JdbcColumn.builder().withName("col_1").withType(Types.IntegerType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_2").withType(Types.BooleanType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(Types.ByteType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_4").withType(Types.ShortType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_5").withType(Types.IntegerType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_6").withType(Types.LongType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_7").withType(Types.FloatType.get()).build());
    columns.add(JdbcColumn.builder().withName("col_8").withType(Types.DoubleType.get()).build());
    columns.add(
        JdbcColumn.builder().withName("col_9").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(JdbcColumn.builder().withName("col_10").withType(Types.DateType.get()).build());
    columns.add(
        JdbcColumn.builder().withName("col_11").withType(Types.FixedCharType.of(10)).build());
    columns.add(JdbcColumn.builder().withName("col_12").withType(Types.VarCharType.of(10)).build());
    columns.add(JdbcColumn.builder().withName("col_13").withType(Types.StringType.get()).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_14")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .build());

    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};
    // create table
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);

    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
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
    String tableName = RandomNameUtils.genRandomName("unsupported_type_table");
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
      columns.add(JdbcColumn.builder().withName("col_1").withType(Types.IntegerType.get()).build());
      columns.add(
          JdbcColumn.builder().withName("col_2").withType(type).withNullable(false).build());

      JdbcColumn[] jdbcCols = columns.toArray(new JdbcColumn[0]);
      IllegalArgumentException illegalArgumentException =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> {
                TABLE_OPERATIONS.create(
                    databaseName,
                    tableName,
                    jdbcCols,
                    tableComment,
                    createProperties(),
                    null,
                    Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1")),
                    Indexes.EMPTY_INDEXES);
              });
      Assertions.assertTrue(
          illegalArgumentException
              .getMessage()
              .contains(
                  String.format(
                      "Couldn't convert Gravitino type %s to Doris type", type.simpleString())));
    }
  }

  @Test
  public void testCreatePartitionedTable() {
    String tableComment = "partition_table_comment";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder().withName("col_2").withType(Types.BooleanType.get()).build();
    JdbcColumn col3 =
        JdbcColumn.builder().withName("col_3").withType(Types.DoubleType.get()).build();
    JdbcColumn col4 =
        JdbcColumn.builder()
            .withName("col_4")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build();
    List<JdbcColumn> columns = Arrays.asList(col1, col2, col3, col4);
    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};

    // create table with range partition
    String rangePartitionTableName = GravitinoITUtils.genRandomName("range_partition_table");
    LocalDate today = LocalDate.now();
    LocalDate tomorrow = today.plusDays(1);
    Literal<LocalDate> todayLiteral = Literals.dateLiteral(today);
    Literal<LocalDate> tomorrowLiteral = Literals.dateLiteral(tomorrow);
    RangePartition rangePartition1 = Partitions.range("p1", todayLiteral, Literals.NULL, null);
    RangePartition rangePartition2 = Partitions.range("p2", tomorrowLiteral, todayLiteral, null);
    RangePartition rangePartition3 = Partitions.range("p3", Literals.NULL, tomorrowLiteral, null);
    Transform[] rangePartition =
        new Transform[] {
          Transforms.range(
              new String[] {col4.name()},
              new RangePartition[] {rangePartition1, rangePartition2, rangePartition3})
        };
    TABLE_OPERATIONS.create(
        databaseName,
        rangePartitionTableName,
        columns.toArray(new JdbcColumn[] {}),
        tableComment,
        createProperties(),
        rangePartition,
        distribution,
        indexes);
    JdbcTable rangePartitionTable = TABLE_OPERATIONS.load(databaseName, rangePartitionTableName);
    assertionsTableInfo(
        rangePartitionTableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        new Transform[] {Transforms.range(new String[] {col4.name()})},
        rangePartitionTable);

    // assert partition info
    JdbcTablePartitionOperations tablePartitionOperations =
        new DorisTablePartitionOperations(
            DATA_SOURCE, rangePartitionTable, JDBC_EXCEPTION_CONVERTER, TYPE_CONVERTER);
    Map<String, RangePartition> loadedRangePartitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (RangePartition) p));
    assertTrue(loadedRangePartitions.containsKey("p1"));
    RangePartition actualP1 = loadedRangePartitions.get("p1");
    assertEquals(todayLiteral, actualP1.upper());
    assertEquals(Literals.of("0000-01-01", Types.DateType.get()), actualP1.lower());
    assertTrue(loadedRangePartitions.containsKey("p2"));
    RangePartition actualP2 = loadedRangePartitions.get("p2");
    assertEquals(tomorrowLiteral, actualP2.upper());
    assertEquals(todayLiteral, actualP2.lower());
    assertTrue(loadedRangePartitions.containsKey("p3"));
    RangePartition actualP3 = loadedRangePartitions.get("p3");
    assertEquals(Literals.of("MAXVALUE", Types.DateType.get()), actualP3.upper());
    assertEquals(tomorrowLiteral, actualP3.lower());

    // create table with list partition
    String listPartitionTableName = GravitinoITUtils.genRandomName("list_partition_table");
    Literal<Integer> integerLiteral1 = Literals.integerLiteral(1);
    Literal<Integer> integerLiteral2 = Literals.integerLiteral(2);
    ListPartition listPartition1 =
        Partitions.list(
            "p1",
            new Literal[][] {{integerLiteral1, todayLiteral}, {integerLiteral1, tomorrowLiteral}},
            null);
    ListPartition listPartition2 =
        Partitions.list(
            "p2",
            new Literal[][] {{integerLiteral2, todayLiteral}, {integerLiteral2, tomorrowLiteral}},
            null);
    Transform[] listPartition =
        new Transform[] {
          Transforms.list(
              new String[][] {{col1.name()}, {col4.name()}},
              new ListPartition[] {listPartition1, listPartition2})
        };
    TABLE_OPERATIONS.create(
        databaseName,
        listPartitionTableName,
        columns.toArray(new JdbcColumn[] {}),
        tableComment,
        createProperties(),
        listPartition,
        distribution,
        indexes);
    JdbcTable listPartitionTable = TABLE_OPERATIONS.load(databaseName, listPartitionTableName);
    assertionsTableInfo(
        listPartitionTableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        new Transform[] {Transforms.list(new String[][] {{col1.name()}, {col4.name()}})},
        listPartitionTable);

    // assert partition info
    tablePartitionOperations =
        new DorisTablePartitionOperations(
            DATA_SOURCE, listPartitionTable, JDBC_EXCEPTION_CONVERTER, TYPE_CONVERTER);
    Map<String, ListPartition> loadedListPartitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (ListPartition) p, (p1, p2) -> p2));
    assertTrue(loadedListPartitions.containsKey("p1"));
    assertTrue(Arrays.deepEquals(listPartition1.lists(), loadedListPartitions.get("p1").lists()));
    assertTrue(loadedListPartitions.containsKey("p2"));
    assertTrue(Arrays.deepEquals(listPartition2.lists(), loadedListPartitions.get("p2").lists()));
  }

  @Test
  public void testOperationIndexDefinition() {
    String tableName = GravitinoITUtils.genRandomName("doris_basic_test_table");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        JdbcColumn.builder().withName("col_1").withType(INT).withComment("id").build();
    columns.add(col_1);
    JdbcColumn col_2 =
        JdbcColumn.builder().withName("col_2").withType(VARCHAR_255).withComment("col_2").build();
    columns.add(col_2);

    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {Indexes.unique("uk_2", new String[][] {{"col_1"}})};

    // create table
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);

    // If ifExists is set to true then the code should not throw an exception if the index doesn't
    // exist.
    TableChange.DeleteIndex deleteIndex = new TableChange.DeleteIndex("uk_1", true);
    String sql = DorisTableOperations.deleteIndexDefinition(null, deleteIndex);
    Assertions.assertEquals("DROP INDEX uk_1", sql);

    // The index existence check should only verify existence when ifExists is false, preventing
    // failures when dropping non-existent indexes.
    TableChange.DeleteIndex deleteIndex2 = new TableChange.DeleteIndex("uk_1", false);
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> DorisTableOperations.deleteIndexDefinition(load, deleteIndex2));
    Assertions.assertEquals("Index does not exist", thrown.getMessage());

    TableChange.DeleteIndex deleteIndex3 = new TableChange.DeleteIndex("uk_2", false);
    sql = DorisTableOperations.deleteIndexDefinition(load, deleteIndex3);
    Assertions.assertEquals("DROP INDEX uk_2", sql);
  }

  @Test
  public void testCalculateDatetimePrecision() {
    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("DATE", 10, 0),
        "DATE type should return 0 precision");

    Assertions.assertEquals(
        0,
        TABLE_OPERATIONS.calculateDatetimePrecision("DATETIME", 20, 0),
        "TIME type should return 0 precision");

    Assertions.assertEquals(
        3,
        TABLE_OPERATIONS.calculateDatetimePrecision("DATETIME", 23, 0),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        6,
        TABLE_OPERATIONS.calculateDatetimePrecision("DATETIME", 26, 0),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        9,
        TABLE_OPERATIONS.calculateDatetimePrecision("DATETIME", 29, 0),
        "Lower case type name should work");

    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("VARCHAR", 50, 0),
        "Non-datetime type should return 0 precision");
  }

  @Test
  public void testCalculateDatetimePrecisionWithUnsupportedDriverVersion() {
    DorisTableOperations operationsWithOldDriver =
        new DorisTableOperations() {
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
        operationsWithOldDriver.calculateDatetimePrecision("DATETIME", 26, 0),
        "DATETIME type should return null for unsupported driver version");
  }
}
