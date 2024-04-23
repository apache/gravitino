/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-it")
public class DorisTableOperationsIT extends TestDorisAbstractIT {
  private static final Type VARCHAR_255 = Types.VarCharType.of(255);
  private static final Type VARCHAR_1024 = Types.VarCharType.of(1024);

  private static final Type INT = Types.IntegerType.get();

  private static final String databaseName = GravitinoITUtils.genRandomName("doris_test_db");

  // Because the creation of Schema Change is an asynchronous process, we need wait for a while
  // For more information, you can refer to the comment in
  // DorisTableOperations.generateAlterTableSql().
  private static final long MAX_WAIT = 30;

  private static final long WAIT_INTERVAL = 1;

  public String DORIS_COL_NAME1 = "doris_col_name1";
  public String DORIS_COL_NAME2 = "doris_col_name2";
  public String DORIS_COL_NAME3 = "doris_col_name3";

  public String DORIS_COL_NAME4 = "doris_col_name4";

  private final String TABLE_COMMENT = "table_comment";

  @BeforeAll
  public static void startup() {
    TestDorisAbstractIT.startup();
    createDatabase();
  }

  private static void createDatabase() {
    DATABASE_OPERATIONS.create(databaseName, "test_comment", new HashMap<>());
  }

  private static Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    return properties;
  }

  private List<JdbcColumn> createColumns() {
    List<JdbcColumn> columns = new ArrayList<>();

    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME1)
            .withType(INT)
            .withComment("col_1_comment")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME2)
            .withType(VARCHAR_255)
            .withComment("col_2_comment")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME3)
            .withType(VARCHAR_255)
            .withComment("col_3_comment")
            .build());
    return columns;
  }

  private Distribution createDistribution() {
    return Distributions.hash(2, NamedReference.field(DORIS_COL_NAME1));
  }

  @Test
  public void testBasicTableOperation() {
    String tableName = GravitinoITUtils.genRandomName("doris_basic_test");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
    Index[] indexes = new Index[] {};

    // create table
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        distribution,
        indexes);
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    Assertions.assertTrue(listTables.contains(tableName));
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    // rename table
    String newName = GravitinoITUtils.genRandomName("new_table");
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.rename(databaseName, tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(databaseName, newName));

    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.drop(databaseName, newName));

    listTables = TABLE_OPERATIONS.listTables(databaseName);
    Assertions.assertFalse(listTables.contains(newName));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.drop(databaseName, newName));
  }

  @Test
  void testAlterColumnType() {
    String tableName = GravitinoITUtils.genRandomName("doris_alter_column_type");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnType(new String[] {DORIS_COL_NAME3}, VARCHAR_1024));

    // After modifying the type, check it
    JdbcColumn col_3 = columns.remove(2);
    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME3)
            .withType(VARCHAR_1024)
            .withComment(col_3.comment())
            .build());

    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testAlterColumnComment() {
    String tableName = GravitinoITUtils.genRandomName("doris_alter_column_comment");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    String colNewComment = "new_col_comment";

    Column col_3 = columns.remove(2);
    Column col_2 = columns.remove(1);

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnComment(new String[] {col_2.name()}, colNewComment),
        TableChange.updateColumnComment(new String[] {col_3.name()}, colNewComment));

    columns.add(
        JdbcColumn.builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(colNewComment)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName(col_3.name())
            .withType(col_3.dataType())
            .withComment(colNewComment)
            .build());

    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testAddColumn() {
    String tableName = GravitinoITUtils.genRandomName("doris_add_column");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.addColumn(new String[] {"col_4"}, VARCHAR_255, "txt4", true));

    JdbcColumn col_4 =
        JdbcColumn.builder().withName("col_4").withType(VARCHAR_255).withComment("txt4").build();
    columns.add(col_4);
    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testChangeColumnPosition() {
    String tableName = GravitinoITUtils.genRandomName("doris_change_column_position");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME4)
            .withType(VARCHAR_255)
            .withComment("txt4")
            .build());
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {DORIS_COL_NAME3}, TableChange.ColumnPosition.after(DORIS_COL_NAME4)));

    JdbcColumn col_4 = columns.remove(3);
    JdbcColumn col_3 = columns.remove(2);

    columns.add(col_4);
    columns.add(col_3);

    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testDropColumn() {
    String tableName = GravitinoITUtils.genRandomName("doris_drop_column");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    columns.add(
        JdbcColumn.builder()
            .withName(DORIS_COL_NAME4)
            .withType(VARCHAR_255)
            .withComment("txt4")
            .build());
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    TABLE_OPERATIONS.alterTable(
        databaseName, tableName, TableChange.deleteColumn(new String[] {DORIS_COL_NAME4}, true));
    columns.remove(3);
    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // delete column that does not exist
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    databaseName,
                    tableName,
                    TableChange.deleteColumn(new String[] {DORIS_COL_NAME4}, false)));

    Assertions.assertEquals(
        "Delete column does not exist: " + DORIS_COL_NAME4, illegalArgumentException.getMessage());
    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                databaseName,
                tableName,
                TableChange.deleteColumn(new String[] {DORIS_COL_NAME4}, true)));
  }

  @Test
  public void testAlterIndex() {
    String tableName = GravitinoITUtils.genRandomName("doris_alter_index");
    String tableComment = TABLE_COMMENT;
    List<JdbcColumn> columns = createColumns();
    Map<String, String> properties = createProperties();
    Distribution distribution = createDistribution();
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    // test add index
    TABLE_OPERATIONS.alterTable(
        databaseName,
        tableName,
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY,
            "k2_index",
            new String[][] {{DORIS_COL_NAME2}, {DORIS_COL_NAME3}}));

    Index[] newIndexes =
        new Index[] {
          Indexes.primary("k2_index", new String[][] {{DORIS_COL_NAME2}, {DORIS_COL_NAME3}})
        };
    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    newIndexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));

    // test delete index
    TABLE_OPERATIONS.alterTable(databaseName, tableName, TableChange.deleteIndex("k2_index", true));

    Awaitility.await()
        .atMost(MAX_WAIT, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
                    TABLE_OPERATIONS.load(databaseName, tableName)));
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("all_type_table");
    String tableComment = TABLE_COMMENT;
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

    Distribution distribution = Distributions.hash(32, NamedReference.field("col_1"));
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
    assertionsTableInfo(tableName, tableComment, columns, Collections.emptyMap(), null, load);
  }

  @Test
  public void testCreateNotSupportTypeTable() {
    String tableName = RandomNameUtils.genRandomName("unsupported_type_table");
    String tableComment = TABLE_COMMENT;
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
                    Distributions.hash(32, NamedReference.field("col_1")),
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
}
