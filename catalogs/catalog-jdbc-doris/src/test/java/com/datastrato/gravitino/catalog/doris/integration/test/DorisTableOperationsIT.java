/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
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

  private static Type INT = Types.IntegerType.get();

  private static final String databaseName = GravitinoITUtils.genRandomName("doris_test_db");

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
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    Assertions.assertTrue(listTables.contains(tableName));
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

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
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
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
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

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
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertionsTableInfo(
                    tableName,
                    tableComment,
                    columns,
                    properties,
                    indexes,
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
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
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
    String tableName = RandomNameUtils.genRandomName("unspport_type_table");
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
