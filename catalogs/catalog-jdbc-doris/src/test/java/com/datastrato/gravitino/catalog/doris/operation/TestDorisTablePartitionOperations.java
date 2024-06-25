/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NotPartitionedTableException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.partitions.RangePartition;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestDorisTablePartitionOperations extends TestDoris {
  private static final String databaseName = GravitinoITUtils.genRandomName("doris_test_db");
  private static final Integer DEFAULT_BUCKET_SIZE = 1;
  private static final JdbcTypeConverter TYPE_CONVERTER = new DorisTypeConverter();

  @BeforeAll
  public static void startup() {
    TestDoris.startup();
    createDatabase();
  }

  private static void createDatabase() {
    DATABASE_OPERATIONS.create(databaseName, "test_comment", new HashMap<>());
  }

  private static Map<String, String> createProperties() {
    return ImmutableMap.of("replication_allocation", "tag.location.default: 1");
  }

  @Test
  public void testRangePartition() {
    String tableComment = "range_partitioned_table_comment";
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
    String rangePartitionTableName = GravitinoITUtils.genRandomName("range_partition_table");
    Transform[] rangePartition = new Transform[] {Transforms.range(new String[] {col4.name()})};
    TABLE_OPERATIONS.create(
        databaseName,
        rangePartitionTableName,
        columns.toArray(new JdbcColumn[] {}),
        tableComment,
        createProperties(),
        rangePartition,
        distribution,
        indexes);

    // Assert table info
    JdbcTable rangePartitionTable = TABLE_OPERATIONS.load(databaseName, rangePartitionTableName);
    assertionsTableInfo(
        rangePartitionTableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        rangePartitionTable);
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    assertTrue(listTables.contains(rangePartitionTableName));

    // Create Table Partition Operations manually
    JdbcTablePartitionOperations tablePartitionOperations =
        new DorisTablePartitionOperations(
            DATA_SOURCE,
            databaseName,
            rangePartitionTableName,
            JDBC_EXCEPTION_CONVERTER,
            TYPE_CONVERTER);

    // Assert partition info when there is no partitions actually
    String[] emptyPartitionNames = tablePartitionOperations.listPartitionNames();
    assertEquals(0, emptyPartitionNames.length);
    Partition[] emptyPartitions = tablePartitionOperations.listPartitions();
    assertEquals(0, emptyPartitions.length);

    // Get non-existing partition
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p1"));

    // Add partition with incorrect type
    Partition incorrect =
        Partitions.list("test_incorrect", new Literal[][] {{Literals.NULL}}, null);
    assertThrows(
        IllegalArgumentException.class, () -> tablePartitionOperations.addPartition(incorrect));

    // Add different kinds of range partitions
    LocalDate today = LocalDate.now();
    LocalDate tomorrow = today.plusDays(1);
    Literal<LocalDate> todayLiteral = Literals.dateLiteral(today);
    Literal<LocalDate> tomorrowLiteral = Literals.dateLiteral(tomorrow);
    Partition p1 = Partitions.range("p1", todayLiteral, Literals.NULL, Collections.emptyMap());
    Partition p2 = Partitions.range("p2", tomorrowLiteral, todayLiteral, Collections.emptyMap());
    Partition p3 = Partitions.range("p3", Literals.NULL, tomorrowLiteral, Collections.emptyMap());
    assertEquals(p1, tablePartitionOperations.addPartition(p1));
    assertEquals(p2, tablePartitionOperations.addPartition(p2));
    assertEquals(p3, tablePartitionOperations.addPartition(p3));

    // Add partition with same name
    Partition p4 = Partitions.range("p3", Literals.NULL, Literals.NULL, Collections.emptyMap());
    assertThrows(
        PartitionAlreadyExistsException.class, () -> tablePartitionOperations.addPartition(p4));

    // Check partitions
    Set<String> partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(3, partitionNames.size());
    assertTrue(partitionNames.contains("p1"));
    assertTrue(partitionNames.contains("p2"));
    assertTrue(partitionNames.contains("p3"));

    Map<String, RangePartition> partitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(p -> p.name(), p -> (RangePartition) p));
    assertEquals(3, partitions.size());
    RangePartition actualP1 = partitions.get("p1");
    assertEquals(todayLiteral, actualP1.upper());
    assertEquals(Literals.of("0000-01-01", Types.DateType.get()), actualP1.lower());
    RangePartition actualP2 = partitions.get("p2");
    assertEquals(tomorrowLiteral, actualP2.upper());
    assertEquals(todayLiteral, actualP2.lower());
    RangePartition actualP3 = partitions.get("p3");
    assertEquals(Literals.of("MAXVALUE", Types.DateType.get()), actualP3.upper());
    assertEquals(tomorrowLiteral, actualP3.lower());

    actualP1 = (RangePartition) tablePartitionOperations.getPartition("p1");
    assertEquals(todayLiteral, actualP1.upper());
    assertEquals(Literals.of("0000-01-01", Types.DateType.get()), actualP1.lower());
    actualP2 = (RangePartition) tablePartitionOperations.getPartition("p2");
    assertEquals(tomorrowLiteral, actualP2.upper());
    assertEquals(todayLiteral, actualP2.lower());
    actualP3 = (RangePartition) tablePartitionOperations.getPartition("p3");
    assertEquals(Literals.of("MAXVALUE", Types.DateType.get()), actualP3.upper());
    assertEquals(tomorrowLiteral, actualP3.lower());

    // Drop partition
    assertTrue(tablePartitionOperations.dropPartition("p3"));
    partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(2, partitionNames.size());
    assertFalse(partitionNames.contains("p3"));
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p3"));

    // Drop non-existing partition
    assertFalse(tablePartitionOperations.dropPartition("p3"));
  }

  @Test
  public void testListPartition() {
    String tableComment = "list_partitioned_table_comment";
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
    String listPartitionTableName = GravitinoITUtils.genRandomName("list_partition_table");
    Transform[] listPartition =
        new Transform[] {Transforms.list(new String[][] {{col1.name()}, {col4.name()}})};
    TABLE_OPERATIONS.create(
        databaseName,
        listPartitionTableName,
        columns.toArray(new JdbcColumn[] {}),
        tableComment,
        createProperties(),
        listPartition,
        distribution,
        indexes);

    // Assert table info
    JdbcTable listPartitionTable = TABLE_OPERATIONS.load(databaseName, listPartitionTableName);
    assertionsTableInfo(
        listPartitionTableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        listPartitionTable);
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    assertTrue(listTables.contains(listPartitionTableName));

    // Create Table Partition Operations manually
    JdbcTablePartitionOperations tablePartitionOperations =
        new DorisTablePartitionOperations(
            DATA_SOURCE,
            databaseName,
            listPartitionTableName,
            JDBC_EXCEPTION_CONVERTER,
            TYPE_CONVERTER);

    // Assert partition info when there is no partitions actually
    String[] emptyPartitionNames = tablePartitionOperations.listPartitionNames();
    assertEquals(0, emptyPartitionNames.length);
    Partition[] emptyPartitions = tablePartitionOperations.listPartitions();
    assertEquals(0, emptyPartitions.length);

    // Get non-existing partition
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p1"));

    // Add partition with incorrect type
    Partition incorrectType =
        Partitions.range("p1", Literals.NULL, Literals.NULL, Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class, () -> tablePartitionOperations.addPartition(incorrectType));

    // Add partition with incorrect value
    Partition incorrectValue =
        Partitions.list("p1", new Literal[][] {{Literals.NULL}}, Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class,
        () -> tablePartitionOperations.addPartition(incorrectValue));

    // Add different kinds of list partitions
    LocalDate today = LocalDate.now();
    LocalDate tomorrow = today.plusDays(1);
    Literal<LocalDate> todayLiteral = Literals.dateLiteral(today);
    Literal<LocalDate> tomorrowLiteral = Literals.dateLiteral(tomorrow);
    Literal[][] p1Values = {{Literals.integerLiteral(1), todayLiteral}};
    Literal[][] p2Values = {{Literals.integerLiteral(2), todayLiteral}};
    Literal[][] p3Values = {{Literals.integerLiteral(1), tomorrowLiteral}};
    Literal[][] p4Values = {{Literals.integerLiteral(2), tomorrowLiteral}};
    Partition p1 = Partitions.list("p1", p1Values, Collections.emptyMap());
    Partition p2 = Partitions.list("p2", p2Values, Collections.emptyMap());
    Partition p3 = Partitions.list("p3", p3Values, Collections.emptyMap());
    Partition p4 = Partitions.list("p4", p4Values, Collections.emptyMap());
    assertEquals(p1, tablePartitionOperations.addPartition(p1));
    assertEquals(p2, tablePartitionOperations.addPartition(p2));
    assertEquals(p3, tablePartitionOperations.addPartition(p3));
    assertEquals(p4, tablePartitionOperations.addPartition(p4));

    // Check partitions
    Set<String> partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(4, partitionNames.size());
    assertTrue(partitionNames.contains("p1"));
    assertTrue(partitionNames.contains("p2"));
    assertTrue(partitionNames.contains("p3"));
    assertTrue(partitionNames.contains("p4"));

    Map<String, ListPartition> partitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(p -> p.name(), p -> (ListPartition) p));
    assertEquals(4, partitions.size());
    ListPartition actualP1 = partitions.get("p1");
    assertTrue(Arrays.deepEquals(actualP1.lists(), p1Values));
    ListPartition actualP2 = partitions.get("p2");
    assertTrue(Arrays.deepEquals(actualP2.lists(), p2Values));
    ListPartition actualP3 = partitions.get("p3");
    assertTrue(Arrays.deepEquals(actualP3.lists(), p3Values));
    ListPartition actualP4 = partitions.get("p4");
    assertTrue(Arrays.deepEquals(actualP4.lists(), p4Values));

    actualP1 = (ListPartition) tablePartitionOperations.getPartition("p1");
    assertTrue(Arrays.deepEquals(actualP1.lists(), p1Values));
    actualP2 = (ListPartition) tablePartitionOperations.getPartition("p2");
    assertTrue(Arrays.deepEquals(actualP2.lists(), p2Values));
    actualP3 = (ListPartition) tablePartitionOperations.getPartition("p3");
    assertTrue(Arrays.deepEquals(actualP3.lists(), p3Values));
    actualP4 = (ListPartition) tablePartitionOperations.getPartition("p4");
    assertTrue(Arrays.deepEquals(actualP4.lists(), p4Values));

    // Drop partition
    assertTrue(tablePartitionOperations.dropPartition("p3"));
    partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(3, partitionNames.size());
    assertFalse(partitionNames.contains("p3"));
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p3"));

    // Drop non-existing partition
    assertFalse(tablePartitionOperations.dropPartition("p3"));
  }

  @Test
  public void testNotPartitionedTable() {
    String tableComment = "not_partitioned_table_comment";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Distribution distribution =
        Distributions.hash(DEFAULT_BUCKET_SIZE, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};
    JdbcColumn[] columns = {col1};
    String tableName = GravitinoITUtils.genRandomName("not_partitioned_table");
    Transform[] partition = Transforms.EMPTY_TRANSFORM;
    TABLE_OPERATIONS.create(
        databaseName,
        tableName,
        columns,
        tableComment,
        createProperties(),
        partition,
        distribution,
        indexes);

    // Assert table info
    JdbcTable load = TABLE_OPERATIONS.load(databaseName, tableName);
    assertionsTableInfo(
        tableName,
        tableComment,
        Arrays.stream(columns).collect(Collectors.toList()),
        Collections.emptyMap(),
        null,
        load);
    List<String> listTables = TABLE_OPERATIONS.listTables(databaseName);
    assertTrue(listTables.contains(tableName));

    // Create Table Partition Operations manually
    JdbcTablePartitionOperations tablePartitionOperations =
        new DorisTablePartitionOperations(
            DATA_SOURCE, databaseName, tableName, JDBC_EXCEPTION_CONVERTER, TYPE_CONVERTER);

    // Operations for not-partitioned table
    assertThrows(
        NotPartitionedTableException.class, () -> tablePartitionOperations.listPartitionNames());
    assertThrows(
        NotPartitionedTableException.class, () -> tablePartitionOperations.listPartitions());
    assertThrows(
        NotPartitionedTableException.class, () -> tablePartitionOperations.getPartition("p1"));
    assertThrows(
        NotPartitionedTableException.class, () -> tablePartitionOperations.addPartition(null));
    assertThrows(
        NotPartitionedTableException.class, () -> tablePartitionOperations.dropPartition("p1"));
  }
}
