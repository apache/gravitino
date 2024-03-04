/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_CATALOG_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_COMMENT;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_SCHEMA_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.META_LAKE_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.initHiveCatalog;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.initHiveSchema;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.identity;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHiveTableOperations extends MiniHiveMetastoreService {

  private static final NameIdentifier tableIdentifier =
      NameIdentifier.of(META_LAKE_NAME, HIVE_CATALOG_NAME, HIVE_SCHEMA_NAME, genRandomName());
  private static HiveCatalog hiveCatalog;
  private static HiveSchema hiveSchema;
  private static HiveTable hiveTable;
  private static Column[] columns;
  private static Partition existingPartition;

  @BeforeAll
  public static void setup() {
    hiveCatalog = initHiveCatalog();
    hiveSchema = initHiveSchema(hiveCatalog);
    hiveTable = createPartitionedTable();

    // add partition: city=0/dt=2020-01-01
    String[] fieldCity = new String[] {columns[1].name()};
    Literal<?> valueCity = Literals.byteLiteral((byte) 0);
    String[] fieldDt = new String[] {columns[2].name()};
    Literal<?> valueDt = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition =
        Partitions.identity(
            new String[][] {fieldCity, fieldDt}, new Literal<?>[] {valueCity, valueDt});

    existingPartition = hiveTable.supportPartitions().addPartition(partition);
  }

  private static HiveTable createPartitionedTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col0 =
        new HiveColumn.Builder().withName("name").withType(Types.StringType.get()).build();
    HiveColumn col1 =
        new HiveColumn.Builder().withName("city").withType(Types.ByteType.get()).build();
    HiveColumn col2 =
        new HiveColumn.Builder().withName("dt").withType(Types.DateType.get()).build();

    columns = new Column[] {col0, col1, col2};

    Transform[] partitioning = new Transform[] {identity(col1.name()), identity(col2.name())};

    return (HiveTable)
        hiveCatalog
            .asTableCatalog()
            .createTable(tableIdentifier, columns, HIVE_COMMENT, properties, partitioning);
  }

  @Test
  public void testListPartitionNames() {
    String[] partitionNames = hiveTable.supportPartitions().listPartitionNames();
    // there maybe other partitions in the list, so we only check the added partition
    Assertions.assertTrue(
        partitionNames.length > 0
            && Arrays.asList(partitionNames).contains(existingPartition.name()));
  }

  @Test
  public void testListPartitions() {
    Partition[] partitions = hiveTable.supportPartitions().listPartitions();
    // there maybe other partitions in the list, so we only check the added partition
    Assertions.assertTrue(
        partitions.length > 0 && Arrays.asList(partitions).contains(existingPartition));
  }

  @Test
  public void testGetPartition() {
    SupportsPartitions partitions = hiveTable.supportPartitions();
    Partition partition = partitions.getPartition(existingPartition.name());
    Assertions.assertEquals(existingPartition, partition);

    NoSuchPartitionException exception =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              partitions.getPartition("does_not_exist_partition");
            });
    Assertions.assertEquals(
        "Hive partition does_not_exist_partition does not exist in Hive Metastore",
        exception.getMessage());
  }

  @Test
  public void testAddPartition() {
    // add partition: city=1/dt=2020-01-01
    String[] fieldCity = new String[] {columns[1].name()};
    Literal<?> valueCity = Literals.byteLiteral((byte) 1);
    String[] fieldDt = new String[] {columns[2].name()};
    Literal<?> valueDt = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition =
        Partitions.identity(
            new String[][] {fieldCity, fieldDt}, new Literal<?>[] {valueCity, valueDt});

    SupportsPartitions partitions = hiveTable.supportPartitions();
    Partition addedPartition = partitions.addPartition(partition);
    Partition gotPartition = partitions.getPartition(addedPartition.name());
    Assertions.assertEquals(addedPartition, gotPartition);

    // test exception
    String[] field1 = new String[] {columns[0].name()};
    Literal<?> value1 = Literals.byteLiteral((byte) 1);
    Partition partition1 = Partitions.identity(new String[][] {field1}, new Literal<?>[] {value1});
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> partitions.addPartition(partition1));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Hive partition field names must be the same as table partitioning field names"),
        exception.getMessage());

    String[] field2 = new String[] {columns[1].name()};
    Literal<?> value2 = Literals.byteLiteral((byte) 1);
    Partition partition2 =
        Partitions.identity(new String[][] {field1, field2}, new Literal<?>[] {value1, value2});
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> partitions.addPartition(partition2));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Hive partition field name must be in table partitioning field names"),
        exception.getMessage());
  }
}
