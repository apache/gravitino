/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.google.common.collect.Maps;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPartitionOperationDispatcher extends TestOperationDispatcher {
  static SchemaOperationDispatcher schemaOperationDispatcher;
  static TableOperationDispatcher tableOperationDispatcher;
  static PartitionOperationDispatcher partitionOperationDispatcher;

  private static final String SCHEMA = "test_partition_schema";
  private static final String TABLE = "test_partition_table";
  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.ofTable(metalake, catalog, SCHEMA, TABLE);
  private static final Partition PARTITION =
      Partitions.identity(
          "p1",
          new String[][] {{"col1"}},
          new Literal[] {Literals.stringLiteral("v1")},
          Maps.newHashMap());

  @BeforeAll
  public static void initialize() {
    prepareTable();
    partitionOperationDispatcher.addPartition(TABLE_IDENT, PARTITION);

    // Assert that the custom class loader is used
    ClassLoader classLoader =
        partitionOperationDispatcher.doWithTable(
            TABLE_IDENT,
            s -> Thread.currentThread().getContextClassLoader(),
            RuntimeException.class);
    Assertions.assertInstanceOf(
        IsolatedClassLoader.CUSTOM_CLASS_LOADER_CLASS,
        classLoader,
        "Custom class loader is not used");
  }

  protected static void prepareTable() {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
    partitionOperationDispatcher =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator);

    NameIdentifier schemaIdent = NameIdentifier.ofSchema(metalake, catalog, SCHEMA);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);
    Column[] columns =
        new Column[] {
          Column.of("col1", Types.StringType.get()), Column.of("col2", Types.StringType.get())
        };
    tableOperationDispatcher.createTable(TABLE_IDENT, columns, "comment", null);
  }

  @Test
  public void testListPartitionNames() {
    String[] partitionNames = partitionOperationDispatcher.listPartitionNames(TABLE_IDENT);
    Assertions.assertTrue(Arrays.asList(partitionNames).contains(PARTITION.name()));
  }

  @Test
  public void testListPartitions() {
    Partition[] partitions = partitionOperationDispatcher.listPartitions(TABLE_IDENT);
    Assertions.assertTrue(Arrays.asList(partitions).contains(PARTITION));
  }

  @Test
  public void testGetPartition() {
    Partition p = partitionOperationDispatcher.getPartition(TABLE_IDENT, PARTITION.name());
    Assertions.assertEquals(PARTITION, p);
  }

  @Test
  public void testPartitionExists() {
    Assertions.assertTrue(
        partitionOperationDispatcher.partitionExists(TABLE_IDENT, PARTITION.name()));
  }

  @Test
  public void testAddPartition() {
    Partition newPartition =
        Partitions.identity(
            "p2",
            new String[][] {{"col1"}},
            new Literal[] {Literals.stringLiteral("v2")},
            Maps.newHashMap());
    partitionOperationDispatcher.addPartition(TABLE_IDENT, newPartition);
    Assertions.assertTrue(
        partitionOperationDispatcher.partitionExists(TABLE_IDENT, newPartition.name()));
  }

  @Test
  public void testDropPartition() {
    Partition testDrop =
        Partitions.identity(
            "p3",
            new String[][] {{"col1"}},
            new Literal[] {Literals.stringLiteral("v2")},
            Maps.newHashMap());
    partitionOperationDispatcher.addPartition(TABLE_IDENT, testDrop);
    Assertions.assertTrue(
        partitionOperationDispatcher.partitionExists(TABLE_IDENT, testDrop.name()));

    Assertions.assertTrue(
        partitionOperationDispatcher.partitionExists(TABLE_IDENT, testDrop.name()));

    boolean dropped = partitionOperationDispatcher.dropPartition(TABLE_IDENT, testDrop.name());
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(
        partitionOperationDispatcher.partitionExists(TABLE_IDENT, testDrop.name()));
  }

  @Test
  public void testPurgePartition() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> partitionOperationDispatcher.purgePartition(TABLE_IDENT, PARTITION.name()));
  }
}
