/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.NameIdentifierUtil;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPartitionNormalizeDispatcher extends TestOperationDispatcher {

  private static PartitionNormalizeDispatcher partitionNormalizeDispatcher;
  private static final String SCHEMA = "test_partition_normalize_schema";
  private static final NameIdentifier TABLE =
      NameIdentifierUtil.ofTable(metalake, catalog, SCHEMA, "TEST_PARTITION_NORMALIZE_TABLE");

  @BeforeAll
  public static void initialize() {
    TestPartitionOperationDispatcher.prepareTable();
    partitionNormalizeDispatcher =
        new PartitionNormalizeDispatcher(
            TestPartitionOperationDispatcher.partitionOperationDispatcher);
    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema(metalake, catalog, SCHEMA);
    TestPartitionOperationDispatcher.schemaOperationDispatcher.createSchema(
        schemaIdent, "comment", null);
    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(metalake, catalog, SCHEMA, "test_partition_normalize_table");
    TestPartitionOperationDispatcher.tableOperationDispatcher.createTable(
        tableIdent,
        new Column[] {
          Column.of("col1", Types.StringType.get()), Column.of("col2", Types.StringType.get())
        },
        "comment",
        null);
  }

  @Test
  public void testNameCaseInsensitive() {
    Partition partition =
        Partitions.identity(
            "pNAME",
            new String[][] {{"col1"}},
            new Literal[] {Literals.stringLiteral("v1")},
            Maps.newHashMap());
    // test case-insensitive in adding
    Partition addedPartition = partitionNormalizeDispatcher.addPartition(TABLE, partition);
    Assertions.assertEquals(partition.name().toLowerCase(), addedPartition.name());

    // test case-insensitive in getting
    Partition gotPartition = partitionNormalizeDispatcher.getPartition(TABLE, partition.name());
    Assertions.assertEquals(partition.name().toLowerCase(), gotPartition.name());

    // test case-insensitive in listing names
    String[] partitionNames = partitionNormalizeDispatcher.listPartitionNames(TABLE);
    Assertions.assertEquals(partition.name().toLowerCase(), partitionNames[0]);

    // test case-insensitive in listing partitions
    Partition[] listedPartitions = partitionNormalizeDispatcher.listPartitions(TABLE);
    Assertions.assertEquals(partition.name().toLowerCase(), listedPartitions[0].name());

    // test case-insensitive in existence check
    Assertions.assertTrue(partitionNormalizeDispatcher.partitionExists(TABLE, partition.name()));

    // test case-insensitive in dropping
    Assertions.assertTrue(
        partitionNormalizeDispatcher.dropPartition(TABLE, partition.name().toUpperCase()));
    Assertions.assertFalse(partitionNormalizeDispatcher.partitionExists(TABLE, partition.name()));
  }
}
