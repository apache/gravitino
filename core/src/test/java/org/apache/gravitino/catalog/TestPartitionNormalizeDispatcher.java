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
package org.apache.gravitino.catalog;

import com.google.common.collect.Maps;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
            TestPartitionOperationDispatcher.partitionOperationDispatcher, catalogManager);
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
