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
import org.apache.gravitino.connector.capability.Capability;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestPartitionNormalizeDispatcher extends TestOperationDispatcher {

  private static PartitionNormalizeDispatcher partitionNormalizeDispatcher;
  private static final String SCHEMA = "test_partition_normalize_schema";
  private static final NameIdentifier TABLE =
      NameIdentifierUtil.ofTable(metalake, catalog, SCHEMA, "TEST_PARTITION_NORMALIZE_TABLE");

  @BeforeAll
  public static void initialize() throws IllegalAccessException {
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

  @Test
  public void testAddPartitionListPartitionsGetPartitionRoundTrip() throws Exception {
    // Mirrors the correct usage pattern for a quote-aware catalog (e.g. Oracle):
    // 1. addPartition is issued with a quoted, case-sensitive name, so the catalog stores the
    //    physical partition with its case preserved: My Partition.
    // 2. listPartitionNames()/listPartitions() surface that physical name unquoted (My
    //    Partition) -- names are not re-folded by the core layer.
    // 3. Getting the partition again requires re-quoting the case-sensitive name
    //    ("My Partition"), not passing the bare listed name back in: normalizeName cannot tell
    //    an already-canonical name apart from raw user input.
    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(metalake, catalog, "schema", "quotedTable");
    PartitionDispatcher mockDispatcher = Mockito.mock(PartitionDispatcher.class);

    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper mockWrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(mockWrapper.doWithCapabilityOps(Mockito.any()))
        .thenAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              org.apache.gravitino.utils.ThrowableFunction<Capability, ?> fn = inv.getArgument(0);
              return fn.apply(TestCapabilityHelpers.QUOTE_AWARE_CAPABILITY);
            });
    Mockito.when(mockCatalogManager.loadCatalogAndWrap(Mockito.any(NameIdentifier.class)))
        .thenReturn(mockWrapper);

    PartitionNormalizeDispatcher dispatcher =
        new PartitionNormalizeDispatcher(mockDispatcher, mockCatalogManager);

    // 1. Add the partition with a quoted, case-sensitive name.
    Partition quotedPartition =
        Partitions.identity(
            "\"My Partition\"",
            new String[][] {{"col1"}},
            new Literal[] {Literals.stringLiteral("v1")},
            Maps.newHashMap());
    Mockito.when(mockDispatcher.addPartition(Mockito.any(NameIdentifier.class), Mockito.any()))
        .thenReturn(quotedPartition);
    dispatcher.addPartition(tableIdent, quotedPartition);

    ArgumentCaptor<Partition> addedPartitionCaptor = ArgumentCaptor.forClass(Partition.class);
    Mockito.verify(mockDispatcher)
        .addPartition(Mockito.any(NameIdentifier.class), addedPartitionCaptor.capture());
    String physicalName = addedPartitionCaptor.getValue().name();
    Assertions.assertEquals("My Partition", physicalName);

    // 2. listPartitionNames()/listPartitions() must surface that physical name unchanged.
    Partition physicalPartition =
        Partitions.identity(
            physicalName,
            new String[][] {{"col1"}},
            new Literal[] {Literals.stringLiteral("v1")},
            Maps.newHashMap());
    Mockito.when(mockDispatcher.listPartitionNames(Mockito.any(NameIdentifier.class)))
        .thenReturn(new String[] {physicalName});
    Mockito.when(mockDispatcher.listPartitions(Mockito.any(NameIdentifier.class)))
        .thenReturn(new Partition[] {physicalPartition});

    String[] listedNames = dispatcher.listPartitionNames(tableIdent);
    Assertions.assertEquals(1, listedNames.length);
    Assertions.assertEquals("My Partition", listedNames[0]);

    Partition[] listedPartitions = dispatcher.listPartitions(tableIdent);
    Assertions.assertEquals(1, listedPartitions.length);
    Assertions.assertEquals("My Partition", listedPartitions[0].name());

    // 3. Getting the partition again requires re-quoting the listed name.
    dispatcher.getPartition(tableIdent, "\"" + listedNames[0] + "\"");

    ArgumentCaptor<String> gotPartitionNameCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockDispatcher)
        .getPartition(Mockito.any(NameIdentifier.class), gotPartitionNameCaptor.capture());
    Assertions.assertEquals("My Partition", gotPartitionNameCaptor.getValue());
  }
}
