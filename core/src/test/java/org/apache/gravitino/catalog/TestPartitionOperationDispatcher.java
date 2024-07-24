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
import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
      NameIdentifierUtil.ofTable(metalake, catalog, SCHEMA, TABLE);
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

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema(metalake, catalog, SCHEMA);
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
