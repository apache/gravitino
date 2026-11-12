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

package org.apache.gravitino.spark.connector.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestHiveGravitinoOperationOperator {

  private static final StructType PARTITION_SCHEMA =
      new StructType().add("ds", DataTypes.StringType);

  private static final StructType MULTI_KEY_SCHEMA =
      new StructType()
          .add("dt", DataTypes.IntegerType, true)
          .add("region", DataTypes.StringType, true);

  private SupportsPartitions partitions;
  private HiveGravitinoOperationOperator operator;

  @BeforeEach
  void setUp() {
    Table table = mock(Table.class);
    partitions = mock(SupportsPartitions.class);
    when(table.supportPartitions()).thenReturn(partitions);
    operator = new HiveGravitinoOperationOperator(table);
  }

  @Test
  void testDropPartitionEscapesValue() {
    when(partitions.dropPartition("ds=a%2Fb")).thenReturn(true);

    Assertions.assertTrue(operator.dropPartition(ident("a/b"), PARTITION_SCHEMA));
    verify(partitions).dropPartition("ds=a%2Fb");
  }

  @Test
  void testListPartitionIdentifiersUnescapesValues() {
    when(partitions.listPartitionNames()).thenReturn(new String[] {"ds=a%2Fb", "ds=c"});

    InternalRow[] rows =
        operator.listPartitionIdentifiers(new String[] {"ds"}, ident("a/b"), PARTITION_SCHEMA);

    Assertions.assertEquals(1, rows.length);
    Assertions.assertEquals("a/b", rows[0].getUTF8String(0).toString());
  }

  @Test
  void testNullPartitionValueUsesHiveDefaultMarker() {
    ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
    when(partitions.dropPartition(name.capture())).thenReturn(true);

    InternalRow ident = new GenericInternalRow(new Object[] {20240101, null});
    operator.dropPartition(ident, MULTI_KEY_SCHEMA);

    Assertions.assertEquals("dt=20240101/region=__HIVE_DEFAULT_PARTITION__", name.getValue());
  }

  @Test
  void testAddNullPartitionValueUsesHiveDefaultMarker() throws Exception {
    ArgumentCaptor<Partition> partition = ArgumentCaptor.forClass(Partition.class);
    when(partitions.addPartition(partition.capture())).thenReturn(null);

    InternalRow ident = new GenericInternalRow(new Object[] {20240101, null});
    operator.createPartition(ident, Collections.emptyMap(), MULTI_KEY_SCHEMA);

    Literal<?>[] values = ((IdentityPartition) partition.getValue()).values();
    Assertions.assertEquals(20240101, values[0].value());
    Assertions.assertEquals("__HIVE_DEFAULT_PARTITION__", values[1].value());
  }

  @Test
  void testPartialSpecResolvesColumnsByName() {
    when(partitions.listPartitionNames())
        .thenReturn(new String[] {"dt=1/region=us", "dt=2/region=eu"});

    // SHOW PARTITIONS ... PARTITION (region = 'us') passes only the region column and its value.
    InternalRow ident = new GenericInternalRow(new Object[] {UTF8String.fromString("us")});
    InternalRow[] result =
        operator.listPartitionIdentifiers(new String[] {"region"}, ident, MULTI_KEY_SCHEMA);

    Assertions.assertEquals(1, result.length);
    Assertions.assertEquals(1, result[0].getInt(0));
    Assertions.assertEquals("us", result[0].getUTF8String(1).toString());
  }

  @Test
  void testNullValueInNonStringColumnUsesHiveDefaultMarker() throws Exception {
    InternalRow ident = new GenericInternalRow(new Object[] {null, UTF8String.fromString("us")});

    ArgumentCaptor<Partition> partition = ArgumentCaptor.forClass(Partition.class);
    when(partitions.addPartition(partition.capture())).thenReturn(null);
    operator.createPartition(ident, Collections.emptyMap(), MULTI_KEY_SCHEMA);
    Literal<?>[] values = ((IdentityPartition) partition.getValue()).values();
    Assertions.assertEquals("__HIVE_DEFAULT_PARTITION__", values[0].value());

    ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
    when(partitions.getPartition(name.capture())).thenReturn(null);
    operator.loadPartitionMetadata(ident, MULTI_KEY_SCHEMA);
    Assertions.assertEquals("dt=__HIVE_DEFAULT_PARTITION__/region=us", name.getValue());
  }

  @Test
  void testPartitionExistsWithNullInPartialSpec() {
    when(partitions.partitionExists("region=__HIVE_DEFAULT_PARTITION__")).thenReturn(true);

    InternalRow ident = new GenericInternalRow(new Object[] {null});
    Assertions.assertTrue(
        operator.partitionExists(new String[] {"region"}, ident, MULTI_KEY_SCHEMA));
  }

  @Test
  void testPartialSpecMatchesWholeColumnName() {
    StructType schema =
        new StructType()
            .add("region", DataTypes.StringType, true)
            .add("subregion", DataTypes.StringType, true);
    when(partitions.listPartitionNames())
        .thenReturn(new String[] {"region=eu/subregion=us", "region=us/subregion=eu"});

    InternalRow[] result =
        operator.listPartitionIdentifiers(new String[] {"region"}, ident("us"), schema);

    Assertions.assertEquals(1, result.length);
    Assertions.assertEquals("us", result[0].getUTF8String(0).toString());
    Assertions.assertEquals("eu", result[0].getUTF8String(1).toString());
  }

  private static InternalRow ident(String value) {
    return new GenericInternalRow(new Object[] {UTF8String.fromString(value)});
  }
}
