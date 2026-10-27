/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.spark.connector.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link HiveGravitinoOperationOperator} partition-name building. */
public class TestHiveGravitinoOperationOperator {

  private static final StructType SCHEMA =
      new StructType()
          .add("dt", DataTypes.IntegerType, true)
          .add("region", DataTypes.StringType, true);

  @Test
  void testNullPartitionValueUsesHiveDefaultMarker() {
    SupportsPartitions partitions = mock(SupportsPartitions.class);
    ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
    when(partitions.dropPartition(name.capture())).thenReturn(true);

    InternalRow ident = new GenericInternalRow(new Object[] {20240101, null});
    operator(partitions).dropPartition(ident, SCHEMA);

    // HMS stores a null partition value as the default partition marker.
    assertEquals("dt=20240101/region=__HIVE_DEFAULT_PARTITION__", name.getValue());
  }

  @Test
  void testPartialSpecResolvesColumnsByName() {
    SupportsPartitions partitions = mock(SupportsPartitions.class);
    when(partitions.listPartitionNames())
        .thenReturn(new String[] {"dt=1/region=us", "dt=2/region=eu"});

    // SHOW PARTITIONS ... PARTITION (region = 'us') passes only the region column and its value.
    InternalRow ident = new GenericInternalRow(new Object[] {UTF8String.fromString("us")});
    InternalRow[] result =
        operator(partitions).listPartitionIdentifiers(new String[] {"region"}, ident, SCHEMA);

    assertEquals(1, result.length);
    assertEquals(1, result[0].getInt(0));
    assertEquals("us", result[0].getUTF8String(1).toString());
  }

  private static HiveGravitinoOperationOperator operator(SupportsPartitions partitions) {
    Table table = mock(Table.class);
    when(table.supportPartitions()).thenReturn(partitions);
    return new HiveGravitinoOperationOperator(table);
  }
}
