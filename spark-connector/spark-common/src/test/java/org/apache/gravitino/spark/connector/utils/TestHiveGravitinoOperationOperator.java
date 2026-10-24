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

import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHiveGravitinoOperationOperator {

  private static final StructType PARTITION_SCHEMA =
      new StructType().add("ds", DataTypes.StringType);

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

  private static InternalRow ident(String value) {
    return new GenericInternalRow(new Object[] {UTF8String.fromString(value)});
  }
}
