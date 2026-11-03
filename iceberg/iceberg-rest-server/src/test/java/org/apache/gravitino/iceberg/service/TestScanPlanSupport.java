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
package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestScanPlanSupport {

  @Test
  void testCurrentNameToSnapshotName() {
    Schema snapshot =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "l", Types.LongType.get()));
    Schema current =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "l2", Types.LongType.get()));

    Assertions.assertEquals(
        ImmutableMap.of("l2", "l"), ScanPlanSupport.currentNameToSnapshotName(current, snapshot));
    Assertions.assertEquals(
        ImmutableMap.of(), ScanPlanSupport.currentNameToSnapshotName(current, current));
  }

  @Test
  void testRewriteFilterNames() {
    Map<String, String> mapping = ImmutableMap.of("l2", "l");
    Expression rewritten =
        ScanPlanSupport.rewriteFilterNames(Expressions.equal("l2", 10L), mapping);
    Assertions.assertEquals(Expressions.equal("l", 10L).toString(), rewritten.toString());

    Expression unchanged = ScanPlanSupport.rewriteFilterNames(Expressions.equal("id", 1L), mapping);
    Assertions.assertEquals(Expressions.equal("id", 1L).toString(), unchanged.toString());
  }

  @Test
  void testRewriteColumnNames() {
    Assertions.assertEquals(
        ImmutableList.of("id", "l"),
        ScanPlanSupport.rewriteColumnNames(
            ImmutableList.of("id", "l2"), ImmutableMap.of("l2", "l")));
  }

  @Test
  void testSanitizeSpecsForDroppedPartitionSource() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()),
            Types.NestedField.optional(3, "n", Types.IntegerType.get()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").build();

    Schema evolvedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "n", Types.IntegerType.get()));
    // allowMissingFields=true mirrors a table whose current schema dropped the partition source
    // while the historical spec is still retained in specs-by-id.
    PartitionSpec staleSpec = originalSpec.toUnbound().bind(evolvedSchema, true);
    Assertions.assertTrue(
        staleSpec.partitionType().fields().stream()
            .anyMatch(f -> f.type().typeId() == Type.TypeID.UNKNOWN));

    Types.StructType partitionDataType =
        Types.StructType.of(Types.NestedField.optional(1000, "s", Types.StringType.get()));
    PartitionData partitionData = new PartitionData(partitionDataType);
    partitionData.set(0, "a");

    DataFile dataFile = mock(DataFile.class);
    doReturn(0).when(dataFile).specId();
    doReturn(partitionData).when(dataFile).partition();

    FileScanTask task = mock(FileScanTask.class);
    doReturn(dataFile).when(task).file();

    Map<Integer, PartitionSpec> sanitized =
        ScanPlanSupport.sanitizeSpecsForSerialization(
            ImmutableMap.of(0, staleSpec), Collections.singletonList(task));

    Assertions.assertFalse(
        sanitized.get(0).partitionType().fields().stream()
            .anyMatch(f -> f.type().typeId() == Type.TypeID.UNKNOWN));
    Assertions.assertEquals(
        Type.TypeID.STRING, sanitized.get(0).partitionType().fields().get(0).type().typeId());
  }
}
