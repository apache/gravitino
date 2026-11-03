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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestScanPlanSupport {

  // Field 2 was "l" at the snapshot and is "l2" now; field 3, added later, reuses the name "l".
  private static final Schema SNAPSHOT_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "l", Types.LongType.get()));
  private static final Schema CURRENT_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "l2", Types.LongType.get()),
          Types.NestedField.optional(3, "l", Types.LongType.get()));

  @Test
  void testSnapshotNamesResolveOnTheCurrentSchema() {
    ScanPlanSupport.SnapshotNames names = names(true);
    Assertions.assertEquals("id", names.toSnapshotName("id"));
    Assertions.assertEquals("l", names.toSnapshotName("l2"));
    // "l" is the column added after the snapshot, not the snapshot's "l".
    Assertions.assertNull(names.toSnapshotName("l"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> names.toSnapshotName("nope"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> names.toSnapshotName("L2"));

    Assertions.assertEquals("l", names(false).toSnapshotName("L2"));
  }

  @Test
  void testRewriteFilterNames() {
    ScanPlanSupport.SnapshotNames names = names(true);
    Assertions.assertEquals(
        Expressions.equal("l", 10L).toString(),
        ScanPlanSupport.rewriteFilterNames(Expressions.equal("l2", 10L), names).toString());
    Assertions.assertEquals(
        Expressions.equal("id", 1L).toString(),
        ScanPlanSupport.rewriteFilterNames(Expressions.equal("id", 1L), names).toString());
    Assertions.assertEquals(
        Expressions.and(Expressions.in("l", 1L, 2L), Expressions.not(Expressions.isNull("l")))
            .toString(),
        ScanPlanSupport.rewriteFilterNames(
                Expressions.and(
                    Expressions.in("l2", 1L, 2L), Expressions.not(Expressions.isNull("l2"))),
                names)
            .toString());
    Assertions.assertEquals(
        Expressions.equal(Expressions.bucket("l", 4), 1).toString(),
        ScanPlanSupport.rewriteFilterNames(Expressions.equal(Expressions.bucket("l2", 4), 1), names)
            .toString());

    IllegalArgumentException addedLater =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ScanPlanSupport.rewriteFilterNames(Expressions.equal("l", 10L), names));
    Assertions.assertTrue(
        addedLater.getMessage().contains("does not exist in snapshot 7"), addedLater.getMessage());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ScanPlanSupport.rewriteFilterNames(Expressions.equal("nope", 1L), names));
  }

  @Test
  void testRewriteColumnNamesDropsColumnsAddedLater() {
    Assertions.assertEquals(
        ImmutableList.of("id", "l"),
        ScanPlanSupport.rewriteColumnNames(ImmutableList.of("id", "l2", "l"), names(true)));
  }

  private static ScanPlanSupport.SnapshotNames names(boolean caseSensitive) {
    return new ScanPlanSupport.SnapshotNames(CURRENT_SCHEMA, SNAPSHOT_SCHEMA, caseSensitive, 7L);
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
