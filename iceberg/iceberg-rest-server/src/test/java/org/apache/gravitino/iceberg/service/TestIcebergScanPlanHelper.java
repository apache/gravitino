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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergScanPlanHelper {

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
  void testRewriteForSnapshotMapsCurrentNames() {
    PlanTableScanRequest rewritten =
        IcebergScanPlanHelper.rewriteForSnapshot(
            PlanTableScanRequest.builder()
                .withSnapshotId(7L)
                .withFilter(Expressions.equal("l2", 10L))
                .withSelect(ImmutableList.of("id", "l2", "l"))
                .withStatsFields(ImmutableList.of("l2", "l"))
                .build(),
            CURRENT_SCHEMA,
            SNAPSHOT_SCHEMA);

    Assertions.assertEquals(Expressions.equal("l", 10L).toString(), rewritten.filter().toString());
    Assertions.assertEquals(ImmutableList.of("id", "l"), rewritten.select());
    Assertions.assertEquals(ImmutableList.of("l"), rewritten.statsFields());
  }

  @Test
  void testRewriteForSnapshotResolvesCaseInsensitively() {
    PlanTableScanRequest rewritten =
        IcebergScanPlanHelper.rewriteForSnapshot(
            PlanTableScanRequest.builder()
                .withSnapshotId(7L)
                .withCaseSensitive(false)
                .withFilter(Expressions.equal("L2", 10L))
                .build(),
            CURRENT_SCHEMA,
            SNAPSHOT_SCHEMA);

    Assertions.assertEquals(Expressions.equal("l", 10L).toString(), rewritten.filter().toString());
  }

  @Test
  void testRewriteForSnapshotKeepsStarSelect() {
    PlanTableScanRequest rewritten =
        IcebergScanPlanHelper.rewriteForSnapshot(
            PlanTableScanRequest.builder()
                .withSnapshotId(7L)
                .withSelect(ImmutableList.of("*"))
                .build(),
            CURRENT_SCHEMA,
            SNAPSHOT_SCHEMA);

    Assertions.assertEquals(ImmutableList.of("*"), rewritten.select());
  }

  @Test
  void testRewriteFilterNames() {
    Assertions.assertEquals(
        Expressions.equal("l", 10L).toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal("l2", 10L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());
    Assertions.assertEquals(
        Expressions.equal("id", 1L).toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal("id", 1L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());
    Assertions.assertEquals(
        Expressions.and(Expressions.in("l", 1L, 2L), Expressions.not(Expressions.isNull("l")))
            .toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.and(
                    Expressions.in("l2", 1L, 2L), Expressions.not(Expressions.isNull("l2"))),
                CURRENT_SCHEMA,
                SNAPSHOT_SCHEMA,
                true)
            .toString());
    Assertions.assertEquals(
        Expressions.equal(Expressions.bucket("l", 4), 1).toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal(Expressions.bucket("l2", 4), 1),
                CURRENT_SCHEMA,
                SNAPSHOT_SCHEMA,
                true)
            .toString());

    // Column added after the snapshot: do not prune; callers AND the full original filter.
    Assertions.assertEquals(
        Expressions.alwaysTrue().toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.isNull("l"), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());
    Assertions.assertEquals(
        Expressions.alwaysTrue().toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal("l", 10L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());

    // Mixed OR: post-snapshot leaf becomes always-true so planning over-includes; residual keeps
    // the complete original filter.
    Assertions.assertEquals(
        Expressions.or(Expressions.equal("id", 1L), Expressions.alwaysTrue()).toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.or(Expressions.equal("id", 1L), Expressions.equal("l", 2L)),
                CURRENT_SCHEMA,
                SNAPSHOT_SCHEMA,
                true)
            .toString());

    // NOT of a post-snapshot predicate must not become always-false (would prune every file).
    Assertions.assertEquals(
        Expressions.alwaysTrue().toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.not(Expressions.equal("l", 1L)), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());
    Assertions.assertEquals(
        Expressions.not(Expressions.equal("id", 1L)).toString(),
        IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.not(Expressions.equal("id", 1L)), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true)
            .toString());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal("nope", 1L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergScanPlanHelper.rewriteFilterNames(
                Expressions.equal("L2", 10L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true));
  }

  @Test
  void testRemapResidualKeepsRequestFieldIdentities() {
    Expression residualOnSnapshot =
        IcebergScanPlanHelper.rewriteFilterNames(
            Expressions.equal("l2", 10L), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true);
    Assertions.assertEquals(Expressions.equal("l", 10L).toString(), residualOnSnapshot.toString());

    Expression residualForClient =
        IcebergScanPlanHelper.remapExpressionNames(
            residualOnSnapshot, SNAPSHOT_SCHEMA, CURRENT_SCHEMA, true);
    Assertions.assertEquals(Expressions.equal("l2", 10L).toString(), residualForClient.toString());

    // With name reuse, evaluating the remapped residual against the request row is true; the
    // snapshot-named residual would bind to the wrong field and evaluate false.
    Types.StructType rowType = CURRENT_SCHEMA.asStruct();
    StructLike row = new ArrayStruct(1L, 10L, null);
    Assertions.assertTrue(new Evaluator(rowType, residualForClient).eval(row));
    Assertions.assertFalse(new Evaluator(rowType, residualOnSnapshot).eval(row));
  }

  @Test
  void testRewriteColumnNamesDropsColumnsAddedLater() {
    Assertions.assertEquals(
        ImmutableList.of("id", "l"),
        IcebergScanPlanHelper.rewriteColumnNames(
            ImmutableList.of("id", "l2", "l"), CURRENT_SCHEMA, SNAPSHOT_SCHEMA, true));
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
        IcebergScanPlanHelper.sanitizeSpecsForSerialization(
            ImmutableMap.of(0, staleSpec),
            Collections.singletonList(task),
            ImmutableList.of(originalSchema, evolvedSchema));

    Assertions.assertFalse(
        sanitized.get(0).partitionType().fields().stream()
            .anyMatch(f -> f.type().typeId() == Type.TypeID.UNKNOWN));
    Assertions.assertEquals(
        Type.TypeID.STRING, sanitized.get(0).partitionType().fields().get(0).type().typeId());
  }

  @Test
  void testSanitizeSpecsDedupesMultipleTransformsOfOneDroppedSource() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").bucket("s", 8).build();
    Schema evolved = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    PartitionSpec stale = originalSpec.toUnbound().bind(evolved, true);

    Map<Integer, PartitionSpec> sanitized =
        IcebergScanPlanHelper.sanitizeSpecsForSerialization(
            ImmutableMap.of(0, stale),
            Collections.emptyList(),
            ImmutableList.of(originalSchema, evolved));

    Assertions.assertDoesNotThrow(() -> sanitized.get(0).schema());
    Assertions.assertEquals(
        1,
        sanitized.get(0).schema().columns().stream()
            .filter(f -> f.name().startsWith("_dropped_source_"))
            .count());
  }

  @Test
  void testSanitizeSpecsDoesNotUseTransformResultTypeAsSource() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").hour("ts").build();
    Schema evolved = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    PartitionSpec stale = originalSpec.toUnbound().bind(evolved, true);

    // Partition data carries transform results (string + int), which must not become source types.
    Types.StructType partitionDataType =
        Types.StructType.of(
            Types.NestedField.optional(1000, "s", Types.StringType.get()),
            Types.NestedField.optional(1001, "ts_hour", Types.IntegerType.get()));
    PartitionData partitionData = new PartitionData(partitionDataType);
    partitionData.set(0, "a");
    partitionData.set(1, 1);
    DataFile dataFile = mock(DataFile.class);
    doReturn(0).when(dataFile).specId();
    doReturn(partitionData).when(dataFile).partition();
    FileScanTask task = mock(FileScanTask.class);
    doReturn(dataFile).when(task).file();

    Map<Integer, PartitionSpec> sanitized =
        IcebergScanPlanHelper.sanitizeSpecsForSerialization(
            ImmutableMap.of(0, stale),
            Collections.singletonList(task),
            ImmutableList.of(originalSchema, evolved));

    Assertions.assertEquals(
        Type.TypeID.STRING, sanitized.get(0).schema().findField(2).type().typeId());
    Assertions.assertEquals(
        Type.TypeID.TIMESTAMP, sanitized.get(0).schema().findField(3).type().typeId());
  }

  @Test
  void testPrepareTasksRewritesUnknownSpecForClientRoundTrip() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").build();
    Schema evolved = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    PartitionSpec unpartitioned = PartitionSpec.builderFor(evolved).withSpecId(1).build();
    PartitionSpec stale = originalSpec.toUnbound().bind(evolved, true);

    PartitionData partitionData =
        new PartitionData(
            Types.StructType.of(Types.NestedField.optional(1000, "s", Types.StringType.get())));
    partitionData.set(0, "a");
    DataFile dataFile =
        DataFiles.builder(originalSpec)
            .withPath("file:///tmp/f.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();

    // Build a task against the stale (client-visible) spec id.
    org.apache.iceberg.BaseFileScanTask task =
        new org.apache.iceberg.BaseFileScanTask(
            DataFiles.builder(stale).copy(dataFile).withPartition(partitionData).build(),
            null,
            SchemaParser.toJson(evolved),
            PartitionSpecParser.toJson(stale),
            ResidualEvaluator.of(stale, Expressions.equal("id", 1L), true));

    Map<Integer, PartitionSpec> clientSpecs = ImmutableMap.of(0, stale, 1, unpartitioned);
    List<FileScanTask> prepared =
        IcebergScanPlanHelper.prepareTasksForResponse(
            Collections.singletonList(task), clientSpecs, evolved, null, null, true);

    Assertions.assertEquals(1, prepared.get(0).file().specId());
    Assertions.assertTrue(clientSpecs.get(prepared.get(0).file().specId()).isUnpartitioned());

    // Serialize with sanitized specs (server), parse with the client's original broken specs.
    Map<Integer, PartitionSpec> sanitized =
        IcebergScanPlanHelper.sanitizeSpecsForSerialization(
            clientSpecs, prepared, ImmutableList.of(originalSchema, evolved));
    String json =
        JsonUtil.generate(
            gen -> ContentFileParser.toJson(prepared.get(0).file(), sanitized.get(1), gen), false);
    JsonNode node = JsonUtil.parse(json, n -> n);
    DataFile parsed = (DataFile) ContentFileParser.fromJson(node, clientSpecs);
    Assertions.assertEquals("file:///tmp/f.parquet", parsed.location());
  }

  @Test
  void testPrepareTasksRewritesToCurrentSpecWhenNeverUnpartitioned() {
    // Evolution (s, id) → (id) never creates an unpartitioned spec; old files keep spec 0.
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").identity("id").build();
    Schema evolved =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    PartitionSpec currentSpec =
        PartitionSpec.builderFor(evolved).withSpecId(1).identity("id").build();
    Schema droppedS = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    PartitionSpec stale = originalSpec.toUnbound().bind(droppedS, true);
    PartitionSpec currentOnDropped = currentSpec.toUnbound().bind(droppedS, true);

    PartitionData partitionData =
        new PartitionData(
            Types.StructType.of(
                Types.NestedField.optional(1000, "s", Types.StringType.get()),
                Types.NestedField.optional(1001, "id", Types.LongType.get())));
    partitionData.set(0, "a");
    partitionData.set(1, 7L);
    DataFile dataFile =
        DataFiles.builder(originalSpec)
            .withPath("file:///tmp/sid.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();

    org.apache.iceberg.BaseFileScanTask task =
        new org.apache.iceberg.BaseFileScanTask(
            DataFiles.builder(stale).copy(dataFile).withPartition(partitionData).build(),
            null,
            SchemaParser.toJson(droppedS),
            PartitionSpecParser.toJson(stale),
            ResidualEvaluator.of(stale, Expressions.alwaysTrue(), true));

    Map<Integer, PartitionSpec> clientSpecs = ImmutableMap.of(0, stale, 1, currentOnDropped);
    List<FileScanTask> prepared =
        IcebergScanPlanHelper.prepareTasksForResponse(
            Collections.singletonList(task), clientSpecs, droppedS, null, null, true);

    Assertions.assertEquals(1, prepared.get(0).file().specId());
    Assertions.assertFalse(clientSpecs.get(1).isUnpartitioned());

    String json =
        JsonUtil.generate(
            gen -> ContentFileParser.toJson(prepared.get(0).file(), clientSpecs.get(1), gen),
            false);
    DataFile parsed =
        (DataFile) ContentFileParser.fromJson(JsonUtil.parse(json, n -> n), clientSpecs);
    Assertions.assertEquals("file:///tmp/sid.parquet", parsed.location());
    Assertions.assertEquals(1, parsed.specId());
  }

  @Test
  void testPrepareTasksRewritesDeleteFilesWithCompatibleSpec() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).withSpecId(0).identity("s").build();
    Schema evolved = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    PartitionSpec unpartitioned = PartitionSpec.builderFor(evolved).withSpecId(1).build();
    PartitionSpec stale = originalSpec.toUnbound().bind(evolved, true);

    PartitionData partitionData =
        new PartitionData(
            Types.StructType.of(Types.NestedField.optional(1000, "s", Types.StringType.get())));
    partitionData.set(0, "a");
    DataFile dataFile =
        DataFiles.builder(originalSpec)
            .withPath("file:///tmp/data.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(originalSpec)
            .ofPositionDeletes()
            .withPath("file:///tmp/data.parquet.pos-deletes")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(4)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();

    org.apache.iceberg.BaseFileScanTask task =
        new org.apache.iceberg.BaseFileScanTask(
            DataFiles.builder(stale).copy(dataFile).withPartition(partitionData).build(),
            new DeleteFile[] {
              FileMetadata.deleteFileBuilder(stale)
                  .copy(deleteFile)
                  .withPartition(partitionData)
                  .build()
            },
            SchemaParser.toJson(evolved),
            PartitionSpecParser.toJson(stale),
            ResidualEvaluator.of(stale, Expressions.alwaysTrue(), true));

    Map<Integer, PartitionSpec> clientSpecs = ImmutableMap.of(0, stale, 1, unpartitioned);
    List<FileScanTask> prepared =
        IcebergScanPlanHelper.prepareTasksForResponse(
            Collections.singletonList(task), clientSpecs, evolved, null, null, true);

    Assertions.assertEquals(1, prepared.get(0).file().specId());
    Assertions.assertEquals(1, prepared.get(0).deletes().size());
    Assertions.assertEquals(1, prepared.get(0).deletes().get(0).specId());

    String deleteJson =
        JsonUtil.generate(
            gen -> ContentFileParser.toJson(prepared.get(0).deletes().get(0), unpartitioned, gen),
            false);
    DeleteFile parsedDelete =
        (DeleteFile) ContentFileParser.fromJson(JsonUtil.parse(deleteJson, n -> n), clientSpecs);
    Assertions.assertEquals("file:///tmp/data.parquet.pos-deletes", parsedDelete.location());
    Assertions.assertEquals(1, parsedDelete.specId());
  }

  @Test
  void testPrepareTasksResidualEvaluatorUsesRequestSchema() {
    // Partitioned by id; snapshot schema names field 2 "l", request schema renamed it to "l2".
    Schema snapshotSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "l", Types.LongType.get()));
    Schema requestSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "l2", Types.LongType.get()));
    PartitionSpec snapshotSpec =
        PartitionSpec.builderFor(snapshotSchema).withSpecId(0).identity("id").build();
    PartitionSpec requestSpec = snapshotSpec.toUnbound().bind(requestSchema, true);

    PartitionData partitionData =
        new PartitionData(
            Types.StructType.of(Types.NestedField.optional(1000, "id", Types.LongType.get())));
    partitionData.set(0, 1L);
    DataFile dataFile =
        DataFiles.builder(snapshotSpec)
            .withPath("file:///tmp/part.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withPartition(partitionData)
            .build();

    Expression snapshotResidual = Expressions.equal("l", 10L);
    org.apache.iceberg.BaseFileScanTask task =
        new org.apache.iceberg.BaseFileScanTask(
            dataFile,
            null,
            SchemaParser.toJson(snapshotSchema),
            PartitionSpecParser.toJson(snapshotSpec),
            ResidualEvaluator.of(snapshotSpec, snapshotResidual, true));

    List<FileScanTask> prepared =
        IcebergScanPlanHelper.prepareTasksForResponse(
            Collections.singletonList(task),
            ImmutableMap.of(0, requestSpec),
            requestSchema,
            snapshotSchema,
            null,
            true);

    Assertions.assertEquals(
        Expressions.equal("l2", 10L).toString(), prepared.get(0).residual().toString());
    // ResidualEvaluator must bind remapped names against the request schema, not the snapshot.
    Types.StructType rowType = requestSchema.asStruct();
    Assertions.assertTrue(
        new Evaluator(rowType, prepared.get(0).residual()).eval(new ArrayStruct(1L, 10L)));
  }

  @Test
  void testRemapExpressionRoundTripThroughParser() {
    Expression remapped =
        IcebergScanPlanHelper.remapExpressionNames(
            Expressions.equal("l", 10L), SNAPSHOT_SCHEMA, CURRENT_SCHEMA, true);
    String json = ExpressionParser.toJson(remapped);
    Expression parsed = ExpressionParser.fromJson(json);
    Assertions.assertEquals(Expressions.equal("l2", 10L).toString(), parsed.toString());
  }

  private static final class ArrayStruct implements StructLike {
    private final Object[] values;

    private ArrayStruct(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) values[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }
}
