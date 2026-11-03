/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/** Tests server-side scan planning against the REST spec's request semantics. */
public class TestIcebergScanPlanning {

  private static final TableIdentifier IDENT = TableIdentifier.of(Namespace.of("db"), "t");
  private static final List<String> F1 = ImmutableList.of("/data/f1.parquet");
  private static final List<String> NONE = ImmutableList.of();

  private CatalogWrapperForREST wrapper;
  private Table table;

  @BeforeEach
  public void setUp() {
    wrapper =
        new CatalogWrapperForREST(
            "scan-test",
            new IcebergConfig(
                ImmutableMap.of(
                    IcebergConstants.CATALOG_BACKEND, "memory",
                    IcebergConstants.WAREHOUSE, "/tmp/warehouse")));
    ((SupportsNamespaces) wrapper.getCatalog()).createNamespace(Namespace.of("db"));
    table =
        wrapper
            .getCatalog()
            .createTable(
                IDENT,
                new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "a", Types.StringType.get())),
                PartitionSpec.unpartitioned());
  }

  @Test
  public void testIncrementalScanStartIsExclusive() {
    long s1 = append("/data/f1.parquet");
    append("/data/f2.parquet");
    long s3 = append("/data/f3.parquet");

    Assertions.assertEquals(
        ImmutableList.of("/data/f2.parquet", "/data/f3.parquet"),
        planFiles(PlanTableScanRequest.builder().withStartSnapshotId(s1).withEndSnapshotId(s3)));
    Assertions.assertEquals(
        ImmutableList.of(),
        planFiles(PlanTableScanRequest.builder().withStartSnapshotId(s3).withEndSnapshotId(s3)));
  }

  @Test
  public void testInvalidFilterAndStatsFieldsAreClientErrors() {
    append("/data/f1.parquet");
    IllegalArgumentException unknownColumn =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                planFiles(PlanTableScanRequest.builder().withFilter(Expressions.equal("nope", 1))));
    Assertions.assertFalse(
        unknownColumn.getMessage().contains("struct<"), unknownColumn.getMessage());

    // Case-sensitive by default.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> planFiles(PlanTableScanRequest.builder().withFilter(Expressions.equal("ID", 1L))));

    IllegalArgumentException unknownStats =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                planFiles(
                    PlanTableScanRequest.builder().withStatsFields(ImmutableList.of("nope"))));
    Assertions.assertTrue(unknownStats.getMessage().contains("nope"), unknownStats.getMessage());
    Assertions.assertFalse(
        unknownStats.getMessage().contains("NullPointerException"), unknownStats.getMessage());
  }

  @Test
  public void testConstantFiltersInTheSpecFormAreAccepted() throws Exception {
    PlanTableScanRequest request =
        IcebergObjectMapper.getInstance()
            .readValue(
                "{\"filter\":{\"type\":\"not\",\"child\":{\"type\":\"or\","
                    + "\"left\":{\"type\":\"false\"},\"right\":{\"type\":\"false\"}}}}",
                PlanTableScanRequest.class);
    Assertions.assertEquals(Expression.Operation.TRUE, request.filter().op());

    PlanTableScanRequest alsoAccepted =
        IcebergObjectMapper.getInstance()
            .readValue("{\"filter\":true}", PlanTableScanRequest.class);
    Assertions.assertEquals(Expression.Operation.TRUE, alsoAccepted.filter().op());
  }

  @Test
  public void testBranchScanBindsToTheCurrentSchema() {
    EvolvedTable evolved = evolvedTable("branch_scan");
    // Branch "b" still points at s1, so this is the request Iceberg's client sends for a branch.
    evolved.table.manageSnapshots().createBranch("b", evolved.s1).commit();

    Assertions.assertEquals(F1, planBranch(evolved, Expressions.equal("l2", 10L)));
    Assertions.assertEquals(NONE, planBranch(evolved, Expressions.equal("l2", 99L)));
    // "l" is the column added after s1. It has no values in f1, so f1 is not pruned on it,
    // while binding to the snapshot's "l" (field 2, bounds [10, 10]) would prune f1.
    Assertions.assertEquals(F1, planBranch(evolved, Expressions.equal("l", 99L)));
    assertInvalidScan(() -> planBranch(evolved, Expressions.equal("a", 5L)), "'a'");

    Assertions.assertEquals(
        F1,
        planFiles(
            evolved.ident,
            branchRequest(evolved).withSelect(ImmutableList.of("id", "l2", "l", "a2"))));
    Assertions.assertEquals(
        F1,
        planFiles(
            evolved.ident,
            branchRequest(evolved)
                .withCaseSensitive(false)
                .withFilter(Expressions.equal("L2", 10L))));
  }

  @Test
  public void testSnapshotNoBranchPointsAtBindsToTheCurrentSchema() {
    EvolvedTable evolved = evolvedTable("moved_branch");
    // The branch moved past s1 after the client read it, so no branch points at s1 any more.
    evolved.table.manageSnapshots().createBranch("b", evolved.s1).commit();
    evolved
        .table
        .newFastAppend()
        .appendFile(file("/data/f3.parquet", ImmutableMap.of(2, 30L)))
        .toBranch("b")
        .commit();
    evolved.table.refresh();

    Assertions.assertEquals(F1, planBranch(evolved, Expressions.equal("l2", 10L)));
    Assertions.assertEquals(NONE, planBranch(evolved, Expressions.equal("l2", 99L)));
    assertInvalidScan(
        () -> planBranch(evolved, Expressions.equal("l", 99L)), "does not exist in snapshot");
    assertInvalidScan(() -> planBranch(evolved, Expressions.equal("a", 5L)), "'a'");

    Assertions.assertEquals(
        F1,
        planFiles(
            evolved.ident,
            branchRequest(evolved)
                .withSelect(ImmutableList.of("id", "l2", "l", "a2"))
                .withStatsFields(ImmutableList.of("l2", "l"))));
    Assertions.assertEquals(
        F1,
        planFiles(
            evolved.ident,
            branchRequest(evolved)
                .withCaseSensitive(false)
                .withFilter(Expressions.equal("L2", 10L))));
  }

  @Test
  public void testTimeTravelBindsToTheSnapshotSchema() {
    EvolvedTable evolved = evolvedTable("time_travel");
    PlanTableScanRequest.Builder request =
        PlanTableScanRequest.builder().withSnapshotId(evolved.s1).withUseSnapshotSchema(true);

    Assertions.assertEquals(
        F1, planFiles(evolved.ident, request.withFilter(Expressions.equal("l", 10L))));
    assertInvalidScan(
        () ->
            planFiles(
                evolved.ident,
                PlanTableScanRequest.builder()
                    .withSnapshotId(evolved.s1)
                    .withUseSnapshotSchema(true)
                    .withFilter(Expressions.equal("l2", 10L))),
        "'l2'");
  }

  @Test
  public void testDeletionVectorsSharingAFileAreRefused() {
    DeleteFile dv1 = deletionVector("/data/dv.puffin", 4L, "/data/f1.parquet");
    DeleteFile dv2 = deletionVector("/data/dv.puffin", 120L, "/data/f2.parquet");
    FileScanTask task1 = Mockito.mock(FileScanTask.class);
    Mockito.when(task1.deletes()).thenReturn(ImmutableList.of(dv1));
    FileScanTask task2 = Mockito.mock(FileScanTask.class);
    Mockito.when(task2.deletes()).thenReturn(ImmutableList.of(dv2));

    Assertions.assertThrows(
        ServiceFailureException.class,
        () ->
            CatalogWrapperForREST.checkDeleteFilesCanBeReferenced(
                IDENT, ImmutableList.of(task1, task2)));
    // The same delete file shared by tasks is fine.
    Assertions.assertDoesNotThrow(
        () ->
            CatalogWrapperForREST.checkDeleteFilesCanBeReferenced(
                IDENT, ImmutableList.of(task1, task1)));
  }

  private static DeleteFile deletionVector(String location, long offset, String dataFile) {
    DeleteFile file = Mockito.mock(DeleteFile.class);
    Mockito.when(file.location()).thenReturn(location);
    Mockito.when(file.contentOffset()).thenReturn(offset);
    Mockito.when(file.referencedDataFile()).thenReturn(dataFile);
    return file;
  }

  private long append(String path) {
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
    table.refresh();
    return table.currentSnapshot().snapshotId();
  }

  private List<String> planFiles(PlanTableScanRequest.Builder request) {
    return planFiles(IDENT, request);
  }

  private List<String> planFiles(TableIdentifier ident, PlanTableScanRequest.Builder request) {
    PlanTableScanResponse response = wrapper.planTableScan(ident, request.build());
    return response.fileScanTasks().stream()
        .map(task -> task.file().location())
        .sorted()
        .collect(Collectors.toList());
  }

  private List<String> planBranch(EvolvedTable evolved, Expression filter) {
    return planFiles(evolved.ident, branchRequest(evolved).withFilter(filter));
  }

  private static PlanTableScanRequest.Builder branchRequest(EvolvedTable evolved) {
    return PlanTableScanRequest.builder().withSnapshotId(evolved.s1).withUseSnapshotSchema(false);
  }

  private static void assertInvalidScan(Executable plan, String field) {
    IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, plan);
    Assertions.assertTrue(e.getMessage().contains(field), e.getMessage());
  }

  /**
   * Creates a table whose snapshot s1 holds f1 with l=10 and a=5. Main then moves on to f2, and the
   * schema evolves: l is renamed to l2, a new column reuses the name l, and a is renamed to a2.
   */
  private EvolvedTable evolvedTable(String name) {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), name);
    Table evolved =
        wrapper
            .getCatalog()
            .createTable(
                ident,
                new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "l", Types.LongType.get()),
                    Types.NestedField.optional(3, "a", Types.LongType.get())),
                PartitionSpec.unpartitioned());
    evolved
        .newFastAppend()
        .appendFile(file("/data/f1.parquet", ImmutableMap.of(2, 10L, 3, 5L)))
        .commit();
    evolved.refresh();
    long s1 = evolved.currentSnapshot().snapshotId();
    evolved.newFastAppend().appendFile(file("/data/f2.parquet", ImmutableMap.of(2, 20L))).commit();
    // Rename first so the next update can reuse the freed name "l".
    evolved.updateSchema().renameColumn("l", "l2").renameColumn("a", "a2").commit();
    evolved.updateSchema().addColumn("l", Types.LongType.get()).commit();
    evolved.refresh();
    return new EvolvedTable(ident, evolved, s1);
  }

  private static DataFile file(String path, Map<Integer, Long> values) {
    Map<Integer, ByteBuffer> bounds = new HashMap<>();
    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullCounts = new HashMap<>();
    values.forEach(
        (fieldId, value) -> {
          bounds.put(fieldId, Conversions.toByteBuffer(Types.LongType.get(), value));
          valueCounts.put(fieldId, 1L);
          nullCounts.put(fieldId, 0L);
        });
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .withMetrics(new Metrics(1L, null, valueCounts, nullCounts, null, bounds, bounds))
        .build();
  }

  private static final class EvolvedTable {
    private final TableIdentifier ident;
    private final Table table;
    private final long s1;

    private EvolvedTable(TableIdentifier ident, Table table, long s1) {
      this.ident = ident;
      this.table = table;
      this.s1 = s1;
    }
  }
}
