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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests server-side scan planning against the REST spec's request semantics. */
public class TestIcebergScanPlanning {

  private static final TableIdentifier IDENT = TableIdentifier.of(Namespace.of("db"), "t");

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
    PlanTableScanResponse response = wrapper.planTableScan(IDENT, request.build());
    return response.fileScanTasks().stream()
        .map(task -> task.file().location())
        .sorted()
        .collect(Collectors.toList());
  }
}
