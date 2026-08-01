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

package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.cache.LocalScanPlanCache;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponseParser;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the two-step scan planning protocol: {@code planTableScan} handing out {@code plan-tasks}
 * once a plan outgrows one batch, and {@code fetchScanTasks} redeeming those plan tasks.
 */
@SuppressWarnings("deprecation")
public class TestScanPlanTaskBatching {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final PlanTableScanRequest SCAN_ALL = PlanTableScanRequest.builder().build();

  @TempDir private Path warehouse;

  @Test
  void testPlanHandsOutPlanTasksAndFetchReturnsTheirTasks() {
    CatalogWrapperForREST wrapper = newWrapper("batched", 1);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);
    Assertions.assertEquals(
        1, plan.fileScanTasks().size(), "Only one batch of tasks should be returned inline");
    Assertions.assertEquals(
        2, plan.planTasks().size(), "Remaining tasks should be handed out as plan tasks");
    // The plan a client sees must be serializable with both inline tasks and plan tasks present.
    Assertions.assertDoesNotThrow(() -> PlanTableScanResponseParser.toJson(plan));

    List<String> fetchedLocations = new ArrayList<>(dataFileLocations(plan.fileScanTasks()));
    for (String planTask : plan.planTasks()) {
      FetchScanTasksResponse response =
          wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(planTask));

      Assertions.assertEquals(1, response.fileScanTasks().size());
      Assertions.assertFalse(
          response.specsById().isEmpty(),
          "Partition specs are required to deserialize file scan tasks");
      Assertions.assertNull(
          response.planTasks(), "A redeemed plan task hands out no further tasks");
      Assertions.assertDoesNotThrow(() -> FetchScanTasksResponseParser.toJson(response));

      fetchedLocations.addAll(dataFileLocations(response.fileScanTasks()));
    }

    Assertions.assertEquals(
        expectedDataFileLocations("tbl", 3),
        fetchedLocations.stream().sorted().collect(Collectors.toList()),
        "Every planned file must be reachable exactly once across the plan and its plan tasks");
  }

  @Test
  void testPlanTaskResolvesAgainstThePinnedSnapshotAfterTheTableChanges() {
    CatalogWrapperForREST wrapper = newWrapper("pinned", 1);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);
    // A plan task is redeemed after the client planned, so the table may have moved on since.
    appendDataFile(wrapper, tableId, dataFileLocation("tbl", 99));

    List<String> fetchedLocations = new ArrayList<>(dataFileLocations(plan.fileScanTasks()));
    for (String planTask : plan.planTasks()) {
      fetchedLocations.addAll(
          dataFileLocations(
              wrapper
                  .fetchScanTasks(tableId, new FetchScanTasksRequest(planTask))
                  .fileScanTasks()));
    }

    Assertions.assertEquals(
        expectedDataFileLocations("tbl", 3),
        fetchedLocations.stream().sorted().collect(Collectors.toList()),
        "Plan tasks must resolve against the snapshot that was planned, not the current one");
  }

  @Test
  void testFetchServesTheCachedPlan() {
    CatalogWrapperForREST wrapper =
        newWrapper(
            "cached",
            ImmutableMap.of(
                IcebergConstants.SCAN_PLAN_TASK_BATCH_SIZE,
                "1",
                IcebergConstants.SCAN_PLAN_CACHE_IMPL,
                LocalScanPlanCache.class.getName()));
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);

    List<String> fetchedLocations = new ArrayList<>(dataFileLocations(plan.fileScanTasks()));
    for (String planTask : plan.planTasks()) {
      fetchedLocations.addAll(
          dataFileLocations(
              wrapper
                  .fetchScanTasks(tableId, new FetchScanTasksRequest(planTask))
                  .fileScanTasks()));
    }

    Assertions.assertEquals(
        expectedDataFileLocations("tbl", 3),
        fetchedLocations.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  void testPlanKeepsEveryTaskInlineWhenItFitsInOneBatch() {
    CatalogWrapperForREST wrapper = newWrapper("unbatched", 3);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);

    Assertions.assertEquals(3, plan.fileScanTasks().size());
    Assertions.assertNull(
        plan.planTasks(), "A plan a client can read in one response needs no plan tasks");
  }

  @Test
  void testBatchingCanBeDisabled() {
    CatalogWrapperForREST wrapper = newWrapper("disabled", 0);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);

    Assertions.assertEquals(3, plan.fileScanTasks().size());
    Assertions.assertNull(plan.planTasks());
  }

  @Test
  void testFetchRejectsPlanTasksItDidNotIssue() {
    CatalogWrapperForREST wrapper = newWrapper("rejecting", 1);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 3);
    TableIdentifier otherTableId = createTableWithDataFiles(wrapper, "other", 3);

    Assertions.assertThrows(
        NoSuchPlanTaskException.class,
        () -> wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest("not-a-plan-task")),
        "A string this server never issued is an unknown plan task");

    String planTaskForOtherTable = PlanTaskCodec.encode(otherTableId, SCAN_ALL, 1, 1);
    Assertions.assertThrows(
        NoSuchPlanTaskException.class,
        () -> wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(planTaskForOtherTable)),
        "A plan task encoded for another table must not resolve against this one");

    String planTaskPastEndOfPlan = PlanTaskCodec.encode(tableId, SCAN_ALL, 99, 1);
    Assertions.assertThrows(
        NoSuchPlanTaskException.class,
        () -> wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(planTaskPastEndOfPlan)),
        "A plan task pointing past the end of the plan is stale");
  }

  @Test
  void testBatchingKeepsDeleteFilesWithTheTasksThatReferenceThem() {
    // A batch is a slice of the plan, and tasks reference delete files by index into the response
    // that carries them, so slicing must not leave a task pointing at a delete file that is not in
    // its own response.
    CatalogWrapperForREST wrapper = newWrapper("merge-on-read", 1);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 2);
    appendPositionDeleteFile(wrapper, tableId, dataFileLocation("tbl", 1));

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);
    Assertions.assertTrue(
        plan.deleteFiles().isEmpty(),
        "The first batch covers the data file without deletes, so it lists no delete files");
    Assertions.assertDoesNotThrow(() -> PlanTableScanResponseParser.toJson(plan));

    FetchScanTasksResponse batchWithDeletes =
        wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(plan.planTasks().get(0)));

    Assertions.assertEquals(
        1,
        batchWithDeletes.deleteFiles().size(),
        "The batch holding the deleted-from data file must carry its delete file");
    String batchJson = FetchScanTasksResponseParser.toJson(batchWithDeletes);
    Assertions.assertTrue(
        batchJson.contains("\"delete-file-references\":[0]"),
        "The task must point at the delete file listed in its own response, but was: " + batchJson);
  }

  @Test
  void testTasksOverTheSameFileAreOrderedApart() {
    // A plan task addresses tasks by position, so tasks that a re-plan could return in a different
    // order must still order the same way. Data file location, offset and length are not enough for
    // that: appending one path twice leaves two manifest entries that tie on all three, and only
    // the entry identity tells them apart. Without a further tie-break, a re-plan could swap the
    // two around a batch boundary, serving one twice and dropping the other.
    CatalogWrapperForREST wrapper = newWrapper("duplicate-path", 1);
    TableIdentifier tableId = createTableWithDataFiles(wrapper, "tbl", 0);
    String duplicatedLocation = dataFileLocation("tbl", 0);
    appendDataFile(wrapper, tableId, duplicatedLocation);
    appendDataFile(wrapper, tableId, duplicatedLocation);

    PlanTableScanResponse plan = planTableScan(wrapper, tableId);
    Assertions.assertEquals(1, plan.fileScanTasks().size());
    Assertions.assertEquals(1, plan.planTasks().size());

    String planTask = plan.planTasks().get(0);
    List<FileScanTask> fetched =
        wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(planTask)).fileScanTasks();
    Assertions.assertEquals(1, fetched.size());

    Assertions.assertEquals(
        ImmutableList.of(duplicatedLocation, duplicatedLocation),
        ImmutableList.<String>builder()
            .addAll(dataFileLocations(plan.fileScanTasks()))
            .addAll(dataFileLocations(fetched))
            .build());
    // Positions must follow the entries, not the order planning happened to return them in: the
    // older entry sorts first because the tasks tie on location, offset and length. Iceberg plans
    // the newest manifest first, so an order that merely preserved the planning order would put
    // sequence number 2 in the inline batch.
    Assertions.assertEquals(
        ImmutableList.of(1L),
        dataSequenceNumbers(plan.fileScanTasks()),
        "The inline batch must hold the older manifest entry of the duplicated file");
    Assertions.assertEquals(
        ImmutableList.of(2L),
        dataSequenceNumbers(fetched),
        "The plan task must hold the newer manifest entry of the duplicated file");

    // Redeeming the same plan task again must land on the same entry.
    Assertions.assertEquals(
        dataSequenceNumbers(fetched),
        dataSequenceNumbers(
            wrapper.fetchScanTasks(tableId, new FetchScanTasksRequest(planTask)).fileScanTasks()));
  }

  @Test
  void testFetchReportsAMissingTableRatherThanAnUnknownPlanTask() {
    CatalogWrapperForREST wrapper = newWrapper("missing-table", 1);
    ((SupportsNamespaces) wrapper.getCatalog()).createNamespace(NAMESPACE);
    TableIdentifier missingTableId = TableIdentifier.of(NAMESPACE, "missing");

    Assertions.assertThrows(
        org.apache.iceberg.exceptions.NoSuchTableException.class,
        () -> wrapper.fetchScanTasks(missingTableId, new FetchScanTasksRequest("any-plan-task")));
  }

  private PlanTableScanResponse planTableScan(
      CatalogWrapperForREST wrapper, TableIdentifier tableId) {
    return wrapper.planTableScan(tableId, SCAN_ALL, false, CredentialPrivilege.READ);
  }

  private CatalogWrapperForREST newWrapper(String catalogName, int batchSize) {
    return newWrapper(
        catalogName,
        ImmutableMap.of(IcebergConstants.SCAN_PLAN_TASK_BATCH_SIZE, String.valueOf(batchSize)));
  }

  private CatalogWrapperForREST newWrapper(String catalogName, Map<String, String> extraConfig) {
    Map<String, String> config = new HashMap<>();
    config.put(IcebergConstants.CATALOG_BACKEND, "memory");
    config.put(IcebergConstants.WAREHOUSE, warehouse.toString());
    config.putAll(extraConfig);
    return new CatalogWrapperForREST(catalogName, new IcebergConfig(config));
  }

  private TableIdentifier createTableWithDataFiles(
      CatalogWrapperForREST wrapper, String tableName, int dataFileCount) {
    Catalog catalog = wrapper.getCatalog();
    if (!((SupportsNamespaces) catalog).namespaceExists(NAMESPACE)) {
      ((SupportsNamespaces) catalog).createNamespace(NAMESPACE);
    }

    TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
    catalog.createTable(tableId, SCHEMA, PartitionSpec.unpartitioned(), Collections.emptyMap());
    for (int i = 0; i < dataFileCount; i++) {
      appendDataFile(wrapper, tableId, dataFileLocation(tableName, i));
    }
    return tableId;
  }

  /**
   * Appends one data file. The file itself is never read: scan planning only reads the manifests
   * that describe it.
   */
  private void appendDataFile(
      CatalogWrapperForREST wrapper, TableIdentifier tableId, String location) {
    Table table = wrapper.getCatalog().loadTable(tableId);
    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(location)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(1)
            .withFileSizeInBytes(1L)
            .build();
    table.newFastAppend().appendFile(dataFile).commit();
  }

  /**
   * Adds a position delete file covering {@code referencedDataFile}, making the table
   * merge-on-read.
   */
  private void appendPositionDeleteFile(
      CatalogWrapperForREST wrapper, TableIdentifier tableId, String referencedDataFile) {
    Table table = wrapper.getCatalog().loadTable(tableId);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(referencedDataFile + ".deletes")
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(1)
            .withFileSizeInBytes(1L)
            .withReferencedDataFile(referencedDataFile)
            .build();
    table.newRowDelta().addDeletes(deleteFile).commit();
  }

  private String dataFileLocation(String tableName, int index) {
    return warehouse.resolve(tableName + "-data-" + index + ".parquet").toUri().toString();
  }

  private List<String> expectedDataFileLocations(String tableName, int dataFileCount) {
    List<String> locations = new ArrayList<>();
    for (int i = 0; i < dataFileCount; i++) {
      locations.add(dataFileLocation(tableName, i));
    }
    return locations.stream().sorted().collect(Collectors.toList());
  }

  private static List<Long> dataSequenceNumbers(List<FileScanTask> fileScanTasks) {
    return fileScanTasks.stream()
        .map(task -> task.file().dataSequenceNumber())
        .collect(Collectors.toList());
  }

  private static List<String> dataFileLocations(List<FileScanTask> fileScanTasks) {
    return fileScanTasks.stream().map(task -> task.file().location()).collect(Collectors.toList());
  }
}
