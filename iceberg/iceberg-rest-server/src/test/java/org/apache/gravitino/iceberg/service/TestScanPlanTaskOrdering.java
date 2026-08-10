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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the order of {@code file-scan-tasks} in a scan planning response: an identical scan of an
 * identical snapshot must produce an identical response, whichever order Iceberg happened to plan
 * the manifests in.
 */
@SuppressWarnings("deprecation")
public class TestScanPlanTaskOrdering {

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final PlanTableScanRequest SCAN_ALL = PlanTableScanRequest.builder().build();

  @TempDir private Path warehouse;

  @Test
  void testTasksAreOrderedByDataFileLocation() {
    CatalogWrapperForREST wrapper = newWrapper("ordering");
    TableIdentifier tableId = createTable(wrapper, "tbl");
    // Appended in an order that does not match the sort order.
    appendDataFile(wrapper, tableId, dataFileLocation("c"));
    appendDataFile(wrapper, tableId, dataFileLocation("a"));
    appendDataFile(wrapper, tableId, dataFileLocation("b"));

    List<String> locations = dataFileLocations(planTableScan(wrapper, tableId));

    Assertions.assertEquals(
        ImmutableList.of(dataFileLocation("a"), dataFileLocation("b"), dataFileLocation("c")),
        locations);
  }

  @Test
  void testTasksOverTheSameFileAreOrderedByManifestEntry() {
    // Data file location, offset and length do not order tasks totally: appending one path twice
    // leaves two manifest entries that tie on all three, and only the entry itself tells them
    // apart. Without a further tie-break the two keep whichever order Iceberg planned them in.
    CatalogWrapperForREST wrapper = newWrapper("duplicate-path");
    TableIdentifier tableId = createTable(wrapper, "tbl");
    String duplicated = dataFileLocation("dup");
    appendDataFile(wrapper, tableId, duplicated);
    appendDataFile(wrapper, tableId, duplicated);

    List<FileScanTask> tasks = planTableScan(wrapper, tableId);

    Assertions.assertEquals(ImmutableList.of(duplicated, duplicated), dataFileLocations(tasks));
    Assertions.assertEquals(
        ImmutableList.of(1L, 2L),
        tasks.stream().map(task -> task.file().dataSequenceNumber()).collect(Collectors.toList()),
        "The older manifest entry of the duplicated file must come first");
  }

  @Test
  void testPlanningTheSameSnapshotTwiceReturnsTheSameOrder() {
    CatalogWrapperForREST wrapper = newWrapper("repeatable");
    TableIdentifier tableId = createTable(wrapper, "tbl");
    for (int i = 0; i < 5; i++) {
      appendDataFile(wrapper, tableId, dataFileLocation("f" + i));
    }

    Assertions.assertEquals(
        dataFileLocations(planTableScan(wrapper, tableId)),
        dataFileLocations(planTableScan(wrapper, tableId)));
  }

  private List<FileScanTask> planTableScan(CatalogWrapperForREST wrapper, TableIdentifier tableId) {
    PlanTableScanResponse response = wrapper.planTableScan(tableId, SCAN_ALL);
    return response.fileScanTasks();
  }

  private CatalogWrapperForREST newWrapper(String catalogName) {
    return new CatalogWrapperForREST(
        catalogName,
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                warehouse.toString())));
  }

  private TableIdentifier createTable(CatalogWrapperForREST wrapper, String tableName) {
    Catalog catalog = wrapper.getCatalog();
    ((SupportsNamespaces) catalog).createNamespace(NAMESPACE);
    TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
    catalog.createTable(tableId, SCHEMA, PartitionSpec.unpartitioned(), Collections.emptyMap());
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

  private String dataFileLocation(String name) {
    return warehouse.resolve(name + ".parquet").toUri().toString();
  }

  private static List<String> dataFileLocations(List<FileScanTask> fileScanTasks) {
    return fileScanTasks.stream().map(task -> task.file().location()).collect(Collectors.toList());
  }
}
