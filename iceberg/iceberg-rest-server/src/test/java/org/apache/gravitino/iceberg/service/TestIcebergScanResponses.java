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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Scan response tests shared by the standard and experimental Iceberg runtimes. */
public class TestIcebergScanResponses {

  private static final TableIdentifier IDENT = TableIdentifier.of("db", "table");

  @TempDir Path warehouse;

  private CatalogWrapperForREST wrapper;
  private Table table;

  @BeforeEach
  void setUp() {
    wrapper =
        new CatalogWrapperForREST(
            "scan-response-test",
            new IcebergConfig(
                Map.of(
                    IcebergConstants.CATALOG_BACKEND,
                    "memory",
                    IcebergConstants.WAREHOUSE,
                    warehouse.toString())));
    ((SupportsNamespaces) wrapper.getCatalog()).createNamespace(Namespace.of("db"));
    table =
        wrapper
            .getCatalog()
            .createTable(
                IDENT,
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (wrapper != null) {
      wrapper.close();
    }
  }

  @Test
  void testMissingScanMethodHasActionableError() {
    for (int attempt = 0; attempt < 2; attempt++) {
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> IcebergRESTUtils.resolveScanResponseMethod(Object.class, "specsById"));
      assertTrue(error.getMessage().contains("java.lang.Object.specsById"));
      assertTrue(error.getMessage().contains("supported Iceberg runtime"));
      assertTrue(error.getCause() instanceof NoSuchMethodException);
      // A failed lookup must not poison the utility class for unrelated REST operations.
      assertEquals(PlanStatus.COMPLETED, plan().planStatus());
    }
  }

  @Test
  void testPlanEmptyTable() throws IOException {
    PlanTableScanResponse response = plan();
    assertEquals(PlanStatus.COMPLETED, response.planStatus());
    assertTrue(response.fileScanTasks().isEmpty());
    assertEquals(table.specs().keySet(), IcebergRESTUtils.scanResponseSpecs(response).keySet());
    assertEquals(PlanStatus.COMPLETED, roundTrip(response).planStatus());
  }

  @Test
  void testPlanAndCopyCredentialsPreservesTasksAndHistoricalSpecs() throws IOException {
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(warehouse.resolve("data.parquet").toString())
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
    table.updateSpec().addField("id").commit();
    PlanTableScanResponse original = plan();
    assertEquals(1, original.fileScanTasks().size());
    assertEquals(2, IcebergRESTUtils.scanResponseSpecs(original).size());

    List<Credential> credentials =
        Collections.singletonList(
            ImmutableCredential.builder()
                .prefix(table.location())
                .config(Map.of("s3.access-key-id", "key"))
                .build());
    PlanTableScanResponse copied = IcebergRESTUtils.copyWithCredentials(original, credentials);
    assertEquals(original.planStatus(), copied.planStatus());
    assertEquals(original.fileScanTasks(), copied.fileScanTasks());
    assertEquals(
        IcebergRESTUtils.scanResponseSpecs(original), IcebergRESTUtils.scanResponseSpecs(copied));
    assertEquals(credentials, copied.credentials());
    assertTrue(original.credentials() == null || original.credentials().isEmpty());

    PlanTableScanResponse decoded = roundTrip(copied);
    assertEquals(1, decoded.fileScanTasks().size());
    assertEquals(table.specs().keySet(), IcebergRESTUtils.scanResponseSpecs(decoded).keySet());
    assertEquals(credentials.get(0).config(), decoded.credentials().get(0).config());
  }

  @Test
  void testRewriteScanCredentials() {
    PlanTableScanResponse original = plan();
    assertSame(original, IcebergRESTUtils.rewriteScanPlanCredentials("catalog", IDENT, original));
    Credential credential =
        ImmutableCredential.builder()
            .prefix(table.location())
            .config(
                Map.of(
                    "s3.access-key-id", "key",
                    "s3.session-token", "token",
                    "client.refresh-credentials-endpoint", "https://upstream/credentials"))
            .build();
    PlanTableScanResponse upstream =
        IcebergRESTUtils.copyWithCredentials(original, Collections.singletonList(credential));
    PlanTableScanResponse rewritten =
        IcebergRESTUtils.rewriteScanPlanCredentials("catalog", IDENT, upstream);
    assertEquals(1, rewritten.credentials().size());
    assertEquals(
        "v1/catalog/namespaces/db/tables/table/credentials",
        rewritten.credentials().get(0).config().get("client.refresh-credentials-endpoint"));
    assertEquals("key", rewritten.credentials().get(0).config().get("s3.access-key-id"));
    assertEquals(
        IcebergRESTUtils.scanResponseSpecs(original),
        IcebergRESTUtils.scanResponseSpecs(rewritten));
    assertFalse(
        upstream.credentials().get(0).config().equals(rewritten.credentials().get(0).config()));
  }

  @Test
  void testCopyPreservesPlanIdAndTasks() {
    PlanTableScanResponse original =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanId("plan-id")
            .withPlanTasks(Collections.singletonList("task"))
            .build();
    PlanTableScanResponse copied =
        IcebergRESTUtils.copyWithCredentials(original, Collections.emptyList());
    assertEquals(original.planStatus(), copied.planStatus());
    assertEquals(original.planId(), copied.planId());
    assertEquals(original.planTasks(), copied.planTasks());
  }

  @Test
  void testCopyPreservesError() {
    ErrorResponse error =
        ErrorResponse.builder().responseCode(500).withType("error").withMessage("failed").build();
    PlanTableScanResponse original =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.FAILED)
            .withErrorResponse(error)
            .build();
    PlanTableScanResponse copied =
        IcebergRESTUtils.copyWithCredentials(original, Collections.emptyList());
    assertEquals(original.planStatus(), copied.planStatus());
    assertSame(error, copied.errorResponse());
  }

  private PlanTableScanResponse plan() {
    return wrapper.planTableScan(IDENT, PlanTableScanRequest.builder().build());
  }

  private PlanTableScanResponse roundTrip(PlanTableScanResponse response) throws IOException {
    return PlanTableScanResponseParser.fromJson(
        IcebergObjectMapper.getInstance().readTree(PlanTableScanResponseParser.toJson(response)),
        table.specs(),
        true);
  }
}
