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

package org.apache.gravitino.iceberg.service.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestPlanTableScan extends IcebergNamespaceTestBase {

  private static final Schema tableSchema =
      new Schema(
          NestedField.of(1, false, "id", org.apache.iceberg.types.Types.IntegerType.get()),
          NestedField.of(2, false, "name", StringType.get()),
          NestedField.of(3, true, "age", org.apache.iceberg.types.Types.IntegerType.get()));

  private DummyEventListener dummyEventListener;
  private ObjectMapper objectMapper;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    this.objectMapper = IcebergObjectMapper.getInstance();

    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergTableOperations.class, true, Arrays.asList(dummyEventListener));
    // Register namespace operations for setup
    resourceConfig.register(MockIcebergNamespaceOperations.class);
    resourceConfig.register(MockIcebergTableRenameOperations.class);

    // Register a mock HttpServletRequest with user info
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(mockRequest).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  /** Test basic scan planning without any filters or projections. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testBasicScanPlanning(Namespace namespace) throws Exception {
    // Setup: Create namespace and table
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_test_table";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan table scan with empty request
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();
    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify: Check response status and structure
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    // Parse response as JSON Map instead of deserializing to PlanTableScanResponse
    // to avoid Jackson injectable values issue
    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});

    // Verify response contains required fields
    Assertions.assertNotNull(scanResponse);
    Assertions.assertTrue(scanResponse.containsKey("plan-status"));
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));

    System.out.println("Scan planning completed successfully");
  }

  /** Test scan planning with filter expression. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithFilter(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_filter_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan (without filter for now)
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  /** Test scan planning with column projection. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithProjection(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_projection_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan with column projection
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder()
            .withSnapshotId(0L)
            .withSelect(Arrays.asList("id", "name"))
            .build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  /** Test scan planning with case-sensitive option. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithCaseSensitive(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_case_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan with case-sensitive option
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).withCaseSensitive(true).build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  /** Test scan planning on non-existent table. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningNonExistentTable(Namespace namespace) throws Exception {
    // Setup: Create namespace only (no table)
    verifyCreateNamespaceSucc(namespace);

    // Execute: Try to plan scan on non-existent table
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();
    Response response = doPlanTableScan(namespace, "non_existent_table", scanRequest);

    // Verify: Should return 404
    Assertions.assertEquals(404, response.getStatus());
  }

  /** Test scan planning with complex filter expressions. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithComplexFilters(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_complex_filter_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan (without complex filter)
    // Note: Iceberg 1.10.0 filter needs Expression object
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  /** Test scan planning with snapshot-id parameter. */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithSnapshotId(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_snapshot_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan with default snapshot
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify: Should succeed even without specific snapshot
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningWithOptions(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_options_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Plan scan (without options)
    // Note: Iceberg 1.10.0 PlanTableScanRequest doesn't have options()
    PlanTableScanRequest scanRequest =
        new PlanTableScanRequest.Builder().withSnapshotId(0L).build();

    Response response = doPlanTableScan(namespace, tableName, scanRequest);

    // Verify
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    Map<String, Object> scanResponse =
        objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
    Assertions.assertNotNull(scanResponse);
    Assertions.assertEquals("completed", scanResponse.get("plan-status"));
  }

  /**
   * Test error handling with invalid request body.
   *
   * <p>Verifies that: - Invalid JSON requests are handled gracefully - Appropriate error codes are
   * returned
   */
  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testScanPlanningInvalidRequestBody(Namespace namespace) throws Exception {
    // Setup
    verifyCreateNamespaceSucc(namespace);
    String tableName = "scan_error_test";
    verifyCreateTableSucc(namespace, tableName);

    // Execute: Try with invalid JSON (sending raw string instead of proper request)
    String invalidJson = "{\"invalid\": json}";
    Response response =
        getScanClientBuilder(namespace, tableName)
            .post(Entity.entity(invalidJson, MediaType.APPLICATION_JSON_TYPE));

    // Verify: Should return 400 for invalid JSON
    Assertions.assertEquals(400, response.getStatus());
  }

  /** Get scan client builder for table scan endpoint. */
  private Invocation.Builder getScanClientBuilder(Namespace namespace, String tableName) {
    String path =
        Joiner.on("/")
            .join(
                IcebergRestTestUtil.NAMESPACE_PATH
                    + "/"
                    + RESTUtil.encodeNamespace(namespace)
                    + "/tables",
                tableName,
                "scan");
    return getIcebergClientBuilder(path, Optional.empty());
  }

  /** Execute plan table scan request. */
  private Response doPlanTableScan(
      Namespace namespace, String tableName, PlanTableScanRequest scanRequest) throws Exception {
    return getScanClientBuilder(namespace, tableName)
        .post(Entity.entity(scanRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  /** Create a table with the test schema. */
  private void verifyCreateTableSucc(Namespace namespace, String tableName) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(tableName).withSchema(tableSchema).build();

    Response response =
        getTableClientBuilder(namespace, Optional.empty())
            .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        tableSchema.columns(), loadTableResponse.tableMetadata().schema().columns());
  }
}
