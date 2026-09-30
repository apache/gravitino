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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.listener.api.event.IcebergFetchScanTasksEvent;
import org.apache.gravitino.listener.api.event.IcebergFetchScanTasksPreEvent;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * End-to-end coverage of the two-step scan planning protocol over HTTP: {@code POST .../plan} hands
 * out {@code plan-tasks} and {@code POST .../tasks} exchanges each one for its scan tasks. The test
 * catalog is configured with a batch size of one so the three data files of a test table span three
 * batches.
 */
@SuppressWarnings("deprecation")
public class TestIcebergFetchScanTasksEndpoint extends IcebergNamespaceTestBase {

  private static final Namespace NAMESPACE = IcebergRestTestUtil.TEST_NAMESPACE_NAME;
  private static final String TABLE_NAME = "batched_scan_table";
  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.of(1, false, "foo_string", StringType.get()));

  private DummyEventListener eventListener;

  @Override
  protected Application configure() {
    eventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergTableOperations.class,
            true,
            Collections.singletonList(eventListener),
            ImmutableMap.of(IcebergConstants.SCAN_PLAN_TASK_BATCH_SIZE, "1"));
    resourceConfig.register(MockIcebergNamespaceOperations.class);

    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(mockRequest).to(HttpServletRequest.class);
          }
        });

    GravitinoAuthorizerProvider.getInstance().initialize(new ServerConfig());

    return resourceConfig;
  }

  @Test
  void testPlanHandsOutPlanTasksAndTasksEndpointReturnsThem() {
    createNamespaceAndTable();

    JsonNode plan = planTableScan();
    Assertions.assertEquals(PlanStatus.COMPLETED.status(), plan.get("status").asText());
    Assertions.assertEquals(
        1, plan.get("file-scan-tasks").size(), "Only one batch should be returned inline");
    Assertions.assertTrue(
        plan.has("plan-tasks"), "Tasks beyond the first batch must be offered as plan tasks");
    Assertions.assertEquals(2, plan.get("plan-tasks").size());

    List<String> dataFiles = new ArrayList<>(dataFilePaths(plan.get("file-scan-tasks")));
    eventListener.clearEvent();
    for (JsonNode planTask : plan.get("plan-tasks")) {
      JsonNode tasks;
      try (Response response = doFetchScanTasks(planTask.asText())) {
        Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
        tasks = response.readEntity(JsonNode.class);
      }
      Assertions.assertEquals(1, tasks.get("file-scan-tasks").size());
      Assertions.assertFalse(tasks.has("plan-tasks"));
      dataFiles.addAll(dataFilePaths(tasks.get("file-scan-tasks")));
      Assertions.assertInstanceOf(IcebergFetchScanTasksPreEvent.class, eventListener.popPreEvent());
      Assertions.assertInstanceOf(IcebergFetchScanTasksEvent.class, eventListener.popPostEvent());
      // HTTP retries must return the same batch, without consuming the plan task.
      try (Response retry = doFetchScanTasks(planTask.asText())) {
        Assertions.assertEquals(Status.OK.getStatusCode(), retry.getStatus());
        Assertions.assertEquals(tasks, retry.readEntity(JsonNode.class));
      }
      eventListener.clearEvent();
    }

    Assertions.assertEquals(
        3,
        dataFiles.stream().distinct().count(),
        "Every data file must be reachable across the plan and its plan tasks, but got: "
            + dataFiles);
  }

  @Test
  void testTasksEndpointRejectsAnUnknownPlanTask() {
    createNamespaceAndTable();

    try (Response response = doFetchScanTasks("not-a-plan-task")) {
      Assertions.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
      Assertions.assertEquals(
          "NoSuchPlanTaskException", response.readEntity(ErrorResponse.class).type());
    }
  }

  private JsonNode planTableScan() {
    try (Response response =
        getTableClientBuilder(NAMESPACE, Optional.of(TABLE_NAME + "/plan"))
            .post(
                Entity.entity(
                    PlanTableScanRequest.builder().build(), MediaType.APPLICATION_JSON_TYPE))) {
      Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
      return response.readEntity(JsonNode.class);
    }
  }

  private Response doFetchScanTasks(String planTask) {
    return getTableClientBuilder(NAMESPACE, Optional.of(TABLE_NAME + "/tasks"))
        .post(Entity.entity(new FetchScanTasksRequest(planTask), MediaType.APPLICATION_JSON_TYPE));
  }

  private void createNamespaceAndTable() {
    try (Response namespaceResponse =
        getNamespaceClientBuilder()
            .post(
                Entity.entity(
                    CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build(),
                    MediaType.APPLICATION_JSON_TYPE))) {
      Assertions.assertEquals(Status.OK.getStatusCode(), namespaceResponse.getStatus());
    }

    // The test catalog appends three data files when this property is set.
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(TABLE_NAME)
            .withSchema(TABLE_SCHEMA)
            .setProperties(
                ImmutableMap.of(
                    CatalogWrapperForTest.GENERATE_PLAN_TASKS_DATA_PROP, Boolean.TRUE.toString()))
            .build();
    try (Response tableResponse =
        getTableClientBuilder(NAMESPACE, Optional.empty())
            .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE))) {
      Assertions.assertEquals(Status.OK.getStatusCode(), tableResponse.getStatus());
    }
  }

  private static List<String> dataFilePaths(JsonNode fileScanTasks) {
    List<String> paths = new ArrayList<>();
    for (JsonNode fileScanTask : fileScanTasks) {
      paths.add(fileScanTask.get("data-file").get("file-path").asText());
    }
    return paths;
  }
}
