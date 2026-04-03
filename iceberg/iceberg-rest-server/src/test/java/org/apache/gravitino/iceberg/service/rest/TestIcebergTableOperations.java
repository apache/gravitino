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
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.iceberg.service.extension.DummyCredentialProvider;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.IcebergCreateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTableEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergListTableEvent;
import org.apache.gravitino.listener.api.event.IcebergListTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergListTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergPlanTableScanEvent;
import org.apache.gravitino.listener.api.event.IcebergPlanTableScanFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergPlanTableScanPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTableEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePreEvent;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.util.JsonUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestIcebergTableOperations extends IcebergNamespaceTestBase {

  private static final Schema tableSchema =
      new Schema(NestedField.of(1, false, "foo_string", StringType.get()));

  private static final Schema newTableSchema =
      new Schema(NestedField.of(2, false, "foo_string1", StringType.get()));

  private DummyEventListener dummyEventListener;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergTableOperations.class, true, Arrays.asList(dummyEventListener));
    // create namespace before each table test
    resourceConfig.register(MockIcebergNamespaceOperations.class);
    resourceConfig.register(MockIcebergTableRenameOperations.class);

    // register a mock HttpServletRequest with user info
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(mockRequest).to(HttpServletRequest.class);
          }
        });

    GravitinoAuthorizerProvider provider = GravitinoAuthorizerProvider.getInstance();
    provider.initialize(new ServerConfig());

    return resourceConfig;
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateTable(Namespace namespace) {
    verifyCreateTableFail(namespace, "create_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCreateTableFailureEvent);

    verifyCreateNamespaceSucc(namespace);

    verifyCreateTableSucc(namespace, "create_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergCreateTableEvent);

    verifyCreateTableFail(namespace, "create_foo1", 409);
    verifyCreateTableFail(namespace, "", 400);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTable(Namespace namespace) {
    verifyLoadTableFail(namespace, "load_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergLoadTableFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "load_foo1");

    dummyEventListener.clearEvent();
    verifyLoadTableSucc(namespace, "load_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergLoadTableEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testPlanTableScan(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "plan_scan_table", true);

    dummyEventListener.clearEvent();
    TableMetadata metadata = getTableMeta(namespace, "plan_scan_table");
    Long snapshotId =
        metadata.currentSnapshot() == null ? null : metadata.currentSnapshot().snapshotId();
    JsonNode planResponse = verifyPlanTableScanSucc(namespace, "plan_scan_table", snapshotId);

    Assertions.assertEquals(PlanStatus.COMPLETED.status(), planResponse.get("status").asText());
    Assertions.assertTrue(planResponse.has("plan-tasks"));
    Assertions.assertTrue(planResponse.get("plan-tasks").isArray());
    Assertions.assertTrue(planResponse.get("plan-tasks").size() > 0);

    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergPlanTableScanPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergPlanTableScanEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testPlanTableScanTableNotFound(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    dummyEventListener.clearEvent();

    verifyPlanTableScanFail(namespace, "missing_table", 404);

    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergPlanTableScanPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergPlanTableScanFailureEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testPlanTableScanWithIncrementalAppendScanValidRange(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "incremental_scan_valid_table", true);

    dummyEventListener.clearEvent();
    TableMetadata metadata = getTableMeta(namespace, "incremental_scan_valid_table");

    // generatePlanData=true creates two snapshots: one from table creation, one from
    // appendSampleData
    List<Snapshot> snapshots = metadata.snapshots();
    Assertions.assertNotNull(snapshots, "Snapshots should not be null");
    Assertions.assertTrue(snapshots.size() >= 2, "Should have at least 2 snapshots");

    Snapshot endSnapshot = snapshots.get(snapshots.size() - 1);
    Long endSnapshotId = endSnapshot.snapshotId();
    Long startSnapshotId = endSnapshot.parentId();
    Assertions.assertNotNull(
        startSnapshotId, "End snapshot should have a parent snapshot for incremental scan");

    JsonNode planResponse =
        verifyPlanTableScanSuccWithRange(
            namespace, "incremental_scan_valid_table", startSnapshotId, endSnapshotId);
    Assertions.assertEquals(PlanStatus.COMPLETED.status(), planResponse.get("status").asText());
    Assertions.assertTrue(planResponse.has("plan-tasks"));
    Assertions.assertTrue(planResponse.get("plan-tasks").isArray());

    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergPlanTableScanPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergPlanTableScanEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testDropTable(Namespace namespace) {
    verifyDropTableFail(namespace, "drop_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergDropTableFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyDropTableFail(namespace, "drop_foo1", 404);

    verifyCreateTableSucc(namespace, "drop_foo1");

    dummyEventListener.clearEvent();
    verifyDropTableSucc(namespace, "drop_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergDropTableEvent);

    verifyLoadTableFail(namespace, "drop_foo1", 404);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testUpdateTable(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "update_foo1");
    TableMetadata metadata = getTableMeta(namespace, "update_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateSucc(namespace, "update_foo1", metadata);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergUpdateTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergUpdateTableEvent);

    verifyDropTableSucc(namespace, "update_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateTableFail(namespace, "update_foo1", 404, metadata);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergUpdateTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergUpdateTableFailureEvent);

    verifyDropNamespaceSucc(namespace);
    verifyUpdateTableFail(namespace, "update_foo1", 404, metadata);
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testListTables(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    verifyListTableFail(namespace, 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergListTableFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "list_foo1");
    verifyCreateTableSucc(namespace, "list_foo2");

    dummyEventListener.clearEvent();
    verifyListTableSucc(namespace, ImmutableSet.of("list_foo1", "list_foo2"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergListTableEvent);
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testListTablesWithPagination(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "page_t1");
    verifyCreateTableSucc(namespace, "page_t2");
    verifyCreateTableSucc(namespace, "page_t3");

    dummyEventListener.clearEvent();

    // First page: pageSize=2
    String tablePath =
        IcebergRestTestUtil.NAMESPACE_PATH + "/" + RESTUtil.encodeNamespace(namespace) + "/tables";
    Response firstPageResponse =
        getIcebergClientBuilder(tablePath, Optional.of(ImmutableMap.of("pageSize", "2"))).get();
    Assertions.assertEquals(Status.OK.getStatusCode(), firstPageResponse.getStatus());
    ListTablesResponse firstPage = firstPageResponse.readEntity(ListTablesResponse.class);
    Assertions.assertEquals(2, firstPage.identifiers().size());
    Assertions.assertNotNull(firstPage.nextPageToken());

    // Second page using nextPageToken
    Response secondPageResponse =
        getIcebergClientBuilder(
                tablePath,
                Optional.of(
                    ImmutableMap.of("pageToken", firstPage.nextPageToken(), "pageSize", "2")))
            .get();
    Assertions.assertEquals(Status.OK.getStatusCode(), secondPageResponse.getStatus());
    ListTablesResponse secondPage = secondPageResponse.readEntity(ListTablesResponse.class);
    Assertions.assertEquals(1, secondPage.identifiers().size());
    Assertions.assertNull(secondPage.nextPageToken());

    // Verify combined results
    Set<String> paginatedNames =
        java.util.stream.Stream.concat(
                firstPage.identifiers().stream(), secondPage.identifiers().stream())
            .map(id -> id.name())
            .collect(Collectors.toSet());
    Assertions.assertEquals(ImmutableSet.of("page_t1", "page_t2", "page_t3"), paginatedNames);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testTableExits(Namespace namespace) {
    verifyTableExistsStatusCode(namespace, "exists_foo2", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergTableExistsPreEvent);
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergTableExistsEvent);
    Assertions.assertEquals(false, ((IcebergTableExistsEvent) postEvent).isExists());

    verifyCreateNamespaceSucc(namespace);
    verifyTableExistsStatusCode(namespace, "exists_foo2", 404);

    verifyCreateTableSucc(namespace, "exists_foo1");
    dummyEventListener.clearEvent();
    verifyTableExistsStatusCode(namespace, "exists_foo1", 204);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergTableExistsPreEvent);
    postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergTableExistsEvent);
    Assertions.assertEquals(true, ((IcebergTableExistsEvent) postEvent).isExists());

    verifyLoadTableSucc(namespace, "exists_foo1");
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testRenameTable(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    // namespace not exits
    verifyRenameTableFail(namespace, "rename_foo1", "rename_foo3", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergRenameTableFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "rename_foo1");

    dummyEventListener.clearEvent();
    // rename
    verifyRenameTableSucc(namespace, "rename_foo1", "rename_foo2");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergRenameTableEvent);

    verifyLoadTableFail(namespace, "rename_foo1", 404);
    verifyLoadTableSucc(namespace, "rename_foo2");

    // source table not exists
    verifyRenameTableFail(namespace, "rename_foo1", "rename_foo3", 404);

    // dest table exists
    verifyCreateTableSucc(namespace, "rename_foo3");
    verifyRenameTableFail(namespace, "rename_foo2", "rename_foo3", 409);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testReportTableMetrics(Namespace namespace) {

    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "metrics_foo1");

    ImmutableCommitMetricsResult commitMetrics = ImmutableCommitMetricsResult.builder().build();
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("metrics_foo1")
            .snapshotId(-1)
            .sequenceNumber(-1)
            .operation("append")
            .commitMetrics(commitMetrics)
            .build();
    ReportMetricsRequest request = ReportMetricsRequest.of(commitReport);
    Response response =
        getReportMetricsClientBuilder("metrics_foo1", namespace)
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateTableWithCredentialVending(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);

    // create the table without credential vending
    Response response = doCreateTable(namespace, "create_without_credential_vending");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    // create the table with credential vending
    String tableName = "create_with_credential_vending";
    String localLocation = "file:///tmp/" + tableName;
    response = doCreateTableWithCredentialVending(namespace, tableName, localLocation);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    String s3TableName = "create_with_credential_vending_s3";
    String s3Location = "s3://dummy-bucket/" + s3TableName;
    response = doCreateTableWithCredentialVending(namespace, s3TableName, s3Location);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse s3LoadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        s3LoadTableResponse.config().get(Credential.CREDENTIAL_TYPE));

    // load the table without credential vending
    response = doLoadTable(namespace, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    // load the table with credential vending
    response = doLoadTableWithCredentialVending(namespace, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    response = doLoadTableWithCredentialVending(namespace, s3TableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        loadTableResponse.config().get(Credential.CREDENTIAL_TYPE));
  }

  private Response doCreateTableWithCredentialVending(Namespace ns, String name, String location) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(name)
            .withSchema(tableSchema)
            .withLocation(location)
            .build();
    return getTableClientBuilder(ns, Optional.empty())
        .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "vended-credentials")
        .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doCreateTable(Namespace ns, String name) {
    return doCreateTable(ns, name, false);
  }

  private Response doCreateTable(Namespace ns, String name, boolean generatePlanData) {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder().withName(name).withSchema(tableSchema);
    if (generatePlanData) {
      builder =
          builder.setProperties(
              ImmutableMap.of(
                  CatalogWrapperForTest.GENERATE_PLAN_TASKS_DATA_PROP, Boolean.TRUE.toString()));
    }
    CreateTableRequest createTableRequest = builder.build();
    return getTableClientBuilder(ns, Optional.empty())
        .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doRenameTable(Namespace ns, String source, String dest) {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of(ns, source))
            .withDestination(TableIdentifier.of(ns, dest))
            .build();
    return getRenameTableClientBuilder()
        .post(Entity.entity(renameTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doListTable(Namespace ns) {
    return getTableClientBuilder(ns, Optional.empty()).get();
  }

  private Response doDropTable(Namespace ns, String name) {
    return getTableClientBuilder(ns, Optional.of(name)).delete();
  }

  private Response doTableExists(Namespace ns, String name) {
    return getTableClientBuilder(ns, Optional.of(name)).head();
  }

  private Response doLoadTableWithCredentialVending(Namespace ns, String name) {
    return getTableClientBuilder(ns, Optional.of(name))
        .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "vended-credentials")
        .get();
  }

  private Response doLoadTable(Namespace ns, String name) {
    return getTableClientBuilder(ns, Optional.of(name)).get();
  }

  private Response doLoadTableWithSnapshots(Namespace ns, String name, String snapshots) {
    String path =
        IcebergRestTestUtil.NAMESPACE_PATH + "/" + RESTUtil.encodeNamespace(ns) + "/tables/" + name;
    Map<String, String> queryParams = ImmutableMap.of("snapshots", snapshots);
    return getIcebergClientBuilder(path, Optional.of(queryParams)).get();
  }

  private Response doPlanTableScan(Namespace ns, String tableName, PlanTableScanRequest request) {
    Invocation.Builder builder = getTableClientBuilder(ns, Optional.of(tableName + "/scan"));
    return builder.post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doUpdateTable(Namespace ns, String name, TableMetadata base) {
    TableMetadata newMetadata = base.updateSchema(newTableSchema);
    List<MetadataUpdate> metadataUpdates = newMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, metadataUpdates);
    UpdateTableRequest updateTableRequest = new UpdateTableRequest(requirements, metadataUpdates);
    return getTableClientBuilder(ns, Optional.of(name))
        .post(Entity.entity(updateTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private TableMetadata getTableMeta(Namespace ns, String tableName) {
    Response response = doLoadTable(ns, tableName);
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    return loadTableResponse.tableMetadata();
  }

  private void verifyUpdateTableFail(Namespace ns, String name, int status, TableMetadata base) {
    Response response = doUpdateTable(ns, name, base);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyUpdateSucc(Namespace ns, String name, TableMetadata base) {
    Response response = doUpdateTable(ns, name, base);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        newTableSchema.columns(), loadTableResponse.tableMetadata().schema().columns());
  }

  private void verifyLoadTableFail(Namespace ns, String name, int status) {
    Response response = doLoadTable(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyLoadTableSucc(Namespace ns, String name) {
    Response response = doLoadTable(ns, name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        tableSchema.columns(), loadTableResponse.tableMetadata().schema().columns());
  }

  private void verifyDropTableSucc(Namespace ns, String name) {
    Response response = doDropTable(ns, name);
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private void verifyTableExistsStatusCode(Namespace ns, String name, int status) {
    Response response = doTableExists(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyPlanTableScanFail(Namespace ns, String tableName, int status) {
    Response response = doPlanTableScan(ns, tableName, buildPlanTableScanRequest(null));
    Assertions.assertEquals(status, response.getStatus());
  }

  private JsonNode verifyPlanTableScanSucc(Namespace ns, String tableName, Long snapshotId) {
    Response response = doPlanTableScan(ns, tableName, buildPlanTableScanRequest(snapshotId));
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    try {
      return JsonUtil.mapper().readTree(responseBody);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse plan table scan response", e);
    }
  }

  private JsonNode verifyPlanTableScanSuccWithRange(
      Namespace ns, String tableName, Long startSnapshotId, Long endSnapshotId) {
    Response response =
        doPlanTableScan(
            ns, tableName, buildPlanTableScanRequestWithRange(startSnapshotId, endSnapshotId));
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String responseBody = response.readEntity(String.class);
    try {
      return JsonUtil.mapper().readTree(responseBody);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse plan table scan response", e);
    }
  }

  private PlanTableScanRequest buildPlanTableScanRequest(Long snapshotId) {
    PlanTableScanRequest.Builder builder = PlanTableScanRequest.builder();
    if (snapshotId != null) {
      builder = builder.withSnapshotId(snapshotId);
    } else {
      builder = builder.withSnapshotId(0L);
    }
    return builder.build();
  }

  private PlanTableScanRequest buildPlanTableScanRequestWithRange(
      Long startSnapshotId, Long endSnapshotId) {
    PlanTableScanRequest.Builder builder = PlanTableScanRequest.builder();
    if (startSnapshotId != null) {
      builder = builder.withStartSnapshotId(startSnapshotId);
    }
    if (endSnapshotId != null) {
      builder = builder.withEndSnapshotId(endSnapshotId);
    }
    return builder.build();
  }

  private void verifyDropTableFail(Namespace ns, String name, int status) {
    Response response = doDropTable(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyCreateTableSucc(Namespace ns, String name) {
    verifyCreateTableSucc(ns, name, false);
  }

  private void verifyCreateTableSucc(Namespace ns, String name, boolean generatePlanData) {
    Response response = doCreateTable(ns, name, generatePlanData);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Schema schema = loadTableResponse.tableMetadata().schema();
    Assertions.assertEquals(schema.columns(), tableSchema.columns());
  }

  private void verifyRenameTableSucc(Namespace ns, String source, String dest) {
    Response response = doRenameTable(ns, source, dest);
    System.out.println(response);
    System.out.flush();
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private void verifyRenameTableFail(Namespace ns, String source, String dest, int status) {
    Response response = doRenameTable(ns, source, dest);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyListTableFail(Namespace ns, int status) {
    Response response = doListTable(ns);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyListTableSucc(Namespace ns, Set<String> expectedTableNames) {
    Response response = doListTable(ns);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListTablesResponse listTablesResponse = response.readEntity(ListTablesResponse.class);
    Set<String> tableNames =
        listTablesResponse.identifiers().stream()
            .map(identifier -> identifier.name())
            .collect(Collectors.toSet());
    Assertions.assertEquals(expectedTableNames, tableNames);
  }

  private void verifyCreateTableFail(Namespace ns, String name, int status) {
    Response response = doCreateTable(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  public void testGetTableCredentials(Namespace ns) {
    String tableName = "test_table_credentials";

    // First create the namespace
    verifyCreateNamespaceSucc(ns);

    // Then create a table
    verifyCreateTableSucc(ns, tableName);

    // Then test getting credentials
    Response response = doGetTableCredentials(ns, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    // Verify the response contains credentials (even if empty)
    String responseBody = response.readEntity(String.class);
    Assertions.assertNotNull(responseBody);
    Assertions.assertTrue(responseBody.contains("credentials"));
  }

  private Response doGetTableCredentials(Namespace ns, String tableName) {
    return getTableClientBuilder(ns, Optional.of(tableName + "/credentials")).get();
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testRemoteSigningNotSupported(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);

    // Attempt to create table with "remote-signing" access delegation
    // This should fail with UnsupportedOperationException -> 406 Not Acceptable
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName("test_remote_signing")
            .withSchema(tableSchema)
            .build();

    Response response =
        getTableClientBuilder(namespace, Optional.empty())
            .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "remote-signing")
            .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(406, response.getStatus());
    String errorBody = response.readEntity(String.class);
    Assertions.assertTrue(
        errorBody.contains("remote signing") || errorBody.contains("remote-signing"),
        "Error message should mention remote signing: " + errorBody);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testTableOperationsWithEncodedName(Namespace namespace) {
    // Table names with special characters that require percent-encoding in URL paths.
    // RESTUtil.encodeString encodes them for the URL, and the server should decode
    // them back via RESTUtil.decodeString thanks to @Encoded on the path parameter.
    String[] specialNames = {"table.with.dots", "table@special"};

    verifyCreateNamespaceSucc(namespace);

    for (String originalName : specialNames) {
      String encodedName = RESTUtil.encodeString(originalName);

      // Create uses the original name in the request body, not in the URL path
      verifyCreateTableSucc(namespace, originalName);

      // Load, exists use the encoded name in the URL path
      verifyLoadTableSucc(namespace, encodedName);
      verifyTableExistsStatusCode(namespace, encodedName, 204);

      // Update: load metadata with encoded path, then update with encoded path
      TableMetadata metadata = getTableMeta(namespace, encodedName);
      verifyUpdateSucc(namespace, encodedName, metadata);
    }

    // Verify list returns the original (decoded) table names
    verifyListTableSucc(namespace, ImmutableSet.copyOf(specialNames));

    for (String originalName : specialNames) {
      verifyDropTableSucc(namespace, RESTUtil.encodeString(originalName));
    }
    verifyDropNamespaceSucc(namespace);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testInvalidAccessDelegation(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);

    // Attempt to create table with invalid access delegation value
    // This should fail with IllegalArgumentException -> 400 Bad Request
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName("test_invalid_delegation")
            .withSchema(tableSchema)
            .build();

    Response response =
        getTableClientBuilder(namespace, Optional.empty())
            .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "invalid-value")
            .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(400, response.getStatus());
    String errorBody = response.readEntity(String.class);
    Assertions.assertTrue(
        errorBody.contains("vended-credentials") && errorBody.contains("illegal"),
        "Error message should mention valid values: " + errorBody);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateTableReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    Response response = doCreateTable(namespace, "create_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in create table response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateTableETagMatchesLoadTableETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    Response createResponse = doCreateTable(namespace, "create_load_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), createResponse.getStatus());
    String createEtag = createResponse.getHeaderString("ETag");
    Assertions.assertNotNull(createEtag, "ETag should be present in create response");

    // Load the same table with default snapshots — ETag should match
    Response loadResponse = doLoadTable(namespace, "create_load_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), loadResponse.getStatus());
    String loadEtag = loadResponse.getHeaderString("ETag");
    Assertions.assertNotNull(loadEtag, "ETag should be present in load response");

    Assertions.assertEquals(
        createEtag, loadEtag, "ETag from createTable should match ETag from default loadTable");

    // The create ETag should be reusable for If-None-Match on a subsequent loadTable
    Response conditionalResponse =
        getTableClientBuilder(namespace, Optional.of("create_load_etag_foo1"))
            .header(IcebergTableOperations.IF_NONE_MATCH, createEtag)
            .get();
    Assertions.assertEquals(
        Status.NOT_MODIFIED.getStatusCode(),
        conditionalResponse.getStatus(),
        "Create ETag should produce 304 on subsequent unchanged loadTable");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testUpdateTableETagMatchesLoadTableETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "update_load_etag_foo1");
    TableMetadata metadata = getTableMeta(namespace, "update_load_etag_foo1");
    Response updateResponse = doUpdateTable(namespace, "update_load_etag_foo1", metadata);
    Assertions.assertEquals(Status.OK.getStatusCode(), updateResponse.getStatus());
    String updateEtag = updateResponse.getHeaderString("ETag");
    Assertions.assertNotNull(updateEtag, "ETag should be present in update response");

    // Load the same table — ETag should match
    Response loadResponse = doLoadTable(namespace, "update_load_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), loadResponse.getStatus());
    String loadEtag = loadResponse.getHeaderString("ETag");

    Assertions.assertEquals(
        updateEtag, loadEtag, "ETag from updateTable should match ETag from default loadTable");

    // The update ETag should be reusable for If-None-Match
    Response conditionalResponse =
        getTableClientBuilder(namespace, Optional.of("update_load_etag_foo1"))
            .header(IcebergTableOperations.IF_NONE_MATCH, updateEtag)
            .get();
    Assertions.assertEquals(
        Status.NOT_MODIFIED.getStatusCode(),
        conditionalResponse.getStatus(),
        "Update ETag should produce 304 on subsequent unchanged loadTable");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testUpdateTableReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "update_etag_foo1");
    TableMetadata metadata = getTableMeta(namespace, "update_etag_foo1");
    Response response = doUpdateTable(namespace, "update_etag_foo1", metadata);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in update table response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_foo1");

    // Load the table and verify ETag header is present
    Response response = doLoadTable(namespace, "etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in load table response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableReturns304WhenETagMatches(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_304_foo1");

    // First, load the table to get the ETag
    Response firstResponse = doLoadTable(namespace, "etag_304_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), firstResponse.getStatus());
    String etag = firstResponse.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present");

    // Second, load the table with If-None-Match header set to the ETag
    Response secondResponse =
        getTableClientBuilder(namespace, Optional.of("etag_304_foo1"))
            .header(IcebergTableOperations.IF_NONE_MATCH, etag)
            .get();
    Assertions.assertEquals(
        Status.NOT_MODIFIED.getStatusCode(),
        secondResponse.getStatus(),
        "Should return 304 when ETag matches");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableReturns200WhenETagDoesNotMatch(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_mismatch_foo1");

    // Load with a non-matching If-None-Match header
    Response response =
        getTableClientBuilder(namespace, Optional.of("etag_mismatch_foo1"))
            .header(IcebergTableOperations.IF_NONE_MATCH, "\"non-matching-etag-value\"")
            .get();
    Assertions.assertEquals(
        Status.OK.getStatusCode(),
        response.getStatus(),
        "Should return 200 when ETag does not match");
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableETagChangesAfterUpdate(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_update_foo1");

    // Load the table and get the initial ETag
    Response firstResponse = doLoadTable(namespace, "etag_update_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), firstResponse.getStatus());
    String firstEtag = firstResponse.getHeaderString("ETag");
    Assertions.assertNotNull(firstEtag, "ETag header should be present");

    // Update the table
    TableMetadata metadata = getTableMeta(namespace, "etag_update_foo1");
    verifyUpdateSucc(namespace, "etag_update_foo1", metadata);

    // Load the table again and verify the ETag has changed
    Response secondResponse = doLoadTable(namespace, "etag_update_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), secondResponse.getStatus());
    String secondEtag = secondResponse.getHeaderString("ETag");
    Assertions.assertNotNull(secondEtag, "ETag header should be present after update");
    Assertions.assertNotEquals(
        firstEtag, secondEtag, "ETag should change after table metadata is updated");

    // Verify old ETag no longer returns 304
    Response thirdResponse =
        getTableClientBuilder(namespace, Optional.of("etag_update_foo1"))
            .header(IcebergTableOperations.IF_NONE_MATCH, firstEtag)
            .get();
    Assertions.assertEquals(
        Status.OK.getStatusCode(),
        thirdResponse.getStatus(),
        "Old ETag should not return 304 after table update");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableETagConsistency(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_consistent_foo1");

    // Load the table twice and verify the ETag is the same
    Response firstResponse = doLoadTable(namespace, "etag_consistent_foo1");
    String firstEtag = firstResponse.getHeaderString("ETag");

    Response secondResponse = doLoadTable(namespace, "etag_consistent_foo1");
    String secondEtag = secondResponse.getHeaderString("ETag");

    Assertions.assertEquals(
        firstEtag, secondEtag, "ETag should be consistent for the same table metadata");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableETagDiffersForSnapshotsParam(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "etag_snapshots_foo1");

    // Load with snapshots=all (default)
    Response allResponse = doLoadTableWithSnapshots(namespace, "etag_snapshots_foo1", "all");
    Assertions.assertEquals(Status.OK.getStatusCode(), allResponse.getStatus());
    String allEtag = allResponse.getHeaderString("ETag");
    Assertions.assertNotNull(allEtag, "ETag header should be present for snapshots=all");

    // Load with snapshots=refs
    Response refsResponse = doLoadTableWithSnapshots(namespace, "etag_snapshots_foo1", "refs");
    Assertions.assertEquals(Status.OK.getStatusCode(), refsResponse.getStatus());
    String refsEtag = refsResponse.getHeaderString("ETag");
    Assertions.assertNotNull(refsEtag, "ETag header should be present for snapshots=refs");

    // ETags should differ for different snapshots values
    Assertions.assertNotEquals(
        allEtag,
        refsEtag,
        "ETags should be distinct for different snapshots query parameter values");

    // Loading with the same snapshots value should produce the same ETag
    Response allResponse2 = doLoadTableWithSnapshots(namespace, "etag_snapshots_foo1", "all");
    String allEtag2 = allResponse2.getHeaderString("ETag");
    Assertions.assertEquals(
        allEtag, allEtag2, "ETag should be consistent for the same snapshots value");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableSnapshotsRefsFiltering(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    // Create table with data; generatePlanData=true produces two snapshots (create + append),
    // but only the latest snapshot is referenced by the "main" branch.
    verifyCreateTableSucc(namespace, "snapshots_refs_foo1", true);

    // Load with snapshots=all: should return all snapshots
    Response allResponse = doLoadTableWithSnapshots(namespace, "snapshots_refs_foo1", "all");
    Assertions.assertEquals(Status.OK.getStatusCode(), allResponse.getStatus());
    LoadTableResponse allTableResponse = allResponse.readEntity(LoadTableResponse.class);
    List<Snapshot> allSnapshots = allTableResponse.tableMetadata().snapshots();
    Assertions.assertTrue(
        allSnapshots.size() >= 2,
        "Table with data should have at least 2 snapshots, got " + allSnapshots.size());

    // Collect snapshot IDs referenced by refs
    Map<String, SnapshotRef> refs = allTableResponse.tableMetadata().refs();
    Set<Long> referencedSnapshotIds =
        refs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());

    // Load with snapshots=refs: should return only ref-referenced snapshots
    Response refsResponse = doLoadTableWithSnapshots(namespace, "snapshots_refs_foo1", "refs");
    Assertions.assertEquals(Status.OK.getStatusCode(), refsResponse.getStatus());
    LoadTableResponse refsTableResponse = refsResponse.readEntity(LoadTableResponse.class);
    List<Snapshot> refsSnapshots = refsTableResponse.tableMetadata().snapshots();

    // The returned snapshots should be exactly the ref-referenced snapshots
    Assertions.assertEquals(
        referencedSnapshotIds,
        refsSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet()),
        "snapshots=refs should return exactly the ref-referenced snapshots");

    // Refs should be preserved in the filtered response
    Assertions.assertEquals(
        refs.keySet(),
        refsTableResponse.tableMetadata().refs().keySet(),
        "Refs should be preserved in filtered response");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadTableSnapshotsAllReturnsAllSnapshots(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateTableSucc(namespace, "snapshots_all_foo1", true);

    // Load with default snapshots=all
    Response defaultResponse = doLoadTable(namespace, "snapshots_all_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), defaultResponse.getStatus());
    LoadTableResponse defaultTableResponse = defaultResponse.readEntity(LoadTableResponse.class);

    // Load with explicit snapshots=all
    Response allResponse = doLoadTableWithSnapshots(namespace, "snapshots_all_foo1", "all");
    Assertions.assertEquals(Status.OK.getStatusCode(), allResponse.getStatus());
    LoadTableResponse allTableResponse = allResponse.readEntity(LoadTableResponse.class);

    // Both should return the same number of snapshots
    Assertions.assertEquals(
        defaultTableResponse.tableMetadata().snapshots().size(),
        allTableResponse.tableMetadata().snapshots().size(),
        "Default load and snapshots=all should return the same number of snapshots");
  }
}
