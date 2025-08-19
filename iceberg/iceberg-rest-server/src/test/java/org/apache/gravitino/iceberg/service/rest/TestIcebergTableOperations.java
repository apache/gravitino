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

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
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
import org.apache.gravitino.listener.api.event.IcebergRenameTableEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergTableExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateTablePreEvent;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
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
    response = doCreateTableWithCredentialVending(namespace, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        loadTableResponse.config().get(Credential.CREDENTIAL_TYPE));

    // load the table without credential vending
    response = doLoadTable(namespace, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    // load the table with credential vending
    response = doLoadTableWithCredentialVending(namespace, tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        loadTableResponse.config().get(Credential.CREDENTIAL_TYPE));
  }

  private Response doCreateTableWithCredentialVending(Namespace ns, String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(tableSchema).build();
    return getTableClientBuilder(ns, Optional.empty())
        .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "vended-credentials")
        .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doCreateTable(Namespace ns, String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(tableSchema).build();
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

  private void verifyDropTableFail(Namespace ns, String name, int status) {
    Response response = doDropTable(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyCreateTableSucc(Namespace ns, String name) {
    Response response = doCreateTable(ns, name);
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
}
