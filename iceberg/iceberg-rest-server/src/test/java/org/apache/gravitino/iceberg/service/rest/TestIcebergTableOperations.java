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
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergTableOperations extends TestIcebergNamespaceOperations {

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
    resourceConfig.register(IcebergNamespaceOperations.class);
    resourceConfig.register(MockIcebergTableRenameOperations.class);

    return resourceConfig;
  }

  @Test
  void testCreateTable() {
    verifyCreateTableFail("create_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCreateTableFailureEvent);

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);

    verifyCreateTableSucc("create_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergCreateTableEvent);

    verifyCreateTableFail("create_foo1", 409);
    verifyCreateTableFail("", 400);
  }

  @Test
  void testLoadTable() {
    verifyLoadTableFail("load_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergLoadTableFailureEvent);

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyCreateTableSucc("load_foo1");

    dummyEventListener.clearEvent();
    verifyLoadTableSucc("load_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergLoadTableEvent);

    verifyLoadTableFail("load_foo2", 404);
  }

  @Test
  void testDropTable() {
    verifyDropTableFail("drop_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergDropTableFailureEvent);

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyDropTableFail("drop_foo1", 404);

    verifyCreateTableSucc("drop_foo1");

    dummyEventListener.clearEvent();
    verifyDropTableSucc("drop_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergDropTableEvent);

    verifyLoadTableFail("drop_foo1", 404);
  }

  @Test
  void testUpdateTable() {
    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyCreateTableSucc("update_foo1");
    TableMetadata metadata = getTableMeta("update_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateSucc("update_foo1", metadata);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergUpdateTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergUpdateTableEvent);

    verifyDropTableSucc("update_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateTableFail("update_foo1", 404, metadata);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergUpdateTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergUpdateTableFailureEvent);

    verifyDropNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyUpdateTableFail("update_foo1", 404, metadata);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  void testListTables(String prefix) {
    setUrlPathWithPrefix(prefix);
    verifyListTableFail(404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergListTableFailureEvent);

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyCreateTableSucc("list_foo1");
    verifyCreateTableSucc("list_foo2");

    dummyEventListener.clearEvent();
    verifyListTableSucc(ImmutableSet.of("list_foo1", "list_foo2"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergListTableEvent);
  }

  @Test
  void testTableExits() {
    verifyTableExistsStatusCode("exists_foo2", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergTableExistsPreEvent);
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergTableExistsEvent);
    Assertions.assertEquals(false, ((IcebergTableExistsEvent) postEvent).isExists());

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyTableExistsStatusCode("exists_foo2", 404);

    verifyCreateTableSucc("exists_foo1");
    dummyEventListener.clearEvent();
    verifyTableExistsStatusCode("exists_foo1", 200);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergTableExistsPreEvent);
    postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergTableExistsEvent);
    Assertions.assertEquals(true, ((IcebergTableExistsEvent) postEvent).isExists());

    verifyLoadTableSucc("exists_foo1");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  void testRenameTable(String prefix) {
    setUrlPathWithPrefix(prefix);
    // namespace not exits
    verifyRenameTableFail("rename_foo1", "rename_foo3", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergRenameTableFailureEvent);

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyCreateTableSucc("rename_foo1");

    dummyEventListener.clearEvent();
    // rename
    verifyRenameTableSucc("rename_foo1", "rename_foo2");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergRenameTableEvent);

    verifyLoadTableFail("rename_foo1", 404);
    verifyLoadTableSucc("rename_foo2");

    // source table not exists
    verifyRenameTableFail("rename_foo1", "rename_foo3", 404);

    // dest table exists
    verifyCreateTableSucc("rename_foo3");
    verifyRenameTableFail("rename_foo2", "rename_foo3", 409);
  }

  @Test
  void testReportTableMetrics() {

    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);
    verifyCreateTableSucc("metrics_foo1");

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
        getReportMetricsClientBuilder("metrics_foo1")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void testCreateTableWithCredentialVending() {
    verifyCreateNamespaceSucc(IcebergRestTestUtil.TEST_NAMESPACE_NAME);

    // create the table without credential vending
    Response response = doCreateTable("create_without_credential_vending");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    // create the table with credential vending
    String tableName = "create_with_credential_vending";
    response = doCreateTableWithCredentialVending(tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        loadTableResponse.config().get(Credential.CREDENTIAL_TYPE));

    // load the table without credential vending
    response = doLoadTable(tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertTrue(!loadTableResponse.config().containsKey(Credential.CREDENTIAL_TYPE));

    // load the table with credential vending
    response = doLoadTableWithCredentialVending(tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        DummyCredentialProvider.DUMMY_CREDENTIAL_TYPE,
        loadTableResponse.config().get(Credential.CREDENTIAL_TYPE));
  }

  private Response doCreateTableWithCredentialVending(String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(tableSchema).build();
    return getTableClientBuilder()
        .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "vended-credentials")
        .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doCreateTable(String name) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName(name).withSchema(tableSchema).build();
    return getTableClientBuilder()
        .post(Entity.entity(createTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doRenameTable(String source, String dest) {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(
                TableIdentifier.of(Namespace.of(IcebergRestTestUtil.TEST_NAMESPACE_NAME), source))
            .withDestination(
                TableIdentifier.of(Namespace.of(IcebergRestTestUtil.TEST_NAMESPACE_NAME), dest))
            .build();
    return getRenameTableClientBuilder()
        .post(Entity.entity(renameTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doListTable() {
    return getTableClientBuilder().get();
  }

  private Response doDropTable(String name) {
    return getTableClientBuilder(Optional.of(name)).delete();
  }

  private Response doTableExists(String name) {
    return getTableClientBuilder(Optional.of(name)).head();
  }

  private Response doLoadTableWithCredentialVending(String name) {
    return getTableClientBuilder(Optional.of(name))
        .header(IcebergTableOperations.X_ICEBERG_ACCESS_DELEGATION, "vended-credentials")
        .get();
  }

  private Response doLoadTable(String name) {
    return getTableClientBuilder(Optional.of(name)).get();
  }

  private Response doUpdateTable(String name, TableMetadata base) {
    TableMetadata newMetadata = base.updateSchema(newTableSchema, base.lastColumnId());
    List<MetadataUpdate> metadataUpdates = newMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, metadataUpdates);
    UpdateTableRequest updateTableRequest = new UpdateTableRequest(requirements, metadataUpdates);
    return getTableClientBuilder(Optional.of(name))
        .post(Entity.entity(updateTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private TableMetadata getTableMeta(String tableName) {
    Response response = doLoadTable(tableName);
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    return loadTableResponse.tableMetadata();
  }

  private void verifyUpdateTableFail(String name, int status, TableMetadata base) {
    Response response = doUpdateTable(name, base);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyUpdateSucc(String name, TableMetadata base) {
    Response response = doUpdateTable(name, base);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        newTableSchema.columns(), loadTableResponse.tableMetadata().schema().columns());
  }

  private TableMetadata doGetTableMetaData(String name) {
    Response response = doLoadTable(name);
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    return loadTableResponse.tableMetadata();
  }

  private void verifyLoadTableFail(String name, int status) {
    Response response = doLoadTable(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyLoadTableSucc(String name) {
    Response response = doLoadTable(name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Assertions.assertEquals(
        tableSchema.columns(), loadTableResponse.tableMetadata().schema().columns());
  }

  private void verifyDropTableSucc(String name) {
    Response response = doDropTable(name);
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private void verifyTableExistsStatusCode(String name, int status) {
    Response response = doTableExists(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyDropTableFail(String name, int status) {
    Response response = doDropTable(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyCreateTableSucc(String name) {
    Response response = doCreateTable(name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
    Schema schema = loadTableResponse.tableMetadata().schema();
    Assertions.assertEquals(schema.columns(), tableSchema.columns());
  }

  private void verifyRenameTableSucc(String source, String dest) {
    Response response = doRenameTable(source, dest);
    System.out.println(response);
    System.out.flush();
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  private void verifyRenameTableFail(String source, String dest, int status) {
    Response response = doRenameTable(source, dest);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyListTableFail(int status) {
    Response response = doListTable();
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyListTableSucc(Set<String> expectedTableNames) {
    Response response = doListTable();
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    ListTablesResponse listTablesResponse = response.readEntity(ListTablesResponse.class);
    Set<String> tableNames =
        listTablesResponse.identifiers().stream()
            .map(identifier -> identifier.name())
            .collect(Collectors.toSet());
    Assertions.assertEquals(expectedTableNames, tableNames);
  }

  private void verifyCreateTableFail(String name, int status) {
    Response response = doCreateTable(name);
    Assertions.assertEquals(status, response.getStatus());
  }
}
