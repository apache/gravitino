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

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionEvent;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionPreEvent;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
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
public class TestIcebergTransactionOperations extends IcebergNamespaceTestBase {

  private static final Schema initialSchema =
      new Schema(NestedField.of(1, false, "col_a", StringType.get()));

  private static final Schema updatedSchema =
      new Schema(
          NestedField.of(1, false, "col_a", StringType.get()),
          NestedField.of(2, true, "col_b", StringType.get()));

  private DummyEventListener dummyEventListener;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergTransactionOperations.class, true, Arrays.asList(dummyEventListener));
    resourceConfig.register(MockIcebergNamespaceOperations.class);
    resourceConfig.register(MockIcebergTableOperations.class);
    resourceConfig.register(MockIcebergTableRenameOperations.class);

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

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCommitTransactionSuccess(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    createTable(namespace, "txn_table1");
    createTable(namespace, "txn_table2");

    TableMetadata meta1 = loadTableMeta(namespace, "txn_table1");
    TableMetadata meta2 = loadTableMeta(namespace, "txn_table2");

    CommitTransactionRequest request = buildCommitRequest(namespace, meta1, meta2);
    Response response = doCommitTransaction(request);

    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergCommitTransactionPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCommitTransactionEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCommitTransactionTableNotFound(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);

    TableIdentifier missingId = TableIdentifier.of(namespace, "does_not_exist");
    UpdateTableRequest change =
        UpdateTableRequest.create(
            missingId, List.of(), List.of(new MetadataUpdate.AddSchema(updatedSchema)));
    CommitTransactionRequest request = new CommitTransactionRequest(List.of(change));
    Response response = doCommitTransaction(request);

    Assertions.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergCommitTransactionPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCommitTransactionFailureEvent);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCommitTransactionWithPrefix(Namespace namespace) {
    setUrlPathWithPrefix(IcebergRestTestUtil.PREFIX);
    verifyCreateNamespaceSucc(namespace);
    createTable(namespace, "prefix_txn_table");

    TableMetadata meta = loadTableMeta(namespace, "prefix_txn_table");
    TableIdentifier tableId = TableIdentifier.of(namespace, "prefix_txn_table");
    List<MetadataUpdate> updates = buildSchemaUpdates(meta);
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(meta, updates);
    UpdateTableRequest change = UpdateTableRequest.create(tableId, requirements, updates);
    CommitTransactionRequest request = new CommitTransactionRequest(List.of(change));

    Response response = doCommitTransaction(request);
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private void createTable(Namespace ns, String name) {
    String path =
        Joiner.on("/")
            .join(IcebergRestTestUtil.NAMESPACE_PATH, RESTUtil.encodeNamespace(ns), "tables");
    getIcebergClientBuilder(path, Optional.empty())
        .post(
            Entity.entity(
                CreateTableRequest.builder().withName(name).withSchema(initialSchema).build(),
                MediaType.APPLICATION_JSON_TYPE));
  }

  private TableMetadata loadTableMeta(Namespace ns, String name) {
    String path =
        Joiner.on("/")
            .join(IcebergRestTestUtil.NAMESPACE_PATH, RESTUtil.encodeNamespace(ns), "tables", name);
    Response response = getIcebergClientBuilder(path, Optional.empty()).get();
    return response.readEntity(LoadTableResponse.class).tableMetadata();
  }

  private CommitTransactionRequest buildCommitRequest(
      Namespace ns, TableMetadata meta1, TableMetadata meta2) {
    TableIdentifier id1 = TableIdentifier.of(ns, "txn_table1");
    TableIdentifier id2 = TableIdentifier.of(ns, "txn_table2");

    List<MetadataUpdate> updates1 = buildSchemaUpdates(meta1);
    List<UpdateRequirement> reqs1 = UpdateRequirements.forUpdateTable(meta1, updates1);
    UpdateTableRequest change1 = UpdateTableRequest.create(id1, reqs1, updates1);

    List<MetadataUpdate> updates2 = buildSchemaUpdates(meta2);
    List<UpdateRequirement> reqs2 = UpdateRequirements.forUpdateTable(meta2, updates2);
    UpdateTableRequest change2 = UpdateTableRequest.create(id2, reqs2, updates2);

    return new CommitTransactionRequest(List.of(change1, change2));
  }

  private List<MetadataUpdate> buildSchemaUpdates(TableMetadata base) {
    TableMetadata updated = base.updateSchema(updatedSchema);
    return updated.changes();
  }

  private Response doCommitTransaction(CommitTransactionRequest request) {
    return getIcebergClientBuilder(IcebergRestTestUtil.COMMIT_TRANSACTION_PATH, Optional.empty())
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }
}
