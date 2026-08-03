/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.deletion.IcebergDeletionException;
import org.apache.gravitino.iceberg.service.deletion.IcebergDeletionException.Outcome;
import org.apache.gravitino.iceberg.service.deletion.IcebergRetainedTableDeletion;
import org.apache.gravitino.iceberg.service.deletion.IcebergTableDeletionLifecycle;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.server.web.filter.IcebergTableDeletionAuthzHandler.AuthorizedDeletionTarget;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** HTTP contract tests for name-only Iceberg table UNDROP. */
public class TestIcebergDeletionManagementOperations extends IcebergTestBase {

  private static final String CATALOG = IcebergRestTestUtil.PREFIX;
  private static final Namespace NAMESPACE = Namespace.of("sales");

  private IcebergTableDeletionLifecycle lifecycle;
  private IcebergTableOperationDispatcher dispatcher;
  private IcebergRetainedTableDeletion deletion;

  @Override
  protected Application configure() {
    lifecycle = org.mockito.Mockito.mock(IcebergTableDeletionLifecycle.class);
    dispatcher = org.mockito.Mockito.mock(IcebergTableOperationDispatcher.class);
    HttpServletRequest httpRequest = IcebergRestTestUtil.createMockHttpRequest();
    ResourceConfig config =
        IcebergRestTestUtil.getIcebergResourceConfig(MockIcebergDeletionManagementOperations.class);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(lifecycle).to(IcebergTableDeletionLifecycle.class).ranked(3);
            bind(dispatcher).to(IcebergTableOperationDispatcher.class).ranked(3);
            bind(httpRequest).to(HttpServletRequest.class);
          }
        });
    return config;
  }

  @BeforeEach
  void resetMocks() {
    reset(lifecycle, dispatcher);
    deletion = retainedDeletion("D1", "orders");
  }

  @Test
  void testBodylessPostReturnsOrdinaryLoadResponse() throws Exception {
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "orders");
    when(lifecycle.getDeleted(CATALOG, identifier)).thenReturn(deletion);
    when(dispatcher.loadTable(any(IcebergRequestContext.class), eq(identifier)))
        .thenReturn(loadResponse());

    Response response = post("orders");

    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString(HttpHeaders.ETAG));
    String body = response.readEntity(String.class);
    assertNotNull(
        IcebergObjectMapper.getInstance().readTree(body).get("metadata-location").asText());
    verify(lifecycle).getDeleted(CATALOG, identifier);
    verify(lifecycle).undrop(any(IcebergRequestContext.class), eq(identifier), eq("D1"), eq(42L));
    ArgumentCaptor<IcebergRequestContext> contexts =
        ArgumentCaptor.forClass(IcebergRequestContext.class);
    verify(dispatcher).loadTable(contexts.capture(), eq(identifier));
    assertFalse(
        contexts.getAllValues().stream().anyMatch(IcebergRequestContext::requestCredentialVending));
  }

  @Test
  void testUndropUsesTheExactGenerationBoundByAuthorization() {
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "orders");
    HttpServletRequest request = IcebergRestTestUtil.createMockHttpRequest();
    when(request.getAttribute(anyString()))
        .thenAnswer(
            invocation ->
                ((String) invocation.getArgument(0)).endsWith(".authorizedDeletion")
                    ? new AuthorizedDeletionTarget(42L, "D-authorized")
                    : null);
    when(dispatcher.loadTable(any(IcebergRequestContext.class), eq(identifier)))
        .thenReturn(loadResponse());
    IcebergDeletionManagementOperations operations =
        new IcebergDeletionManagementOperations(lifecycle, dispatcher);

    Response response =
        operations.undrop(
            CATALOG + "/",
            RESTUtil.encodeNamespace(
                NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8),
            "orders",
            request);

    assertEquals(200, response.getStatus());
    verify(lifecycle, never()).getDeleted(any(), any());
    verify(lifecycle)
        .undrop(any(IcebergRequestContext.class), eq(identifier), eq("D-authorized"), eq(42L));
  }

  @Test
  void testMissingNameIsNotFoundBeforeRestore() {
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "missing");
    when(lifecycle.getDeleted(CATALOG, identifier)).thenThrow(IcebergDeletionException.notFound());

    Response response = post("missing");

    assertEquals(404, response.getStatus());
    verify(lifecycle, never()).undrop(any(), any(), any(), anyLong());
    verify(dispatcher, never()).loadTable(any(), any());
  }

  @Test
  void testLifecycleOutcomesMapToStableHttpStatuses() {
    assertOutcome(Outcome.GONE, 410);
    assertOutcome(Outcome.CONFLICT, 409);
  }

  private void assertOutcome(Outcome outcome, int status) {
    reset(lifecycle, dispatcher);
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "orders");
    when(lifecycle.getDeleted(CATALOG, identifier)).thenReturn(deletion);
    doThrow(new IcebergDeletionException(outcome, "safe failure"))
        .when(lifecycle)
        .undrop(any(IcebergRequestContext.class), eq(identifier), eq("D1"), eq(42L));

    assertEquals(status, post("orders").getStatus());
    verify(dispatcher, never()).loadTable(any(), any());
  }

  private Response post(String table) {
    return getIcebergClientBuilder(undropPath(table), Optional.empty()).method("POST");
  }

  private static String undropPath(String table) {
    return "/management/v1/"
        + CATALOG
        + "/namespaces/"
        + RESTUtil.encodeNamespace(NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8)
        + "/tables/"
        + RESTUtil.encodeString(table)
        + "/undrop";
  }

  private static IcebergRetainedTableDeletion retainedDeletion(
      String deletionId, String tableName) {
    TablePO table =
        TablePO.builder()
            .withTableId(42L)
            .withTableName(tableName)
            .withMetalakeId(1L)
            .withCatalogId(2L)
            .withSchemaId(3L)
            .withAuditInfo("{}")
            .withCurrentVersion(7L)
            .withLastVersion(7L)
            .withDeletedAt(100L)
            .withDeletionId(deletionId)
            .build();
    EntityDeletionPO action =
        EntityDeletionPO.builder()
            .deletionId(deletionId)
            .state("DELETED")
            .retentionExpiresAt(1_000L)
            .build();
    return IcebergRetainedTableDeletion.builder().table(table).deletion(action).build();
  }

  private static LoadTableResponse loadResponse() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    String location = "file:/warehouse/sales/orders";
    TableMetadata base =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), location, ImmutableMap.of());
    TableMetadata metadata =
        TableMetadataParser.fromJson(
            location + "/metadata/v1.metadata.json", TableMetadataParser.toJson(base));
    return LoadTableResponse.builder()
        .withTableMetadata(metadata)
        .addAllConfig(ImmutableMap.of())
        .build();
  }
}
