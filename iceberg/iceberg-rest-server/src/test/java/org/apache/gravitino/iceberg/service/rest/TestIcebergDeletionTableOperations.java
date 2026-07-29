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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.deletion.IcebergTableDeletionLifecycle;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.server.web.filter.IcebergTableDeletionAuthzHandler.AuthorizedDeletionTarget;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** HTTP tests for deletion-aware, idempotent Iceberg table DELETE. */
public class TestIcebergDeletionTableOperations extends IcebergTestBase {

  private static final Namespace NAMESPACE = Namespace.of("sales");
  private static final String CATALOG = IcebergRestTestUtil.PREFIX;

  private IcebergTableDeletionLifecycle lifecycle;
  private IcebergTableOperationDispatcher dispatcher;

  @Override
  protected Application configure() {
    lifecycle = mock(IcebergTableDeletionLifecycle.class);
    dispatcher = mock(IcebergTableOperationDispatcher.class);
    HttpServletRequest httpRequest = IcebergRestTestUtil.createMockHttpRequest();
    ResourceConfig config =
        IcebergRestTestUtil.getIcebergResourceConfig(MockIcebergDeletionTableOperations.class);
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
  }

  @Test
  void testDeleteDispatchesAndReturnsNoContent() {
    TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "orders");

    Response response =
        getIcebergClientBuilder(
                tablePath("orders"), Optional.of(ImmutableMap.of("purgeRequested", "true")))
            .delete();

    assertEquals(204, response.getStatus());
    verify(dispatcher).dropTable(any(), eq(identifier), eq(true));
  }

  @Test
  void testRetainedDeleteDoesNotRedispatchByName() {
    HttpServletRequest request = IcebergRestTestUtil.createMockHttpRequest();
    when(request.getAttribute(anyString()))
        .thenAnswer(
            invocation ->
                ((String) invocation.getArgument(0)).endsWith(".authorizedDeletion")
                    ? new AuthorizedDeletionTarget(42L, "D1")
                    : null);
    IcebergTableOperations operations =
        new IcebergTableOperations(mock(IcebergMetricsManager.class), dispatcher, lifecycle);

    Response response =
        operations.dropTable(
            CATALOG + "/",
            RESTUtil.encodeNamespace(
                NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8),
            "orders",
            true,
            request);

    assertEquals(204, response.getStatus());
    verify(dispatcher, never()).dropTable(any(), any(), anyBoolean());
  }

  @Test
  void testDeletedListReturnsAuthorizedNamesWithExistingPagination() throws Exception {
    when(lifecycle.listDeleted(CATALOG, NAMESPACE))
        .thenReturn(
            List.of(
                retainedTable("D1", 41L, "customers"),
                retainedTable("D-hidden", 99L, "hidden"),
                retainedTable("D2", 42L, "orders"),
                retainedTable("D3", 43L, "returns")));

    Response firstResponse =
        getIcebergClientBuilder(
                tableCollectionPath(),
                Optional.of(ImmutableMap.of("deleted", "true", "pageSize", "2")))
            .get();

    assertEquals(200, firstResponse.getStatus());
    assertEquals("private, no-store", firstResponse.getHeaderString(HttpHeaders.CACHE_CONTROL));
    String firstBody = firstResponse.readEntity(String.class);
    assertFalse(firstBody.contains("D1"));
    ListTablesResponse firstPage =
        IcebergObjectMapper.getInstance().readValue(firstBody, ListTablesResponse.class);
    assertEquals(
        List.of(
            TableIdentifier.of(NAMESPACE, "customers"), TableIdentifier.of(NAMESPACE, "orders")),
        firstPage.identifiers());

    Response secondResponse =
        getIcebergClientBuilder(
                tableCollectionPath(),
                Optional.of(
                    ImmutableMap.of(
                        "deleted",
                        "true",
                        "pageSize",
                        "2",
                        "pageToken",
                        firstPage.nextPageToken())))
            .get();
    ListTablesResponse secondPage = secondResponse.readEntity(ListTablesResponse.class);
    assertEquals(List.of(TableIdentifier.of(NAMESPACE, "returns")), secondPage.identifiers());
    verify(dispatcher, never()).listTable(any(), any());
  }

  @Test
  void testDefaultListStillUsesTheLiveTableDispatcher() {
    when(dispatcher.listTable(any(), eq(NAMESPACE)))
        .thenReturn(ListTablesResponse.builder().build());

    Response response = getIcebergClientBuilder(tableCollectionPath(), Optional.empty()).get();

    assertEquals(200, response.getStatus());
    verify(dispatcher).listTable(any(), eq(NAMESPACE));
    verify(lifecycle, never()).listDeleted(any(), any());
  }

  private static String tableCollectionPath() {
    return "/v1/"
        + CATALOG
        + "/namespaces/"
        + RESTUtil.encodeNamespace(NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8)
        + "/tables";
  }

  private static String tablePath(String table) {
    return "/v1/"
        + CATALOG
        + "/namespaces/"
        + RESTUtil.encodeNamespace(NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8)
        + "/tables/"
        + RESTUtil.encodeString(table);
  }

  private static TablePO retainedTable(String deletionId, long tableId, String tableName) {
    return TablePO.builder()
        .withTableId(tableId)
        .withTableName(tableName)
        .withMetalakeId(1L)
        .withCatalogId(2L)
        .withSchemaId(3L)
        .withAuditInfo("{}")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(100L)
        .withDeletionId(deletionId)
        .build();
  }
}
