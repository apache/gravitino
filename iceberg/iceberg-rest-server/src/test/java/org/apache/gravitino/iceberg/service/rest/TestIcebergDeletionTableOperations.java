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
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.server.web.filter.IcebergTableDeletionAuthzHandler.AuthorizedDeletionTarget;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** HTTP tests for deletion-aware, idempotent Iceberg table DELETE. */
public class TestIcebergDeletionTableOperations extends IcebergTestBase {

  private static final Namespace NAMESPACE = Namespace.of("sales");
  private static final String CATALOG = IcebergRestTestUtil.PREFIX;

  private IcebergTableOperationDispatcher dispatcher;

  @Override
  protected Application configure() {
    dispatcher = mock(IcebergTableOperationDispatcher.class);
    HttpServletRequest httpRequest = IcebergRestTestUtil.createMockHttpRequest();
    ResourceConfig config =
        IcebergRestTestUtil.getIcebergResourceConfig(MockIcebergDeletionTableOperations.class);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(IcebergTableOperationDispatcher.class).ranked(3);
            bind(httpRequest).to(HttpServletRequest.class);
          }
        });
    return config;
  }

  @BeforeEach
  void resetMocks() {
    reset(dispatcher);
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
        new IcebergTableOperations(mock(IcebergMetricsManager.class), dispatcher);

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

  private static String tablePath(String table) {
    return "/v1/"
        + CATALOG
        + "/namespaces/"
        + RESTUtil.encodeNamespace(NAMESPACE, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8)
        + "/tables/"
        + RESTUtil.encodeString(table);
  }
}
