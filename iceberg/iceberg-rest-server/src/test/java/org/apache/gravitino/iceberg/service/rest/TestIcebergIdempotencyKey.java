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
import java.util.Arrays;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.idempotency.IcebergIdempotencyManager;
import org.apache.gravitino.listener.api.event.IcebergCreateTableEvent;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** End-to-end coverage of the {@code Idempotency-Key} header on mutation endpoints. */
@SuppressWarnings("deprecation")
public class TestIcebergIdempotencyKey extends IcebergNamespaceTestBase {

  private static final Namespace NAMESPACE = Namespace.of("idempotency_test");

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String OTHER_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";

  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.of(1, false, "foo_string", StringType.get()));

  private DummyEventListener dummyEventListener;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergTableOperations.class,
            true,
            Arrays.asList(dummyEventListener),
            ImmutableMap.of(IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED, "true"));
    resourceConfig.register(MockIcebergNamespaceOperations.class);
    resourceConfig.register(IcebergConfigOperations.class);

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

    GravitinoAuthorizerProvider.getInstance().initialize(new ServerConfig());
    return resourceConfig;
  }

  @Test
  void testConfigAdvertisesTheKeyLifetime() {
    Response response = getConfigClientBuilder().get();
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    // The Iceberg REST spec puts the lifetime at the top level of CatalogConfig, next to
    // `defaults` and `overrides`, not inside them.
    Assertions.assertEquals(
        "PT30M", response.readEntity(JsonNode.class).get("idempotency-key-lifetime").asText());
  }

  @Test
  void testRetryWithTheSameKeyReplaysTheOriginalResponse() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());

    Response first = createTable("replayed_table", KEY);
    Assertions.assertEquals(Status.OK.getStatusCode(), first.getStatus());
    String firstMetadataLocation = first.readEntity(LoadTableResponse.class).metadataLocation();
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergCreateTableEvent);
    dummyEventListener.clearEvent();

    Response retry = createTable("replayed_table", KEY);
    Assertions.assertEquals(Status.OK.getStatusCode(), retry.getStatus());
    Assertions.assertEquals(
        firstMetadataLocation, retry.readEntity(LoadTableResponse.class).metadataLocation());
    Assertions.assertTrue(
        dummyEventListener.postEvents.isEmpty(),
        "a replayed request must not re-run the mutation: " + dummyEventListener.postEvents);
  }

  @Test
  void testRetryWithANewKeyReExecutesAndConflicts() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());

    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("fresh_key_table", KEY).getStatus());
    // A new key means a new logical operation, so the mutation runs and hits the existing table.
    Assertions.assertEquals(
        Status.CONFLICT.getStatusCode(), createTable("fresh_key_table", OTHER_KEY).getStatus());
  }

  @Test
  void testTerminalErrorIsReplayed() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());
    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("conflict_table", OTHER_KEY).getStatus());

    Assertions.assertEquals(
        Status.CONFLICT.getStatusCode(), createTable("conflict_table", KEY).getStatus());
    // The stored 409 is replayed rather than re-running the create.
    Assertions.assertEquals(
        Status.CONFLICT.getStatusCode(), createTable("conflict_table", KEY).getStatus());
  }

  @Test
  void testInvalidKeyIsRejected() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());

    // A UUIDv4 is well-formed but the spec requires UUIDv7.
    Response response = createTable("rejected_table", "f47ac10b-58cc-4372-a567-0e02b2c3d479");
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());

    // The mutation must not have run.
    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("rejected_table", KEY).getStatus());
  }

  @Test
  void testKeyReusedForAnotherOperationIsRejected() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());
    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("reused_table", KEY).getStatus());

    Response response =
        getTableClientBuilder(NAMESPACE, Optional.of("reused_table"))
            .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, KEY)
            .delete();

    Assertions.assertEquals(Status.CONFLICT.getStatusCode(), response.getStatus());
    Assertions.assertNull(response.getHeaderString(HttpHeaders.RETRY_AFTER));
    // The table is still there: the drop never ran.
    Assertions.assertEquals(
        Status.OK.getStatusCode(),
        getTableClientBuilder(NAMESPACE, Optional.of("reused_table")).get().getStatus());
  }

  @Test
  void testRequestWithoutAKeyIsUnaffected() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());

    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("no_key_table", null).getStatus());
    Assertions.assertEquals(
        Status.CONFLICT.getStatusCode(), createTable("no_key_table", null).getStatus());
  }

  @Test
  void testDropIsReplayed() {
    Assertions.assertEquals(Status.OK.getStatusCode(), doCreateNamespace(NAMESPACE).getStatus());
    Assertions.assertEquals(
        Status.OK.getStatusCode(), createTable("dropped_table", OTHER_KEY).getStatus());

    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), dropTable("dropped_table", KEY));
    // Without idempotency the retry would be a 404; the stored 204 is replayed instead.
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), dropTable("dropped_table", KEY));
  }

  private Response createTable(String name, String idempotencyKey) {
    CreateTableRequest request =
        CreateTableRequest.builder().withName(name).withSchema(TABLE_SCHEMA).build();
    Invocation.Builder builder = getTableClientBuilder(NAMESPACE, Optional.empty());
    if (idempotencyKey != null) {
      builder = builder.header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, idempotencyKey);
    }
    return builder.post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private int dropTable(String name, String idempotencyKey) {
    return getTableClientBuilder(NAMESPACE, Optional.of(name))
        .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, idempotencyKey)
        .delete()
        .getStatus();
  }
}
