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
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.idempotency.InMemoryIdempotencyStore;
import org.apache.gravitino.iceberg.service.idempotency.IcebergIdempotencyManager;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** HTTP coverage of a committed mutation whose response cannot be retained. */
public class TestIcebergIdempotencyFinalizeFailure extends IcebergTestBase {
  @Override
  protected Application configure() {
    ResourceConfig config =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergNamespaceOperations.class,
            true,
            Collections.emptyList(),
            ImmutableMap.of(
                IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED,
                "true",
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE,
                NoOpFinalizeStore.class.getName()));
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
            Mockito.when(request.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(request).to(HttpServletRequest.class);
          }
        });
    GravitinoAuthorizerProvider.getInstance().initialize(new ServerConfig());
    return config;
  }

  @Test
  void testRetryExecutesAgainAfterSuccessfulMutationCannotFinalize() {
    Namespace namespace = Namespace.of("finalize_failure");
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();
    String key = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
    try (Response first =
        getNamespaceClientBuilder()
            .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, key)
            .post(Entity.json(request))) {
      Assertions.assertEquals(200, first.getStatus());
    }
    // The catalog committed even though the store did not retain the response.
    try (Response loaded = getNamespaceClientBuilder(Optional.of(namespace)).get()) {
      Assertions.assertEquals(200, loaded.getStatus());
    }
    try (Response retry =
        getNamespaceClientBuilder()
            .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, key)
            .post(Entity.json(request))) {
      // Release permits execution again: this is the catalog's AlreadyExists response,
      // not an in-flight idempotency conflict or a replayed 200.
      Assertions.assertEquals(409, retry.getStatus());
      Assertions.assertNull(retry.getHeaderString(HttpHeaders.RETRY_AFTER));
      Assertions.assertEquals(
          "AlreadyExistsException",
          retry.readEntity(JsonNode.class).get("error").get("type").asText());
    }
  }

  /** Fault-injection store that accepts reservations but never retains final responses. */
  public static class NoOpFinalizeStore extends InMemoryIdempotencyStore {
    @Override
    public void finalizeRecord(String key, long claim, int status, @Nullable String summary) {
      // Deliberately leave the record reserved to exercise the manager's release path.
    }
  }
}
