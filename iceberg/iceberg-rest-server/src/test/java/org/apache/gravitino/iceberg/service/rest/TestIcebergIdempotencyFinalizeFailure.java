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
import java.util.OptionalLong;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.idempotency.IcebergIdempotencyManager;
import org.apache.gravitino.iceberg.service.idempotency.InMemoryIdempotencyStore;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/** HTTP coverage of idempotency storage failures before and after catalog mutations. */
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
                FaultInjectingStore.class.getName()));
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

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testRetryDoesNotReexecuteAfterFinalizationFailure(int failureMode) {
    Namespace namespace = Namespace.of("finalize_failure");
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();
    String key = "017f22e2-79b0-7cc3-98c4-dc0c0c07398" + failureMode;
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
      if (failureMode == 2) {
        Assertions.assertEquals(200, retry.getStatus(), "a committed response remains replayable");
      } else {
        Assertions.assertEquals(409, retry.getStatus());
        Assertions.assertEquals("1", retry.getHeaderString(HttpHeaders.RETRY_AFTER));
        Assertions.assertEquals(
            "IdempotencyConflictException",
            retry.readEntity(JsonNode.class).get("error").get("type").asText());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 4})
  void testReservationFailureDoesNotExecuteMutation(int failureMode) {
    Namespace namespace = Namespace.of("reserve_failure");
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();
    String key = "017f22e2-79b0-7cc3-98c4-dc0c0c07398" + failureMode;
    try (Response first =
        getNamespaceClientBuilder()
            .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, key)
            .post(Entity.json(request))) {
      Assertions.assertEquals(503, first.getStatus());
      Assertions.assertEquals("1", first.getHeaderString(HttpHeaders.RETRY_AFTER));
      JsonNode error = first.readEntity(JsonNode.class).get("error");
      Assertions.assertEquals("ServiceUnavailableException", error.get("type").asText());
      Assertions.assertTrue(error.get("message").asText().contains("did not execute"));
    }
    try (Response retry =
        getNamespaceClientBuilder()
            .header(IcebergIdempotencyManager.IDEMPOTENCY_KEY, key)
            .post(Entity.json(request))) {
      Assertions.assertEquals(failureMode == 3 ? 503 : 409, retry.getStatus());
      Assertions.assertEquals("1", retry.getHeaderString(HttpHeaders.RETRY_AFTER));
    }
    try (Response loaded = getNamespaceClientBuilder(Optional.of(namespace)).get()) {
      Assertions.assertEquals(404, loaded.getStatus(), "neither attempt may mutate the catalog");
    }
  }

  /** Fault-injection store simulating missing writes and lost commit acknowledgements. */
  public static class FaultInjectingStore extends InMemoryIdempotencyStore {
    @Override
    public OptionalLong reserve(String key, String binding, long expiresAtMs) {
      if (key.endsWith("3")) {
        throw new IllegalStateException("reservation unavailable");
      }
      OptionalLong result = super.reserve(key, binding, expiresAtMs);
      if (key.endsWith("4") && result.isPresent()) {
        throw new IllegalStateException("reservation committed but acknowledgement lost");
      }
      return result;
    }

    @Override
    public void finalizeRecord(String key, long claim, int status, @Nullable String summary) {
      if (key.endsWith("2")) {
        super.finalizeRecord(key, claim, status, summary);
        throw new IllegalStateException("response committed but acknowledgement lost");
      }
      if (key.endsWith("1")) {
        throw new IllegalStateException("response write failed");
      }
      // Leave the reservation intact when the response cannot be retained.
    }
  }
}
