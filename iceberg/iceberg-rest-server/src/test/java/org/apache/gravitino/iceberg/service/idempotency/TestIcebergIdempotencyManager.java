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
package org.apache.gravitino.iceberg.service.idempotency;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore.ReserveResult;
import org.apache.gravitino.iceberg.common.idempotency.InMemoryIdempotencyStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergIdempotencyManager {

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String OTHER_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";
  private static final String BINDING = "POST v1/cat1/namespaces/ns1/tables";

  @Test
  void testDisabledManagerRunsEveryRequest() {
    IcebergIdempotencyManager manager = newDisabledManager();
    AtomicInteger executions = new AtomicInteger();

    Assertions.assertEquals(200, execute(manager, KEY, executions).getStatus());
    Assertions.assertEquals(200, execute(manager, KEY, executions).getStatus());

    Assertions.assertEquals(2, executions.get());
    Assertions.assertEquals(Optional.empty(), manager.advertisedKeyLifetime());
  }

  @Test
  void testEnabledManagerAdvertisesTheConfiguredLifetime() {
    IcebergIdempotencyManager manager =
        newManager(ImmutableMap.of(IcebergConstants.ICEBERG_IDEMPOTENCY_KEY_LIFETIME, "PT2H"));
    Assertions.assertEquals(Optional.of("PT2H"), manager.advertisedKeyLifetime());
  }

  @Test
  void testRequestWithoutKeyIsNotDeduplicated() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    execute(manager, null, executions);
    execute(manager, "", executions);

    Assertions.assertEquals(2, executions.get());
  }

  @Test
  void testRetryReplaysTheStoredResponse() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    Response first = execute(manager, KEY, executions);
    Response replayed = execute(manager, KEY, executions);

    Assertions.assertEquals(1, executions.get(), "the mutation must run exactly once");
    Assertions.assertEquals(first.getStatus(), replayed.getStatus());
    Assertions.assertEquals("{\"payload\":1}", replayed.getEntity());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, replayed.getMediaType());
  }

  @Test
  void testDifferentKeyExecutesAgain() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    execute(manager, KEY, executions);
    execute(manager, OTHER_KEY, executions);

    Assertions.assertEquals(2, executions.get());
  }

  @Test
  void testResponseWithoutBodyIsReplayed() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    manager.replayOrExecute(
        KEY,
        BINDING,
        () -> {
          executions.incrementAndGet();
          return Response.noContent().build();
        });
    Response replayed =
        manager.replayOrExecute(
            KEY,
            BINDING,
            () -> {
              executions.incrementAndGet();
              return Response.noContent().build();
            });

    Assertions.assertEquals(1, executions.get());
    Assertions.assertEquals(204, replayed.getStatus());
    Assertions.assertNull(replayed.getEntity());
  }

  @Test
  void testTerminalClientErrorIsReplayed() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    Response first = respondWith(manager, 409, executions);
    Response replayed = respondWith(manager, 409, executions);

    Assertions.assertEquals(1, executions.get());
    Assertions.assertEquals(409, first.getStatus());
    Assertions.assertEquals(409, replayed.getStatus());
  }

  @Test
  void testServerErrorReleasesTheKey() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    Assertions.assertEquals(500, respondWith(manager, 500, executions).getStatus());
    // The spec treats 5xx as retryable, so the same key must be usable again.
    Assertions.assertEquals(503, respondWith(manager, 503, executions).getStatus());
    Assertions.assertEquals(2, executions.get());
  }

  @Test
  void testUnauthorizedResponseReleasesTheKey() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    respondWith(manager, 401, executions);
    respondWith(manager, 401, executions);

    Assertions.assertEquals(2, executions.get());
  }

  @Test
  void testThrownExceptionReleasesTheKey() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            manager.replayOrExecute(
                KEY,
                BINDING,
                () -> {
                  executions.incrementAndGet();
                  throw new IllegalStateException("boom");
                }));

    Assertions.assertEquals(200, execute(manager, KEY, executions).getStatus());
    Assertions.assertEquals(2, executions.get());
  }

  @Test
  void testInvalidKeyIsRejectedWithoutExecuting() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    Response response = execute(manager, "not-a-uuid-v7", executions);

    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(0, executions.get());
  }

  @Test
  void testKeyReusedForAnotherOperationIsRejected() {
    IcebergIdempotencyManager manager = newManager(ImmutableMap.of());
    AtomicInteger executions = new AtomicInteger();

    execute(manager, KEY, executions);
    Response response =
        manager.replayOrExecute(
            KEY,
            "DELETE v1/cat1/namespaces/ns1/tables/t1",
            () -> {
              executions.incrementAndGet();
              return Response.ok().build();
            });

    Assertions.assertEquals(409, response.getStatus());
    Assertions.assertEquals(1, executions.get(), "the second operation must not run");
    Assertions.assertNull(response.getHeaderString(HttpHeaders.RETRY_AFTER));
  }

  @Test
  void testRetryWhileTheFirstRequestIsRunningIsRetryable() {
    IdempotencyStore store = Mockito.mock(IdempotencyStore.class);
    Mockito.when(store.reserve(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong()))
        .thenReturn(ReserveResult.DUPLICATE);
    Mockito.when(store.load(KEY)).thenReturn(Optional.empty());
    IcebergIdempotencyManager manager =
        new IcebergIdempotencyManager(enabledConfig(ImmutableMap.of()), Optional.of(store));
    AtomicInteger executions = new AtomicInteger();

    Response response = execute(manager, KEY, executions);

    Assertions.assertEquals(409, response.getStatus());
    Assertions.assertEquals(0, executions.get());
    Assertions.assertEquals("1", response.getHeaderString(HttpHeaders.RETRY_AFTER));
  }

  @Test
  void testOperationBindingIncludesSortedQueryParameters() {
    MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
    queryParameters.putSingle("purgeRequested", "true");
    queryParameters.putSingle("mode", "async");
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(uriInfo.getPath()).thenReturn("v1/cat1/namespaces/ns1/tables/t1");
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryParameters);

    Assertions.assertEquals(
        "DELETE v1/cat1/namespaces/ns1/tables/t1?mode=async&purgeRequested=true",
        IcebergIdempotencyManager.operationBinding("DELETE", uriInfo));
  }

  @Test
  void testOperationBindingWithoutUriInfoFallsBackToTheMethod() {
    Assertions.assertEquals("POST", IcebergIdempotencyManager.operationBinding("POST", null));
  }

  private static Response execute(
      IcebergIdempotencyManager manager, String idempotencyKey, AtomicInteger executions) {
    return manager.replayOrExecute(
        idempotencyKey,
        BINDING,
        () -> {
          executions.incrementAndGet();
          return Response.ok("{\"payload\":1}", MediaType.APPLICATION_JSON_TYPE).build();
        });
  }

  private static Response respondWith(
      IcebergIdempotencyManager manager, int status, AtomicInteger executions) {
    return manager.replayOrExecute(
        KEY,
        BINDING,
        () -> {
          executions.incrementAndGet();
          return Response.status(status).build();
        });
  }

  private static IcebergIdempotencyManager newManager(Map<String, String> properties) {
    IcebergConfig icebergConfig = enabledConfig(properties);
    InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    store.initialize(icebergConfig.getAllConfig());
    return new IcebergIdempotencyManager(icebergConfig, Optional.of(store));
  }

  private static IcebergIdempotencyManager newDisabledManager() {
    return new IcebergIdempotencyManager(new IcebergConfig(ImmutableMap.of()), Optional.empty());
  }

  private static IcebergConfig enabledConfig(Map<String, String> properties) {
    Map<String, String> merged = new HashMap<>(properties);
    merged.put(IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED, "true");
    return new IcebergConfig(merged);
  }
}
