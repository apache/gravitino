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
package org.apache.gravitino.server.web.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import javax.ws.rs.core.Response;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.server.web.ServerHealth;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHealthOperations {

  private HealthOperations newOps(EntityStore store) {
    return newOps(store, 2000L);
  }

  private HealthOperations newOps(EntityStore store, long probeTimeoutMs) {
    return newOps(store, probeTimeoutMs, new ServerHealth());
  }

  private HealthOperations newOps(EntityStore store, long probeTimeoutMs, ServerHealth health) {
    return new HealthOperations(health) {
      @Override
      EntityStore getEntityStore() {
        return store;
      }

      @Override
      long getProbeTimeoutMs() {
        return probeTimeoutMs;
      }
    };
  }

  @Test
  public void testLiveReturns200WithUpStatus() {
    HealthOperations ops = newOps(null);
    Response response = ops.live();
    assertEquals(200, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
    assertEquals(1, body.getChecks().size());
    assertEquals("httpServer", body.getChecks().get(0).getName());
  }

  @Test
  public void testReadyReturns503WhenEntityStoreNotInitialized() {
    HealthOperations ops = newOps(null);
    Response response = ops.ready();
    assertEquals(503, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    assertEquals("entityStore", body.getChecks().get(0).getName());
    assertNotNull(body.getChecks().get(0).getDetails().get("reason"));
  }

  @Test
  public void testReadyReturns200WhenEntityStoreReachable() throws IOException {
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.exists(Mockito.any(), Mockito.any())).thenReturn(false);
    HealthOperations ops = newOps(store);
    Response response = ops.ready();
    assertEquals(200, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
  }

  @Test
  public void testReadyReturns503WhenEntityStoreThrows() throws IOException {
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.exists(Mockito.any(), Mockito.any()))
        .thenThrow(new IOException("connection refused"));
    HealthOperations ops = newOps(store);
    Response response = ops.ready();
    assertEquals(503, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    assertEquals("IOException", body.getChecks().get(0).getDetails().get("reason"));
  }

  @Test
  public void testAggregateReturns200WhenAllChecksPass() throws IOException {
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.exists(Mockito.any(), Mockito.any())).thenReturn(false);
    HealthOperations ops = newOps(store);
    Response response = ops.health();
    assertEquals(200, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(2, body.getChecks().size());
    assertTrue(body.isUp());
  }

  @Test
  public void testAggregateReturns503WhenEntityStoreDown() {
    HealthOperations ops = newOps(null);
    Response response = ops.health();
    assertEquals(503, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    assertEquals(2, body.getChecks().size());
  }

  @Test
  public void testReadyReturns503WhenEntityStoreTimesOut() throws IOException {
    // Use a latch that is never released so the probe blocks indefinitely.
    // This avoids the flakiness of a fixed sleep that could race on a loaded CI host.
    CountDownLatch neverReleased = new CountDownLatch(1);
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.exists(Mockito.any(), Mockito.any()))
        .thenAnswer(
            invocation -> {
              neverReleased.await();
              return false;
            });
    HealthOperations ops = newOps(store, 100L);
    Response response = ops.ready();
    assertEquals(503, response.getStatus());
    HealthResponse body = (HealthResponse) response.getEntity();
    assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    assertEquals("timeout", body.getChecks().get(0).getDetails().get("reason"));
    neverReleased.countDown();
  }

  @Test
  public void testValidateThrowsWhenStatusIsNull() {
    HealthResponse response = new HealthResponse();
    assertThrows(IllegalArgumentException.class, response::validate);
  }
  /** All probes stay down after OOM, even if the entity store would answer successfully. */
  @Test
  public void testAllEndpointsStayDownAfterOutOfMemory() {
    ServerHealth health = new ServerHealth();
    EntityStore store = Mockito.mock(EntityStore.class);
    HealthOperations ops = newOps(store, 2000L, health);
    health.recordFailure(new OutOfMemoryError("Metaspace"));
    for (Response response : new Response[] {ops.live(), ops.ready(), ops.health()}) {
      try (Response ignored = response) {
        assertEquals(503, response.getStatus());
        HealthResponse body = (HealthResponse) response.getEntity();
        assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
        assertEquals("jvm", body.getChecks().get(0).getName());
        assertEquals(
            "OutOfMemoryError; restart required",
            body.getChecks().get(0).getDetails().get("reason"));
      }
    }
    Mockito.verifyNoInteractions(store);
  }

  /** A captured executor OOM must poison liveness too, and must not recover on the next probe. */
  @Test
  public void testProbeOutOfMemoryIsSticky() throws IOException {
    ServerHealth health = new ServerHealth();
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.exists(Mockito.any(), Mockito.any()))
        .thenThrow(new OutOfMemoryError("Java heap space"))
        .thenReturn(false);
    HealthOperations ops = newOps(store, 2000L, health);
    try (Response first = ops.ready();
        Response live = ops.live();
        Response next = ops.ready()) {
      assertEquals(503, first.getStatus());
      assertEquals(503, live.getStatus());
      assertEquals(503, next.getStatus());
    }
    Mockito.verify(store).exists(Mockito.any(), Mockito.any());
  }

  /** OOM from a timed-out probe must still be recorded when the abandoned task finishes. */
  @Test
  public void testOutOfMemoryAfterProbeTimeout() throws IOException {
    ServerHealth health = new ServerHealth();
    EntityStore store = Mockito.mock(EntityStore.class);
    CountDownLatch release = new CountDownLatch(1);
    Mockito.when(store.exists(Mockito.any(), Mockito.any()))
        .thenAnswer(
            invocation -> {
              release.await();
              throw new OutOfMemoryError("Metaspace");
            });
    try {
      try (Response response = newOps(store, 100L, health).ready()) {
        assertEquals(503, response.getStatus());
        HealthResponse body = (HealthResponse) response.getEntity();
        assertEquals("timeout", body.getChecks().get(0).getDetails().get("reason"));
      }
    } finally {
      release.countDown();
    }
    // If the first worker has not recorded the OOM yet, this probe queues behind it.
    HealthOperations ops = newOps(store, 2000L, health);
    try (Response ready = ops.ready();
        Response live = ops.live()) {
      assertEquals(503, ready.getStatus());
      assertEquals(503, live.getStatus());
      assertTrue(health.hasOutOfMemoryError());
    }
  }
}
