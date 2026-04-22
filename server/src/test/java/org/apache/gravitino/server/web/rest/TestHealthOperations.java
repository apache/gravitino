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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHealthOperations {

  private HealthOperations newOps(EntityStore store) {
    return new HealthOperations() {
      @Override
      EntityStore getEntityStore() {
        return store;
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
}
