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

import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.server.web.ServerHealth;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestIcebergHealthOperations {

  /** Verifies the documented status casing with the service's actual JSON mapper. */
  @Test
  public void testSerializedHealthStatus() throws Exception {
    ServerHealth health = new ServerHealth();
    IcebergHealthOperations operations = new IcebergHealthOperations(health);
    ObjectMapper mapper = IcebergObjectMapper.getInstance();
    try (Response response = operations.live()) {
      JsonNode json = mapper.readTree(mapper.writeValueAsString(response.getEntity()));
      Assertions.assertEquals("UP", json.path("status").asText());
      Assertions.assertEquals("UP", json.path("checks").get(0).path("status").asText());
    }
    health.recordFailure(new OutOfMemoryError("Metaspace"));
    try (Response response = operations.live()) {
      JsonNode json = mapper.readTree(mapper.writeValueAsString(response.getEntity()));
      Assertions.assertEquals(503, response.getStatus());
      Assertions.assertEquals("DOWN", json.path("status").asText());
      Assertions.assertEquals("DOWN", json.path("checks").get(0).path("status").asText());
      Assertions.assertEquals("jvm", json.path("checks").get(0).path("name").asText());
    }
  }

  private static IcebergHealthOperations operationsWithManager(
      IcebergCatalogWrapperManager manager) {
    return new IcebergHealthOperations(new ServerHealth()) {
      @Override
      IcebergCatalogWrapperManager getCatalogWrapperManager() {
        return manager;
      }
    };
  }

  @Test
  public void testLiveReturns200() {
    IcebergHealthOperations ops = operationsWithManager(null);
    Response resp = ops.live();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
  }

  @Test
  public void testReadyReturns200WhenManagerInitialized() {
    IcebergCatalogWrapperManager manager = mock(IcebergCatalogWrapperManager.class);
    IcebergHealthOperations ops = operationsWithManager(manager);
    Response resp = ops.ready();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
  }

  @Test
  public void testReadyReturns503WhenManagerNotInitialized() {
    IcebergHealthOperations ops = operationsWithManager(null);
    Response resp = ops.ready();
    Assertions.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    Assertions.assertFalse(body.getChecks().isEmpty());
    Assertions.assertEquals("catalogWrapperManager", body.getChecks().get(0).getName());
  }

  @Test
  public void testHealthReturns200WhenManagerInitialized() {
    IcebergCatalogWrapperManager manager = mock(IcebergCatalogWrapperManager.class);
    IcebergHealthOperations ops = operationsWithManager(manager);
    Response resp = ops.health();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
    Assertions.assertEquals(2, body.getChecks().size());
  }

  @Test
  public void testHealthReturns503WhenManagerNotInitialized() {
    IcebergHealthOperations ops = operationsWithManager(null);
    Response resp = ops.health();
    Assertions.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
    boolean hasCatalogCheck =
        body.getChecks().stream().anyMatch(c -> "catalogWrapperManager".equals(c.getName()));
    Assertions.assertTrue(hasCatalogCheck);
  }

  /** Verifies mapped direct and wrapped OOM disable all health probes. */
  @Test
  public void testMappedOutOfMemoryMakesAllProbesUnhealthy() {
    for (Throwable failure :
        new Throwable[] {
          new OutOfMemoryError("Metaspace"),
          new IllegalStateException(new OutOfMemoryError("Java heap space"))
        }) {
      ServerHealth health = new ServerHealth();
      IcebergHealthOperations ops =
          new IcebergHealthOperations(health) {
            @Override
            IcebergCatalogWrapperManager getCatalogWrapperManager() {
              Assertions.fail("Readiness must skip initialization checks after OOM");
              return null;
            }
          };
      try (MockedStatic<ServerHealth> shared = Mockito.mockStatic(ServerHealth.class)) {
        shared.when(ServerHealth::getInstance).thenReturn(health);
        try (Response response = IcebergExceptionMapper.toRESTResponse(failure)) {
          Assertions.assertEquals(500, response.getStatus());
        }
      }
      for (Supplier<Response> probe :
          List.<Supplier<Response>>of(ops::live, ops::ready, ops::health)) {
        try (Response response = probe.get()) {
          Assertions.assertEquals(503, response.getStatus());
          HealthResponse body = (HealthResponse) response.getEntity();
          Assertions.assertEquals(HealthCheckDTO.Status.DOWN, body.getStatus());
          Assertions.assertEquals(1, body.getChecks().size());
          Assertions.assertEquals("jvm", body.getChecks().get(0).getName());
          Assertions.assertEquals(
              "OutOfMemoryError; restart required",
              body.getChecks().get(0).getDetails().get("reason"));
        }
      }
      Assertions.assertTrue(health.hasOutOfMemoryError());
    }
  }

  /** Verifies an ordinary mapped failure leaves liveness healthy. */
  @Test
  public void testOrdinaryMappedFailureDoesNotPoisonLiveness() {
    ServerHealth health = new ServerHealth();
    Throwable failure = new IllegalStateException("ordinary failure");
    try (MockedStatic<ServerHealth> shared = Mockito.mockStatic(ServerHealth.class)) {
      shared.when(ServerHealth::getInstance).thenReturn(health);
      try (Response response = IcebergExceptionMapper.toRESTResponse(failure)) {
        Assertions.assertEquals(500, response.getStatus());
      }
    }
    try (Response response = new IcebergHealthOperations(health).live()) {
      Assertions.assertEquals(200, response.getStatus());
    }
  }

  /** Verifies OOM recorded during initialization checks overrides their successful result. */
  @Test
  public void testOutOfMemoryObservedDuringReadinessOverridesSuccess() {
    for (boolean aggregate : new boolean[] {false, true}) {
      ServerHealth health = new ServerHealth();
      IcebergCatalogWrapperManager dependency = mock(IcebergCatalogWrapperManager.class);
      IcebergHealthOperations ops =
          new IcebergHealthOperations(health) {
            @Override
            IcebergCatalogWrapperManager getCatalogWrapperManager() {
              health.recordFailure(new OutOfMemoryError("Metaspace"));
              return dependency;
            }
          };
      try (Response response = aggregate ? ops.health() : ops.ready()) {
        Assertions.assertEquals(503, response.getStatus());
        HealthResponse body = (HealthResponse) response.getEntity();
        Assertions.assertEquals("jvm", body.getChecks().get(0).getName());
      }
    }
  }
}
