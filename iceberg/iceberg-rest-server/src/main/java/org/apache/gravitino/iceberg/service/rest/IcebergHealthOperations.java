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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.metrics.MetricNames;

/**
 * Health check endpoints for the Iceberg REST server. Follows the same MicroProfile Health
 * semantics as the main Gravitino server.
 *
 * <ul>
 *   <li>{@code GET /iceberg/health/live} — liveness, 200 as long as the HTTP thread can respond
 *   <li>{@code GET /iceberg/health/ready} — readiness, 200 when the catalog wrapper manager is
 *       initialized
 *   <li>{@code GET /iceberg/health} — aggregate, 200 when both pass
 * </ul>
 *
 * All endpoints return 503 with a JSON body describing the failed check(s) when unhealthy.
 */
@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class IcebergHealthOperations {

  private static final String CHECK_HTTP_SERVER = "httpServer";
  private static final String CHECK_CATALOG_WRAPPER_MANAGER = "catalogWrapperManager";

  @Inject private IcebergCatalogWrapperManager catalogWrapperManager;

  /** Default constructor for Jersey auto-discovery. */
  public IcebergHealthOperations() {}

  @GET
  @Path("/live")
  @Timed(name = "iceberg.health.live." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "iceberg.health.live", absolute = true)
  public Response live() {
    HealthCheckDTO check = up(CHECK_HTTP_SERVER, Collections.emptyMap());
    return ok(new HealthResponse(HealthCheckDTO.Status.UP, Collections.singletonList(check)));
  }

  @GET
  @Path("/ready")
  @Timed(name = "iceberg.health.ready." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "iceberg.health.ready", absolute = true)
  public Response ready() {
    HealthCheckDTO managerCheck = checkCatalogWrapperManager();
    HealthCheckDTO.Status overall = managerCheck.getStatus();
    HealthResponse body = new HealthResponse(overall, Collections.singletonList(managerCheck));
    return overall == HealthCheckDTO.Status.UP ? ok(body) : serviceUnavailable(body);
  }

  @GET
  @Timed(name = "iceberg.health." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "iceberg.health", absolute = true)
  public Response health() {
    List<HealthCheckDTO> checks = new ArrayList<>(2);
    checks.add(up(CHECK_HTTP_SERVER, Collections.emptyMap()));
    checks.add(checkCatalogWrapperManager());

    HealthCheckDTO.Status overall =
        checks.stream().anyMatch(c -> c.getStatus() == HealthCheckDTO.Status.DOWN)
            ? HealthCheckDTO.Status.DOWN
            : HealthCheckDTO.Status.UP;

    HealthResponse body = new HealthResponse(overall, checks);
    return overall == HealthCheckDTO.Status.UP ? ok(body) : serviceUnavailable(body);
  }

  private HealthCheckDTO checkCatalogWrapperManager() {
    if (getCatalogWrapperManager() == null) {
      return down(
          CHECK_CATALOG_WRAPPER_MANAGER, "reason", "catalog wrapper manager not initialized");
    }
    return up(CHECK_CATALOG_WRAPPER_MANAGER, Collections.emptyMap());
  }

  /** Visible for testing — subclasses override to inject a different manager instance. */
  IcebergCatalogWrapperManager getCatalogWrapperManager() {
    return catalogWrapperManager;
  }

  private static HealthCheckDTO up(String name, java.util.Map<String, String> details) {
    return new HealthCheckDTO(name, HealthCheckDTO.Status.UP, details);
  }

  private static HealthCheckDTO down(String name, String detailKey, String detailValue) {
    return new HealthCheckDTO(
        name, HealthCheckDTO.Status.DOWN, Collections.singletonMap(detailKey, detailValue));
  }

  private static Response ok(HealthResponse body) {
    return Response.status(Response.Status.OK)
        .entity(body)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  private static Response serviceUnavailable(HealthResponse body) {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
        .entity(body)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
