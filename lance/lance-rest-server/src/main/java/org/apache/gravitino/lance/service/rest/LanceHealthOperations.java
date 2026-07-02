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
package org.apache.gravitino.lance.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;

/**
 * Health check endpoints for the Lance REST server. Follows the same MicroProfile Health semantics
 * as the main Gravitino server.
 *
 * <ul>
 *   <li>{@code GET /lance/health/live} — liveness, 200 as long as the HTTP thread can respond
 *   <li>{@code GET /lance/health/ready} — readiness, 200 when the namespace wrapper is initialized
 *   <li>{@code GET /lance/health} — aggregate, 200 when both pass
 * </ul>
 *
 * All endpoints return 503 with a JSON body describing the failed check(s) when unhealthy.
 */
@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class LanceHealthOperations {

  private static final String CHECK_HTTP_SERVER = "httpServer";
  private static final String CHECK_NAMESPACE_WRAPPER = "namespaceWrapper";

  @Inject private NamespaceWrapper namespaceWrapper;

  /** Default constructor for Jersey auto-discovery. */
  public LanceHealthOperations() {}

  /**
   * Liveness probe. Returns 200 as long as the HTTP thread can respond.
   *
   * @return 200 OK with an UP {@link HealthResponse}
   */
  @GET
  @Path("/live")
  @Timed(name = "lance.health.live." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "lance.health.live", absolute = true)
  public Response live() {
    HealthCheckDTO check = up(CHECK_HTTP_SERVER, Collections.emptyMap());
    HealthResponse healthResponse =
        new HealthResponse(HealthCheckDTO.Status.UP, Collections.singletonList(check));
    return Utils.ok(healthResponse);
  }

  /**
   * Readiness probe. Returns 200 when the {@link NamespaceWrapper} is initialized, 503 otherwise.
   *
   * @return 200 OK when ready, 503 Service Unavailable with a DOWN {@link HealthResponse} otherwise
   */
  @GET
  @Path("/ready")
  @Timed(name = "lance.health.ready." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "lance.health.ready", absolute = true)
  public Response ready() {
    HealthCheckDTO namespaceCheck = checkNamespaceWrapper();
    HealthCheckDTO.Status overall = namespaceCheck.getStatus();
    HealthResponse body = new HealthResponse(overall, Collections.singletonList(namespaceCheck));
    return overall == HealthCheckDTO.Status.UP ? Utils.ok(body) : Utils.serviceUnavailable(body);
  }

  /**
   * Aggregate health check. Returns 200 when both liveness and readiness pass, 503 otherwise.
   *
   * @return 200 OK when healthy, 503 Service Unavailable with failing checks described in the body
   */
  @GET
  @Timed(name = "lance.health." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "lance.health", absolute = true)
  public Response health() {
    List<HealthCheckDTO> checks = new ArrayList<>(2);
    checks.add(up(CHECK_HTTP_SERVER, Collections.emptyMap()));
    checks.add(checkNamespaceWrapper());

    HealthCheckDTO.Status overall =
        checks.stream().anyMatch(c -> c.getStatus() == HealthCheckDTO.Status.DOWN)
            ? HealthCheckDTO.Status.DOWN
            : HealthCheckDTO.Status.UP;

    HealthResponse body = new HealthResponse(overall, checks);
    return overall == HealthCheckDTO.Status.UP ? Utils.ok(body) : Utils.serviceUnavailable(body);
  }

  private HealthCheckDTO checkNamespaceWrapper() {
    NamespaceWrapper wrapper = getNamespaceWrapper();
    if (wrapper == null) {
      return down(CHECK_NAMESPACE_WRAPPER, "reason", "namespace wrapper not initialized");
    }
    if (wrapper.isInitialized()) {
      return up(CHECK_NAMESPACE_WRAPPER, Collections.emptyMap());
    } else {
      return down(CHECK_NAMESPACE_WRAPPER, "reason", "namespace wrapper not initialized");
    }
  }

  /** Visible for testing — subclasses override to inject a different wrapper instance. */
  NamespaceWrapper getNamespaceWrapper() {
    return namespaceWrapper;
  }

  private static HealthCheckDTO up(String name, Map<String, String> details) {
    return new HealthCheckDTO(name, HealthCheckDTO.Status.UP, details);
  }

  private static HealthCheckDTO down(String name, String detailKey, String detailValue) {
    return new HealthCheckDTO(
        name, HealthCheckDTO.Status.DOWN, Collections.singletonMap(detailKey, detailValue));
  }
}
