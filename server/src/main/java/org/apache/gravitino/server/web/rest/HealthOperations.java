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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Health check endpoints following MicroProfile Health semantics. Exposes separate liveness,
 * readiness, and aggregate endpoints so Kubernetes probes, load balancers, and global traffic
 * managers can distinguish "restart this pod" from "route traffic elsewhere."
 *
 * <ul>
 *   <li>{@code GET /api/health/live} — liveness, 200 as long as the HTTP thread can respond
 *   <li>{@code GET /api/health/ready} — readiness, 200 when entity store is reachable
 *   <li>{@code GET /api/health} — aggregate, 200 when both pass
 * </ul>
 *
 * All endpoints return 503 with a JSON body describing the failed check(s) when unhealthy.
 */
@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class HealthOperations extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(HealthOperations.class);

  private static final long ENTITY_STORE_PROBE_TIMEOUT_MS = 2000L;
  private static final String HEALTH_PROBE_SENTINEL = "gravitino_health_probe";

  private static final String CHECK_HTTP_SERVER = "httpServer";
  private static final String CHECK_ENTITY_STORE = "entityStore";

  /**
   * Default constructor for Jersey auto-discovery. The entity store is resolved lazily at request
   * time via {@link #getEntityStore()} so that probes issued before {@link GravitinoEnv} has
   * finished initializing report DOWN rather than throwing NullPointerException.
   */
  public HealthOperations() {}

  @GET
  @Path("/live")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "health.live." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "health.live", absolute = true)
  public Response live() {
    HealthCheckDTO check = up(CHECK_HTTP_SERVER, Collections.emptyMap());
    return ok(new HealthResponse(HealthCheckDTO.Status.UP, Collections.singletonList(check)));
  }

  @GET
  @Path("/ready")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "health.ready." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "health.ready", absolute = true)
  public Response ready() {
    HealthCheckDTO entityStoreCheck = checkEntityStore();
    HealthCheckDTO.Status overall = entityStoreCheck.getStatus();
    HealthResponse body = new HealthResponse(overall, Collections.singletonList(entityStoreCheck));
    return overall == HealthCheckDTO.Status.UP ? ok(body) : serviceUnavailable(body);
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "health." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "health", absolute = true)
  public Response health() {
    List<HealthCheckDTO> checks = new ArrayList<>(2);
    checks.add(up(CHECK_HTTP_SERVER, Collections.emptyMap()));
    checks.add(checkEntityStore());

    HealthCheckDTO.Status overall =
        checks.stream().anyMatch(c -> c.getStatus() == HealthCheckDTO.Status.DOWN)
            ? HealthCheckDTO.Status.DOWN
            : HealthCheckDTO.Status.UP;

    HealthResponse body = new HealthResponse(overall, checks);
    return overall == HealthCheckDTO.Status.UP ? ok(body) : serviceUnavailable(body);
  }

  private HealthCheckDTO checkEntityStore() {
    EntityStore entityStore = getEntityStore();
    if (entityStore == null) {
      return down(CHECK_ENTITY_STORE, "reason", "entity store not initialized");
    }

    try {
      CompletableFuture<Boolean> future =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return entityStore.exists(
                      NameIdentifier.of(HEALTH_PROBE_SENTINEL), EntityType.METALAKE);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
      future.get(ENTITY_STORE_PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return up(CHECK_ENTITY_STORE, Collections.emptyMap());
    } catch (TimeoutException e) {
      LOG.warn("Entity store probe timed out after {}ms", ENTITY_STORE_PROBE_TIMEOUT_MS);
      return down(CHECK_ENTITY_STORE, "reason", "timeout");
    } catch (ExecutionException e) {
      // Unwrap the RuntimeException we used inside supplyAsync to tunnel checked IOException.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException && cause.getCause() instanceof IOException) {
        cause = cause.getCause();
      }
      if (cause == null) {
        cause = e;
      }
      LOG.warn("Entity store probe failed: {}", cause.toString());
      return down(CHECK_ENTITY_STORE, "reason", cause.getClass().getSimpleName());
    } catch (Exception e) {
      LOG.warn("Entity store probe encountered unexpected error", e);
      return down(CHECK_ENTITY_STORE, "reason", e.getClass().getSimpleName());
    }
  }

  /** Visible for testing — subclasses override to inject a mock entity store. */
  EntityStore getEntityStore() {
    try {
      return GravitinoEnv.getInstance().entityStore();
    } catch (Exception e) {
      LOG.debug("Unable to resolve entity store from GravitinoEnv", e);
      return null;
    }
  }

  private static HealthCheckDTO up(String name, Map<String, String> details) {
    return new HealthCheckDTO(name, HealthCheckDTO.Status.UP, details);
  }

  private static HealthCheckDTO down(String name, String detailKey, String detailValue) {
    Map<String, String> details = new HashMap<>(1);
    details.put(detailKey, detailValue);
    return new HealthCheckDTO(name, HealthCheckDTO.Status.DOWN, details);
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
