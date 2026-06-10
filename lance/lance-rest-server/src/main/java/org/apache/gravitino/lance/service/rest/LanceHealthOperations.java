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
import java.util.Collections;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;

/**
 * Health check endpoints for the Lance REST server. Follows the same MicroProfile Health semantics
 * as the main Gravitino server.
 *
 * <ul>
 *   <li>{@code GET /lance/health/live} — liveness, 200 as long as the HTTP thread can respond
 * </ul>
 *
 * Returns 200 with a JSON body describing the healthy check.
 */
@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class LanceHealthOperations {
  private static final String CHECK_HTTP_SERVER = "httpServer";

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
    HealthCheckDTO check =
        new HealthCheckDTO(
            CHECK_HTTP_SERVER, HealthCheckDTO.Status.UP, Collections.<String, String>emptyMap());
    HealthResponse healthResponse =
        new HealthResponse(HealthCheckDTO.Status.UP, Collections.singletonList(check));
    return Utils.ok(healthResponse);
  }
}
