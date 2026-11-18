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
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.IcebergRESTServiceResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports the endpoint of the Gravitino Iceberg REST server, so that clients which already connect
 * to this Gravitino server can discover it instead of requiring it to be configured separately.
 */
@Path("/system/iceberg-rest")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergRESTServiceOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServiceOperations.class);

  private static final String AUX_SERVICE_NAME = "iceberg-rest";

  @Context private HttpServletRequest httpRequest;

  /**
   * Reports the Iceberg REST server's endpoint for the requested metalake.
   *
   * @param metalake the metalake the caller intends to route through the Iceberg REST server; may
   *     be blank, in which case the endpoint is reported regardless of which metalake it serves
   * @return a response whose {@code uri} is {@code null} when the Iceberg REST server is not
   *     running, or does not serve the requested metalake
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "iceberg-rest-service." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "iceberg-rest-service", absolute = true)
  public Response getIcebergRestServiceUri(@QueryParam("metalake") String metalake) {
    String uri;
    try {
      uri =
          Utils.resolveIcebergRestServiceUri(
              getAuxServiceManager(),
              getIcebergRestServiceConfig(),
              metalake,
              getHttpRequest().getServerName());
    } catch (IllegalStateException e) {
      // A misconfiguration, re-reported on every discovery poll until fixed; the message alone
      // identifies it, so the stack trace is omitted from both the log and the response.
      LOG.error("Failed to resolve the Iceberg REST service endpoint: {}", e.getMessage());
      return Utils.internalError(e.getMessage());
    }
    // The reported host can depend on the caller's own Host header, so this
    // response must never be cached and replayed to a different caller.
    return Response.fromResponse(Utils.ok(new IcebergRESTServiceResponse(uri)))
        .header("Cache-Control", "no-store")
        .build();
  }

  // Overridable so tests can inject a fixture without bootstrapping GravitinoEnv, matching
  // HealthOperations's testing pattern.
  AuxiliaryServiceManager getAuxServiceManager() {
    return GravitinoEnv.getInstance().auxServiceManager();
  }

  // Resolved through AuxiliaryServiceManager.getAuxServiceConfig rather than reading
  // gravitino.iceberg-rest.* directly, so the deprecated gravitino.auxService.iceberg-rest.*
  // config form is honored too — the same precedence the Iceberg REST server itself sees.
  Map<String, String> getIcebergRestServiceConfig() {
    return AuxiliaryServiceManager.getAuxServiceConfig(
        GravitinoEnv.getInstance().config(), AUX_SERVICE_NAME);
  }

  HttpServletRequest getHttpRequest() {
    return httpRequest;
  }
}
