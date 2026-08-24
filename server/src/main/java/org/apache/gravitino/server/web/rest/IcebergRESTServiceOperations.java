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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.responses.IcebergRESTServiceResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.Utils;

/**
 * Reports the endpoint of the Gravitino Iceberg REST server, so that clients which already connect
 * to this Gravitino server can discover it instead of requiring it to be configured separately.
 */
@Path("/system/iceberg-rest")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergRESTServiceOperations {

  // Matches gravitino.auxService.names / AuxiliaryServiceManager's registration key.
  private static final String AUX_SERVICE_NAME = "iceberg-rest";
  private static final String CONFIG_PREFIX = "gravitino.iceberg-rest.";
  // The post-strip key used by the Iceberg REST server itself; see
  // IcebergConstants.GRAVITINO_METALAKE and DynamicIcebergConfigProvider.
  private static final String SERVED_METALAKE_KEY = CONFIG_PREFIX + "gravitino-metalake";

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
  @Timed(name = "iceberg-rest-service." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "iceberg-rest-service", absolute = true)
  public Response getIcebergRestServiceUri(@QueryParam("metalake") String metalake) {
    return Utils.ok(new IcebergRESTServiceResponse(resolveUri(metalake)));
  }

  private String resolveUri(String metalake) {
    if (!GravitinoEnv.getInstance().auxServiceManager().isAuxServiceRegistered(AUX_SERVICE_NAME)) {
      return null;
    }

    Config config = GravitinoEnv.getInstance().config();
    String servedMetalake = config.getRawString(SERVED_METALAKE_KEY, "");
    if (StringUtils.isNotBlank(metalake)
        && StringUtils.isNotBlank(servedMetalake)
        && !servedMetalake.equals(metalake)) {
      // The Iceberg REST server serves exactly one metalake. Routing a different metalake's
      // catalogs at it would 404 on every request, so report it as unavailable instead.
      return null;
    }

    JettyServerConfig icebergRestConfig = JettyServerConfig.fromConfig(config, CONFIG_PREFIX);
    String host = icebergRestConfig.getHost();
    if (isWildcardHost(host)) {
      // The Iceberg REST server binds to all interfaces, so it has no single externally
      // reachable address of its own. The caller already reached this Gravitino server at some
      // resolvable host, so reuse it — this holds whenever both services share a host, which is
      // the common case, and callers with a genuinely split topology can still set
      // gravitino.iceberg.rest-uri manually.
      host = httpRequest.getServerName();
    }
    String scheme = icebergRestConfig.isEnableHttps() ? "https" : "http";
    return String.format("%s://%s:%d/iceberg", scheme, host, icebergRestConfig.getHttpPort());
  }

  private static boolean isWildcardHost(String host) {
    return StringUtils.isBlank(host) || "0.0.0.0".equals(host) || "::".equals(host);
  }
}
