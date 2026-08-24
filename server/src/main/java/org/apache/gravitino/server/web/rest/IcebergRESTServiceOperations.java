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
import org.apache.commons.lang3.StringUtils;
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

  // Matches gravitino.auxService.names / AuxiliaryServiceManager's registration key.
  private static final String AUX_SERVICE_NAME = "iceberg-rest";
  // Keys below are read from AuxiliaryServiceManager.getAuxServiceConfig, which already strips
  // the gravitino.iceberg-rest. (or deprecated gravitino.auxService.iceberg-rest.) prefix, so
  // they must NOT be re-prefixed here.
  // The provider name used by the Iceberg REST server itself; see
  // IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER and DynamicIcebergConfigProvider. The
  // server module cannot depend on iceberg-common/catalog-common, hence the literal here.
  private static final String CATALOG_CONFIG_PROVIDER_KEY = "catalog-config-provider";
  private static final String DYNAMIC_CATALOG_CONFIG_PROVIDER_NAME = "dynamic-config-provider";
  // The post-strip key used by the Iceberg REST server itself; see
  // IcebergConstants.GRAVITINO_METALAKE and DynamicIcebergConfigProvider.
  private static final String SERVED_METALAKE_KEY = "gravitino-metalake";
  private static final String HOST_KEY = "host";
  private static final String HTTP_PORT_KEY = "httpPort";
  private static final String HTTPS_PORT_KEY = "httpsPort";
  private static final String ENABLE_HTTPS_KEY = "enableHttps";
  // Match IcebergConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT/HTTPS_PORT: the server module
  // cannot depend on iceberg-common, and JettyServerConfig's own defaults are the Gravitino
  // server's (8090/8433), not the Iceberg REST server's — reading raw values with these
  // defaults avoids silently reporting the wrong port when httpPort is not set explicitly.
  private static final int DEFAULT_HTTP_PORT = 9001;
  private static final int DEFAULT_HTTPS_PORT = 9433;
  private static final String DEFAULT_HOST = "0.0.0.0";

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
    // The reported host can depend on the caller's own Host header (see resolveUri), so this
    // response must never be cached and replayed to a different caller.
    return Response.fromResponse(Utils.ok(new IcebergRESTServiceResponse(resolveUri(metalake))))
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

  private String resolveUri(String metalake) {
    if (!getAuxServiceManager().isAuxServiceRegistered(AUX_SERVICE_NAME)) {
      return null;
    }

    Map<String, String> config = getIcebergRestServiceConfig();
    String provider = config.getOrDefault(CATALOG_CONFIG_PROVIDER_KEY, "");
    if (!DYNAMIC_CATALOG_CONFIG_PROVIDER_NAME.equals(provider)) {
      // Only the dynamic catalog config provider maps Iceberg REST catalog names onto Gravitino
      // catalogs; the default static provider serves statically-declared catalogs unrelated to
      // Gravitino catalog names, so routing at it would 404 on every request.
      LOG.debug(
          "Iceberg REST service does not use the dynamic catalog config provider "
              + "(catalog-config-provider={}); not reporting its endpoint for auto-discovery.",
          provider);
      return null;
    }

    String servedMetalake = config.getOrDefault(SERVED_METALAKE_KEY, "");
    if (StringUtils.isNotBlank(metalake)
        && StringUtils.isNotBlank(servedMetalake)
        && !servedMetalake.equals(metalake)) {
      // The Iceberg REST server serves exactly one metalake. Routing a different metalake's
      // catalogs at it would 404 on every request, so report it as unavailable instead.
      LOG.debug(
          "Iceberg REST service serves metalake {}, not the requested metalake {}; not "
              + "reporting its endpoint for auto-discovery.",
          servedMetalake,
          metalake);
      return null;
    }

    String host = config.getOrDefault(HOST_KEY, DEFAULT_HOST);
    if (isWildcardHost(host)) {
      // The Iceberg REST server binds to all interfaces, so it has no single externally
      // reachable address of its own. The caller already reached this Gravitino server at some
      // resolvable host, so reuse it — this holds whenever both services share a host, which is
      // the common case, and callers with a genuinely split topology can still set
      // gravitino.iceberg.rest-uri manually.
      host = getHttpRequest().getServerName();
    }
    boolean enableHttps = Boolean.parseBoolean(config.getOrDefault(ENABLE_HTTPS_KEY, "false"));
    String scheme = enableHttps ? "https" : "http";
    int port =
        parsePort(
            config,
            enableHttps ? HTTPS_PORT_KEY : HTTP_PORT_KEY,
            enableHttps ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT);
    return String.format("%s://%s:%d/iceberg", scheme, host, port);
  }

  private static int parsePort(Map<String, String> config, String key, int defaultPort) {
    String value = config.getOrDefault(key, "");
    if (StringUtils.isBlank(value)) {
      return defaultPort;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return defaultPort;
    }
  }

  private static boolean isWildcardHost(String host) {
    return StringUtils.isBlank(host)
        || "0.0.0.0".equals(host)
        || "::".equals(host)
        || "[::]".equals(host)
        || "0:0:0:0:0:0:0:0".equals(host);
  }
}
