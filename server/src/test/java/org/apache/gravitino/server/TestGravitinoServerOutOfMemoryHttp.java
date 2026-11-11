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
package org.apache.gravitino.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.ServerHealth;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises OOM reporting through the production server's filters and Jersey providers. */
@Isolated(
    "Uses the production server environment and restores its shared OOM marker after shutdown")
class TestGravitinoServerOutOfMemoryHttp {
  private static final String[] HEALTH_PATHS = {
    "/api/health",
    "/api/health/live",
    "/api/health/ready",
    "/health",
    "/health/live",
    "/health/ready",
    "/health.html"
  };

  @TempDir File temporaryDirectory;

  /** Adds only failure injection; all health resources and providers come from production. */
  @Path("/oom-wiring/{kind}")
  @Produces("application/vnd.gravitino.v1+json")
  public static class FailingResource {
    /**
     * Simulates a failed allocation without exhausting the test JVM.
     *
     * @param kind the failure to simulate
     * @return a successful response when no failure was requested
     */
    @GET
    public Response get(@PathParam("kind") String kind) {
      fail(kind);
      return Response.ok().build();
    }
  }

  /** Injects failures downstream of the production request-context and audit filters. */
  public static class FailingFilter implements Filter {
    /** {@inheritDoc} */
    @Override
    public void init(FilterConfig config) {}

    /** {@inheritDoc} */
    @Override
    public void destroy() {}

    /** {@inheritDoc} */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      String path = ((HttpServletRequest) request).getRequestURI();
      String prefix = "/api/oom-wiring/filter-";
      if (path.startsWith(prefix)) {
        fail(path.substring(prefix.length()));
      }
      chain.doFilter(request, response);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"oom", "wrapped", "filter-oom", "filter-wrapped"})
  void testProductionWiringRecordsOutOfMemory(String kind) throws Exception {
    ServerHealth health = ServerHealth.getInstance();
    assertFalse(health.hasOutOfMemoryError());
    // Real Jersey resources may be constructed on HTTP worker threads. A thread-local static
    // mock would not isolate those callers, so restore the real marker only after Jetty stops.
    Field marker = ServerHealth.class.getDeclaredField("outOfMemory");
    marker.setAccessible(true);
    int port = RESTUtils.findAvailablePort(0, 0);
    ServerConfig config = new ServerConfig();
    config.loadFromMap(
        Map.of(
            GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            String.valueOf(port),
            GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.CUSTOM_FILTERS.getKey(),
            FailingFilter.class.getName(),
            Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
            temporaryDirectory.toPath().resolve("jdbc").toString(),
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            ""),
        entry -> true);
    GravitinoServer server = new GravitinoServer(config, GravitinoEnv.getInstance());
    try {
      server.initialize();
      server.register(FailingResource.class);
      server.start();
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
      assertHealth(client, port, 200);
      for (String ordinary : new String[] {"ordinary", "filter-ordinary"}) {
        assertEquals(500, get(client, port, "/api/oom-wiring/" + ordinary).statusCode());
        assertHealth(client, port, 200);
        assertFalse(health.hasOutOfMemoryError());
      }
      HttpResponse<String> failure = get(client, port, "/api/oom-wiring/" + kind);
      assertEquals(500, failure.statusCode(), failure.body());
      if (kind.equals("oom")) {
        // Prove that Jersey instantiated the production class-registered ErrorExceptionMapper.
        JsonNode body = ObjectMapperProvider.objectMapper().readTree(failure.body());
        assertEquals("OutOfMemoryError", body.path("type").asText());
        assertEquals(
            "Server error while processing request: java.lang.OutOfMemoryError: Requested array size exceeds VM limit",
            body.path("message").asText());
      }
      assertTrue(health.hasOutOfMemoryError());
      assertHealth(client, port, 503);
      assertEquals(200, get(client, port, "/api/oom-wiring/warm").statusCode());
      assertHealth(client, port, 503);
    } finally {
      try {
        server.stop();
      } finally {
        marker.setBoolean(health, false);
      }
    }
  }

  private static void fail(String kind) {
    switch (kind) {
      case "oom":
        throw new OutOfMemoryError("Requested array size exceeds VM limit");
      case "wrapped":
        throw new IllegalStateException(new OutOfMemoryError("Java heap space"));
      case "ordinary":
        throw new IllegalStateException("ordinary failure");
      default:
        break;
    }
  }

  private static void assertHealth(HttpClient client, int port, int status) throws Exception {
    for (String path : HEALTH_PATHS) {
      HttpResponse<String> response = get(client, port, path);
      assertEquals(status, response.statusCode(), path + ": " + response.body());
      JsonNode body = ObjectMapperProvider.objectMapper().readTree(response.body());
      assertEquals(status == 200 ? "up" : "down", body.path("status").asText(), path);
      if (status == 503) {
        assertEquals(1, body.path("checks").size(), path);
        assertEquals("jvm", body.path("checks").get(0).path("name").asText(), path);
        assertEquals("down", body.path("checks").get(0).path("status").asText(), path);
      }
    }
  }

  private static HttpResponse<String> get(HttpClient client, int port, String path)
      throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(10))
            .header("Accept", "application/vnd.gravitino.v1+json")
            .GET()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
