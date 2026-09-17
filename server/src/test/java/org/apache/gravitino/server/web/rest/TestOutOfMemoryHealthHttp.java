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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.HealthAliasServlet;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.OutOfMemoryErrorListener;
import org.apache.gravitino.server.web.ServerHealth;
import org.apache.gravitino.server.web.mapper.ErrorExceptionMapper;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Exercises an OOM on a separate HTTP request before checking every health alias. */
class TestOutOfMemoryHealthHttp {
  private static final String[] HEALTH_PATHS = {
    "/api/health",
    "/api/health/live",
    "/api/health/ready",
    "/health",
    "/health/live",
    "/health/ready",
    "/health.html"
  };

  /** A warm endpoint and failing requests, independent of the health resources. */
  @Path("/test/{kind}")
  public static class FailingResource {
    /**
     * Simulates errors without exhausting the test worker's memory.
     *
     * @param kind the outcome to simulate
     * @return the successful or ordinary server error response
     */
    @GET
    public Response get(@PathParam("kind") String kind) {
      switch (kind) {
        case "oom":
          throw new OutOfMemoryError("Metaspace");
        case "wrapped":
          throw new IllegalStateException(new OutOfMemoryError("Java heap space"));
        case "ordinary":
          return Response.serverError().build();
        default:
          return Response.ok().build();
      }
    }
  }

  /** Simulates a resource-specific mapper that consumes wrapped errors. */
  public static class RuntimeMapper implements ExceptionMapper<IllegalStateException> {
    /** {@inheritDoc} */
    @Override
    public Response toResponse(IllegalStateException error) {
      return Response.serverError().build();
    }
  }

  /** Supplies a reachable store so an unrelated readiness failure cannot mask the result. */
  public static class TestHealthResource extends HealthOperations {
    private final EntityStore store;

    TestHealthResource(ServerHealth health, EntityStore store) {
      super(health);
      this.store = store;
    }

    @Override
    EntityStore getEntityStore() {
      return store;
    }

    @Override
    long getProbeTimeoutMs() {
      return 2000L;
    }
  }

  @Test
  void directOomPoisonsAllHealthPathsWhileWarmEndpointsStillRespond() throws Exception {
    assertHealthAfterFailure("oom");
  }

  @Test
  void mappedWrappedOomAlsoPoisonsAllHealthPaths() throws Exception {
    assertHealthAfterFailure("wrapped");
  }

  @ParameterizedTest
  @ValueSource(strings = {"filter-oom", "filter-wrapped", "servlet-oom", "servlet-wrapped"})
  void errorsOutsideJerseyPoisonAllHealthPaths(String kind) throws Exception {
    assertHealthAfterFailure(kind);
  }

  private void assertHealthAfterFailure(String failureKind) throws Exception {
    ServerHealth health = new ServerHealth();
    EntityStore store = mock(EntityStore.class);
    when(store.exists(any(), any())).thenReturn(false);
    ResourceConfig config =
        new ResourceConfig()
            .register(new TestHealthResource(health, store))
            .register(FailingResource.class)
            .register(new OutOfMemoryErrorListener(health))
            .register(new ErrorExceptionMapper(health))
            .register(RuntimeMapper.class)
            .register(ObjectMapperProvider.class)
            .register(JacksonFeature.class);
    int port = RESTUtils.findAvailablePort(0, 0);
    Config serverConfig = new Config(false) {};
    serverConfig.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);
    JettyServer server = new JettyServer();
    // Capture an independent health state when the production filter is constructed.
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      server.initialize(JettyServerConfig.fromConfig(serverConfig), "oom-test", false);
    }
    server.addServlet(new ServletContainer(config), "/api/*");
    server.addServlet(new HealthAliasServlet(), "/health/*");
    server.addServlet(new HealthAliasServlet(), "/health.html");
    server.addFilter(
        new Filter() {
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
            if (path.endsWith("filter-oom")) {
              throw new OutOfMemoryError("Metaspace");
            }
            if (path.endsWith("filter-wrapped")) {
              throw new IllegalStateException(new OutOfMemoryError("Java heap space"));
            }
            if (path.endsWith("filter-ordinary")) {
              throw new IllegalStateException("ordinary filter failure");
            }
            chain.doFilter(request, response);
          }
        },
        "/*");
    server.addServlet(
        new HttpServlet() {
          /** {@inheritDoc} */
          @Override
          protected void doGet(HttpServletRequest request, HttpServletResponse response)
              throws IOException {
            if (request.getRequestURI().endsWith("servlet-oom")) {
              throw new OutOfMemoryError("Metaspace");
            }
            if (request.getRequestURI().endsWith("servlet-wrapped")) {
              throw new IllegalStateException(new OutOfMemoryError("Java heap space"));
            }
            throw new IOException("ordinary servlet failure");
          }
        },
        "/outside/*");
    try {
      server.start();
      HttpClient client = HttpClient.newHttpClient();
      for (String path : HEALTH_PATHS) {
        assertEquals(200, get(client, port, path).statusCode(), path);
      }
      assertEquals(500, get(client, port, "/api/test/ordinary").statusCode());
      assertEquals(500, get(client, port, "/api/test/filter-ordinary").statusCode());
      assertEquals(500, get(client, port, "/outside/ordinary").statusCode());
      assertEquals(200, get(client, port, "/api/health").statusCode());
      String failurePath = failureKind.startsWith("servlet-") ? "/outside/" : "/api/test/";
      assertEquals(500, get(client, port, failurePath + failureKind).statusCode());
      // This is the production failure mode: a successful warm endpoint is not proof of recovery.
      assertEquals(200, get(client, port, "/api/test/warm").statusCode());
      for (String path : HEALTH_PATHS) {
        HttpResponse<String> response = get(client, port, path);
        assertEquals(503, response.statusCode(), path);
        JsonNode body = new ObjectMapper().readTree(response.body());
        assertEquals("down", body.path("status").asText());
        assertEquals("jvm", body.path("checks").get(0).path("name").asText());
      }
    } finally {
      server.stop();
    }
  }

  private HttpResponse<String> get(HttpClient client, int port, String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
