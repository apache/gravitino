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
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.server.web.HealthAliasServlet;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.server.web.OutOfMemoryErrorListener;
import org.apache.gravitino.server.web.ServerHealth;
import org.apache.gravitino.server.web.mapper.ErrorExceptionMapper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.Test;

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
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new ServletContainer(config)), "/api/*");
    context.addServlet(new ServletHolder(new HealthAliasServlet()), "/health/*");
    context.addServlet(new ServletHolder(new HealthAliasServlet()), "/health.html");
    try {
      server.start();
      int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
      HttpClient client = HttpClient.newHttpClient();
      for (String path : HEALTH_PATHS) {
        assertEquals(200, get(client, port, path).statusCode(), path);
      }
      assertEquals(500, get(client, port, "/api/test/ordinary").statusCode());
      assertEquals(200, get(client, port, "/api/health").statusCode());
      assertEquals(500, get(client, port, "/api/test/" + failureKind).statusCode());
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
