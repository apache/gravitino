/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs {@link VersioningFilter} in front of real Jersey resources. The filter rewrites the {@code
 * Accept} header that Jersey reads, so resources that produce a plain {@code application/json}
 * (such as the OpenLineage endpoint) can only be checked together with Jersey's content
 * negotiation, which a mocked servlet request does not exercise.
 */
public class TestVersioningFilterWithJerseyResource {

  private static final String VERSIONED_JSON = "application/vnd.gravitino.v1+json";

  private static Server server;
  private static int port;

  /** A resource that produces a plain {@code application/json}, like the OpenLineage endpoint. */
  @Path("/json-only")
  public static class JsonOnlyResource {
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(String body) {
      return Response.status(Response.Status.CREATED).entity("{}").build();
    }
  }

  /** A resource that produces the versioned media type, like the Gravitino REST resources. */
  @Path("/versioned")
  public static class VersionedResource {
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(VERSIONED_JSON)
    public Response post(String body) {
      return Response.status(Response.Status.CREATED).entity("{}").build();
    }
  }

  @BeforeAll
  public static void startServer() throws Exception {
    server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setHost("127.0.0.1");
    connector.setPort(0);
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addFilter(
        new FilterHolder(new VersioningFilter()), "/api/*", EnumSet.of(DispatcherType.REQUEST));
    ResourceConfig resourceConfig =
        new ResourceConfig(JsonOnlyResource.class, VersionedResource.class);
    context.addServlet(new ServletHolder(new ServletContainer(resourceConfig)), "/api/*");
    server.setHandler(context);
    server.start();
    port = connector.getLocalPort();
  }

  @AfterAll
  public static void stopServer() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"*/*", "application/json", "application/json, text/plain;q=0.5"})
  public void testJsonOnlyResourceAcceptsRequestsWithoutVersionedAccept(String accept)
      throws Exception {
    assertEquals(201, post("/api/json-only", accept));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"*/*", "application/json", VERSIONED_JSON})
  public void testVersionedResourceStillMatches(String accept) throws Exception {
    assertEquals(201, post("/api/versioned", accept));
  }

  @Test
  public void testJsonOnlyResourceRejectsUnrelatedAccept() throws Exception {
    // The client's own preference is kept, so an unrelated media type is still not acceptable.
    assertEquals(406, post("/api/json-only", "text/plain"));
  }

  private static int post(String path, String accept) throws Exception {
    // java.net.http.HttpClient sends no Accept header unless one is set, unlike HttpURLConnection.
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .header("Content-Type", MediaType.APPLICATION_JSON)
            .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (accept != null) {
      request.header("Accept", accept);
    }
    return HttpClient.newHttpClient()
        .send(request.build(), HttpResponse.BodyHandlers.discarding())
        .statusCode();
  }
}
