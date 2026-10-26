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
package org.apache.gravitino.maintenance.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceActionHttpClient}. */
public class TestMaintenanceActionHttpClient {

  private HttpServer server;
  private final AtomicReference<String> auth = new AtomicReference<>();
  private final AtomicReference<String> path = new AtomicReference<>();
  private final AtomicReference<String> body = new AtomicReference<>();

  @BeforeEach
  public void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          path.set(exchange.getRequestURI().getPath());
          body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] payload = "{\"code\":0}".getBytes(StandardCharsets.UTF_8);
          int status = 200;
          if (exchange.getRequestURI().getPath().endsWith("/token")) {
            payload =
                "{\"access_token\":\"tok-1\",\"token_type\":\"bearer\",\"expires_in\":3600}"
                    .getBytes(StandardCharsets.UTF_8);
          }
          if (exchange.getRequestURI().getPath().contains("/fail")) {
            status = 500;
            payload = "{\"code\":-1}".getBytes(StandardCharsets.UTF_8);
          }
          exchange.sendResponseHeaders(status, payload.length);
          exchange.getResponseBody().write(payload);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  public void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testSimpleAuthPostsMetricsAndFinish() {
    Map<String, String> options = baseOptions();
    options.put("auth_type", "simple");
    options.put("username", "alice");
    try (MaintenanceActionHttpClient client =
        MaintenanceActionHttpClient.fromUpdaterOptions(options)) {
      client.reportBeforeMetrics(7L, new MaintenanceActionMetricsRequest(1L, 20L, 2L, "{}"));
      assertEquals("/api/metalakes/demo/maintenance/actions/7/metrics/before", path.get());
      assertTrue(body.get().contains("\"tableId\":1"));
      assertTrue(body.get().contains("\"policyId\":20"));
      client.finish(7L, new MaintenanceActionFinishRequest(1L, 20L, 11L));
      assertEquals("/api/metalakes/demo/maintenance/actions/7/finish", path.get());
    }
    assertEquals(basic("alice", "dummy"), auth.get());
  }

  @Test
  public void testOauthFetchesBearerToken() {
    Map<String, String> options = baseOptions();
    options.put("auth_type", "oauth");
    options.put("oauth_server_uri", gravitinoUri());
    options.put("oauth_path", "oauth/token");
    options.put("oauth_credential", "id:secret");
    options.put("oauth_scope", "openid");
    try (MaintenanceActionHttpClient client =
        MaintenanceActionHttpClient.fromUpdaterOptions(options)) {
      client.reportBeforeMetrics(1L, new MaintenanceActionMetricsRequest(1L, 20L, 2L, "{}"));
    }
    assertEquals("Bearer tok-1", auth.get());
    assertTrue(path.get().endsWith("/metrics/before"));
  }

  @Test
  public void testHttpErrorFails() {
    Map<String, String> options = baseOptions();
    options.put("gravitino_uri", gravitinoUri() + "/fail");
    MaintenanceActionHttpClient client = MaintenanceActionHttpClient.fromUpdaterOptions(options);
    assertThrows(
        IllegalStateException.class,
        () ->
            client.reportBeforeMetrics(1L, new MaintenanceActionMetricsRequest(1L, 20L, 2L, "{}")));
    client.close();
  }

  private Map<String, String> baseOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("gravitino_uri", gravitinoUri());
    options.put("metalake", "demo");
    return options;
  }

  private String gravitinoUri() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static String basic(String user, String password) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
