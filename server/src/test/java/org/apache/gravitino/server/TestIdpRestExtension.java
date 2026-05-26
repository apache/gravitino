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

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.REST_API_EXTENSION_PACKAGES;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

/**
 * Verifies that built-in IdP REST resources are registered through {@link
 * Configs#REST_API_EXTENSION_PACKAGES} when the {@code idp-basic} plugin is on the server
 * classpath.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestIdpRestExtension {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";

  private static final String IDP_REST_PACKAGE = "org.apache.gravitino.idp.web.rest";

  private int httpPort;

  private Path h2Path;

  @BeforeAll
  void init() throws IOException {
    httpPort = RESTUtils.findAvailablePort(5000, 6000);
    h2Path = Files.createTempDirectory("gravitino_server_idp_rest_it_");
  }

  @AfterAll
  void cleanup() throws IOException {
    if (h2Path != null) {
      FileUtils.deleteDirectory(h2Path.toFile());
    }
  }

  @Test
  public void testWithoutExtension() throws Exception {
    ServerConfig serverConfig = newConfig(false);

    try (ServerHarness harness = startServer(serverConfig)) {
      HttpResponse<String> response = getUser(harness.port(), "missing-user");

      assertEquals(404, response.statusCode());
      assertFalse(
          isNotFoundError(response.body()),
          "IdP REST should not be registered without extensionPackages");
    }
  }

  @Test
  public void testWithExtension() throws Exception {
    ServerConfig serverConfig = newConfig(true);

    try (ServerHarness harness = startServer(serverConfig)) {
      String username = "idp-rest-it-user";
      String password = "Passw0rd-For-User";

      HttpResponse<String> addResponse = addUser(harness.port(), username, password);
      assertEquals(200, addResponse.statusCode(), addResponse.body());

      HttpResponse<String> getResponse = getUser(harness.port(), username);
      assertEquals(200, getResponse.statusCode(), getResponse.body());
      JsonNode userNode = JsonUtils.objectMapper().readTree(getResponse.body()).get("user");
      assertEquals(username, userNode.get("name").asText());

      HttpResponse<String> missingResponse = getUser(harness.port(), "missing-user");
      assertEquals(404, missingResponse.statusCode());
      assertTrue(
          isNotFoundError(missingResponse.body()),
          "Registered IdP REST should return Gravitino NOT_FOUND error body");
    }
  }

  private ServerConfig newConfig(boolean enableExtension) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(
                GravitinoServer.WEBSERVER_CONF_PREFIX
                    + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
                String.valueOf(httpPort))
            .put(ENTITY_STORE.getKey(), RELATIONAL_ENTITY_STORE)
            .put(
                ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
                String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path))
            .put(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER.getKey(), "org.h2.Driver")
            .put(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS.getKey(), "100")
            .put(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS.getKey(), "1000")
            .put(STORE_DELETE_AFTER_TIME.getKey(), String.valueOf(20 * 60 * 1000L))
            .put(CACHE_ENABLED.getKey(), "false")
            .put(ENABLE_AUTHORIZATION.getKey(), "false");

    if (enableExtension) {
      builder.put(REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_PACKAGE);
    }

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(builder.build(), key -> true);

    ServerConfig spyServerConfig = Mockito.spy(serverConfig);
    Mockito.when(
            spyServerConfig.getConfigsWithPrefix(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
    return spyServerConfig;
  }

  private ServerHarness startServer(ServerConfig serverConfig) throws Exception {
    GravitinoServer gravitinoServer = new GravitinoServer(serverConfig, GravitinoEnv.getInstance());
    gravitinoServer.initialize();
    gravitinoServer.start();
    return new ServerHarness(gravitinoServer, httpPort);
  }

  private HttpResponse<String> getUser(int port, String user) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("http://localhost:%d/api/idp/users/%s", port, user)))
            .header("Accept", ACCEPT)
            .GET()
            .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> addUser(int port, String user, String password) throws Exception {
    String body =
        JsonUtils.objectMapper()
            .writeValueAsString(ImmutableMap.of("user", user, "password", password));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("http://localhost:%d/api/idp/users", port)))
            .header("Accept", ACCEPT)
            .header("Content-Type", ACCEPT)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private boolean isNotFoundError(String body) throws IOException {
    JsonNode root = JsonUtils.objectMapper().readTree(body);
    return root.has("code") && root.get("code").asInt() == ErrorConstants.NOT_FOUND_CODE;
  }

  private static final class ServerHarness implements AutoCloseable {

    private final GravitinoServer gravitinoServer;
    private final int port;

    private ServerHarness(GravitinoServer gravitinoServer, int port) {
      this.gravitinoServer = gravitinoServer;
      this.port = port;
    }

    private int port() {
      return port;
    }

    @Override
    public void close() throws IOException {
      gravitinoServer.stop();
    }
  }
}
