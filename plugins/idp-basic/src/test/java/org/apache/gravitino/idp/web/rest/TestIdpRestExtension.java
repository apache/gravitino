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
package org.apache.gravitino.idp.web.rest;

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
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

/** Verifies IdP REST resources load via {@link Configs#REST_API_EXTENSION_PACKAGES}. */
@TestInstance(Lifecycle.PER_CLASS)
public class TestIdpRestExtension {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private int httpPort;
  private Path h2Path;

  @BeforeAll
  void init() throws IOException {
    httpPort = RESTUtils.findAvailablePort(5000, 6000);
    h2Path = Files.createTempDirectory("gravitino_idp_rest_extension_it_");
  }

  @AfterAll
  void cleanup() throws IOException {
    if (h2Path != null) {
      FileUtils.deleteDirectory(h2Path.toFile());
    }
  }

  @Test
  public void testWithoutExtension() throws Exception {
    try (ServerHarness harness = startServer(newConfig(false))) {
      HttpResponse<String> response = getUser(harness.port(), "missing-user");
      assertEquals(404, response.statusCode());
      assertFalse(isNotFoundError(response.body()));
    }
  }

  @Test
  public void testWithExtensionWithoutBasicAuthenticator() throws Exception {
    try (ServerHarness harness = startServer(newConfig(true))) {
      HttpResponse<String> response = getUser(harness.port(), "missing-user");
      assertEquals(404, response.statusCode());
      assertFalse(
          isNotFoundError(response.body()),
          "IdP REST routes should not be registered when basic authenticator is disabled");
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
      builder.put(REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_EXTENSION_PACKAGE);
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
    return send(
        HttpRequest.newBuilder()
            .uri(URI.create(String.format("http://localhost:%d/api/idp/users/%s", port, user)))
            .header("Accept", ACCEPT)
            .GET()
            .build());
  }

  private HttpResponse<String> send(HttpRequest request) throws Exception {
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
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
