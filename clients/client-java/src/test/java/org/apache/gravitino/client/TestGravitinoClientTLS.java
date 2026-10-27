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

package org.apache.gravitino.client;

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.server.web.TestTlsServerUtils.TEST_STORE_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.TestTlsServerUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoClientTLS {
  private TestGravitinoServer testServer;
  private GravitinoAdminClient adminClient;
  private boolean cleanupMetalake = false;

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (adminClient != null) {
        if (cleanupMetalake) {
          adminClient.dropMetalake("metalake", true);
        }
      }
    } finally {
      if (testServer != null) {
        testServer.stop();
      }
    }
  }

  @Test
  void testGravitinoClientmTLSConfigurer() throws Exception {
    // set up a gravitino server with client auth enabled
    // create a tls config, with a client cert
    // create a metalake utilizing the admin client (must include the tls config)
    // create a gravitino client with the tls config
    // make the gravitino client request to the server and verify that it succeeds

    testServer = startGravitinoServer(true, true);
    TLSConfigurer tlsConfigurer = createTestTlsConfigurer(true);

    // create an admin client with the tls config, to then create a metalake
    adminClient =
        GravitinoAdminClient.builder(testServer.uri()).withTlsConfigurer(tlsConfigurer).build();
    adminClient.createMetalake("metalake", "test metalake", Map.of());
    cleanupMetalake = true;

    // Create the gravitino client to run a user request against the server, with the tls config
    GravitinoClient gravitinoClient =
        createGravitinoClient(testServer.uri(), "metalake", tlsConfigurer);

    String[] catalogs = gravitinoClient.listCatalogs();

    // verify that the request succeeds and returns an empty list of catalogs (expected)
    assertEquals(0, catalogs.length);
  }

  @Test
  void testGravitinoClientmTLSConfigurerRejectsMissingClientCert() throws Exception {
    // set up a gravitino server with client auth enabled
    // create a tls config, without a client cert
    // Create a gravitino admin client with the tls config
    // Attempt to make a request to the server, which should fail

    testServer = startGravitinoServer(true, true);
    TLSConfigurer tlsConfigurer = createTestTlsConfigurer(false);

    // Create an admin client with the tls config, to then create a metalake
    adminClient =
        GravitinoAdminClient.builder(testServer.uri()).withTlsConfigurer(tlsConfigurer).build();

    assertThrows(
        RESTException.class,
        () -> adminClient.createMetalake("metalake", "test metalake", Map.of()));
  }

  @Test
  void testGravitinoClientTLSConfigurer() throws Exception {
    // set up a gravitino server without client auth, but with tls config
    // create a tls config without a client cert
    // create a metalake utilizing the admin client (must include the tls config)
    // create a gravitino client with the tls config
    // make the gravitino client request to the server and verify that it succeeds

    testServer = startGravitinoServer(false, true);
    TLSConfigurer tlsConfigurer = createTestTlsConfigurer(false);

    adminClient =
        GravitinoAdminClient.builder(testServer.uri()).withTlsConfigurer(tlsConfigurer).build();
    adminClient.createMetalake("metalake", "test metalake", Map.of());
    cleanupMetalake = true;

    GravitinoClient gravitinoClient =
        createGravitinoClient(testServer.uri(), "metalake", tlsConfigurer);

    String[] catalogs = gravitinoClient.listCatalogs();

    // verify that the request succeeds and returns an empty list of catalogs (expected)
    assertEquals(0, catalogs.length);
  }

  @Test
  void testGravitinoClientHTTP() throws Exception {
    // set up a gravitino server without client auth or tls config (http only)
    // create a metalake utilizing the admin client
    // create a gravitino client
    // make the gravitino client request to the server and verify that it succeeds

    testServer = startGravitinoServer(false, false);

    adminClient = GravitinoAdminClient.builder(testServer.uri()).build();
    adminClient.createMetalake("metalake", "test metalake", Map.of());
    cleanupMetalake = true;

    GravitinoClient gravitinoClient =
        GravitinoClient.builder(testServer.uri()).withMetalake("metalake").build();

    String[] catalogs = gravitinoClient.listCatalogs();

    // verify that the request succeeds and returns an empty list of catalogs (expected)
    assertEquals(0, catalogs.length);
  }

  public TLSConfigurer createTestTlsConfigurer(Boolean clientAuth) throws Exception {
    if (clientAuth) {
      return TLSConfigurers.builder()
          .trustStore(
              TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
          .keyStore(
              TestTlsServerUtils.testResource("test-trusted-client-keystore.p12"),
              TEST_STORE_PASSWORD)
          .build();
    } else {
      return TLSConfigurers.builder()
          .trustStore(
              TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
          .build();
    }
  }

  public GravitinoClient createGravitinoClient(
      String uri, String metalake, TLSConfigurer tlsConfigurer) {
    return GravitinoClient.builder(uri)
        .withMetalake(metalake)
        .withTlsConfigurer(tlsConfigurer)
        .build();
  }

  public record TestGravitinoServer(
      GravitinoServer server, int port, Path backendDir, boolean tlsEnabled) {
    public void stop() throws IOException {
      try {
        server.stop();
      } finally {
        FileUtils.deleteDirectory(backendDir.toFile());
      }
    }

    public String uri() {
      return (tlsEnabled ? "https" : "http") + "://localhost:" + port;
    }
  }

  public static TestGravitinoServer startGravitinoServer(boolean clientAuth, boolean tlsEnabled)
      throws Exception {
    // create a temporary directory for the backend database to delete after the test is done
    Path backendDir = Files.createTempDirectory("gravitino-client-tls-");
    Path backendPath = backendDir.resolve("gravitino.db");

    int port = RESTUtils.findAvailablePort(6000, 7000);

    Map<String, String> configs = new HashMap<>();

    configs.put(ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(), backendPath.toString());
    if (tlsEnabled) {

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.ENABLE_HTTPS.getKey(), "true");

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
          String.valueOf(port));

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.SSL_KEYSTORE_PATH.getKey(),
          TestTlsServerUtils.testResource("test-server-keystore.p12").toString());

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.SSL_KEYSTORE_PASSWORD.getKey(),
          TEST_STORE_PASSWORD);

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.SSL_MANAGER_PASSWORD.getKey(),
          TEST_STORE_PASSWORD);

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.ENABLE_CLIENT_AUTH.getKey(),
          String.valueOf(clientAuth));

      if (clientAuth) {
        configs.put(
            GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.SSL_TRUST_STORE_PATH.getKey(),
            TestTlsServerUtils.testResource("test-server-truststore.p12").toString());

        configs.put(
            GravitinoServer.WEBSERVER_CONF_PREFIX
                + JettyServerConfig.SSL_TRUST_STORE_PASSWORD.getKey(),
            TEST_STORE_PASSWORD);
      }
    } else {
      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.ENABLE_HTTPS.getKey(), "false");

      configs.put(
          GravitinoServer.WEBSERVER_CONF_PREFIX + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
          String.valueOf(port));
    }

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(configs, t -> true);

    ServerConfig spyServerConfig = Mockito.spy(serverConfig);

    Mockito.when(
            spyServerConfig.getConfigsWithPrefix(
                AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));

    GravitinoServer server = new GravitinoServer(spyServerConfig, GravitinoEnv.getInstance());

    server.initialize();
    server.start();

    return new TestGravitinoServer(server, port, backendDir, tlsEnabled);
  }
}
