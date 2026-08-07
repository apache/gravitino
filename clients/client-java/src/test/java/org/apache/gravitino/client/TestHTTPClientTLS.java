/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import javax.net.ssl.SSLHandshakeException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHTTPClientTLS {

  private static final String TEST_STORE_PASSWORD = "changeit";

  private static final String TEST_SERVLET_PATH = "/tls-test";
  private static final String TEST_CLIENT_PATH = "tls-test";

  private JettyServer jettyServer;

  @BeforeEach
  public void setUp() {
    jettyServer = new JettyServer();
  }

  @AfterEach
  public void tearDown() {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

  @Test
  public void testTrustedClientCertificateSucceedsWhenClientAuthRequired() throws Exception {
    int port = startHttpsServer(true);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .keyStore(testResource("test-trusted-client-keystore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpClient(port, tlsConfigurer)) {
      assertDoesNotThrow(() -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));
    }
  }

  @Test
  public void testMissingClientCertificateFailsWhenClientAuthRequired() throws Exception {
    int port = startHttpsServer(true);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testUntrustedClientCertificateFailsWhenClientAuthRequired() throws Exception {
    int port = startHttpsServer(true);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .keyStore(testResource("test-untrusted-client-keystore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testHttpsSucceedsWithoutClientAuthentication() throws Exception {
    int port = startHttpsServer(false);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpClient(port, tlsConfigurer)) {
      assertDoesNotThrow(() -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));
    }
  }

  @Test
  public void testUntrustedServerCertificateFails() throws Exception {
    int port = startHttpsServer(false);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(testResource("test-untrusted-server-truststore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      assertHandshakeFailure(exception);
    }
  }

  private int startHttpsServer(boolean requireClientAuthentication) throws Exception {
    int port = RESTUtils.findAvailablePort(6000, 7000);

    Config config = new Config(false) {};
    config.set(JettyServerConfig.ENABLE_HTTPS, true);
    config.set(JettyServerConfig.WEBSERVER_HTTPS_PORT, port);
    config.set(
        JettyServerConfig.SSL_KEYSTORE_PATH, testResource("test-server-keystore.p12").toString());
    config.set(JettyServerConfig.SSL_KEYSTORE_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.SSL_MANAGER_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.ENABLE_CLIENT_AUTH, requireClientAuthentication);

    if (requireClientAuthentication) {
      config.set(
          JettyServerConfig.SSL_TRUST_STORE_PATH,
          testResource("test-server-truststore.p12").toString());
      config.set(JettyServerConfig.SSL_TRUST_STORE_PASSWORD, TEST_STORE_PASSWORD);
    }

    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);

    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();
    jettyServer.addServlet(createTestServlet(), TEST_SERVLET_PATH);

    return port;
  }

  private static HTTPClient createHttpClient(int port, TLSConfigurer tlsConfigurer) {
    return HTTPClient.builder(ImmutableMap.of())
        .uri("https://localhost:" + port)
        .withTlsConfigurer(tlsConfigurer)
        .build();
  }

  private static Path testResource(String filename) throws Exception {
    return Path.of(
        Objects.requireNonNull(
                TestHTTPClientTLS.class.getResource("/tls/" + filename),
                "Missing TLS test resource: " + filename)
            .toURI());
  }

  private static HttpServlet createTestServlet() {
    return new HttpServlet() {
      @Override
      protected void doHead(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
      }
    };
  }

  private static void assertHandshakeFailure(Throwable throwable) {
    Throwable current = throwable;

    while (current != null) {
      if (current instanceof SSLHandshakeException) {
        return;
      }

      current = current.getCause();
    }

    fail("Expected an SSL handshake failure, but received: " + throwable, throwable);
  }
}
