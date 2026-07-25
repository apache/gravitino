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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Objects;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.Filter;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJettyServer {

  private static final String TEST_STORE_TYPE = "PKCS12";
  private static final String TEST_STORE_PASSWORD = "changeit";
  private static final char[] TEST_STORE_PASSWORD_CHARS = TEST_STORE_PASSWORD.toCharArray();

  private static final String TEST_PATH = "/tls-test";
  private static final String TEST_RESPONSE = "success";

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
  public void testInitialize() throws IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);

    // TODO might be nice to have an isInitialised method or similar?
  }

  @Test
  public void testStartAndStop() throws RuntimeException, IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();
    // TODO might be nice to have an IsRunning method or similar?
    jettyServer.stop();
  }

  @Test
  public void testAddServletAndFilter() throws RuntimeException, IOException {
    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, RESTUtils.findAvailablePort(5000, 6000));
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);
    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();

    Servlet mockServlet = mock(Servlet.class);
    Filter mockFilter = mock(Filter.class);
    jettyServer.addServlet(mockServlet, "/test");
    jettyServer.addFilter(mockFilter, "/filter");

    // TODO add asserts

    jettyServer.stop();
  }

  @Test
  public void testStopWithNullServer() {
    assertDoesNotThrow(() -> jettyServer.stop());
  }

  @Test
  public void testStartWithoutInitialise() throws InterruptedException {
    assertThrows(RuntimeException.class, () -> jettyServer.start());
  }

  @Test
  public void testClientAuthRequiresTrustStore() throws Exception {
    Config config = new Config(false) {};

    config.set(JettyServerConfig.ENABLE_HTTPS, true);
    config.set(JettyServerConfig.WEBSERVER_HTTPS_PORT, RESTUtils.findAvailablePort(6000, 7000));

    config.set(
        JettyServerConfig.SSL_KEYSTORE_PATH, testResource("test-server-keystore.p12").toString());
    config.set(JettyServerConfig.SSL_KEYSTORE_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.SSL_MANAGER_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.ENABLE_CLIENT_AUTH, true);

    assertThrows(IllegalArgumentException.class, () -> JettyServerConfig.fromConfig(config));
  }

  @Test
  public void testMutualTlsAcceptsTrustedClientCertificate() throws Exception {
    int port = startHttpsServer(true);

    HttpClient client =
        createHttpsClient(
            testResource("test-client-truststore.p12"),
            testResource("test-trusted-client-keystore.p12"));

    HttpRequest request = createHttpsRequest(port);

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpServletResponse.SC_OK, response.statusCode());
    assertEquals(TEST_RESPONSE, response.body());
  }

  @Test
  public void testMutualTlsRejectsMissingClientCertificate() throws Exception {
    int port = startHttpsServer(true);

    // The client trusts the server but does not provide a certificate.
    HttpClient client = createHttpsClient(testResource("test-client-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    assertHandshakeFailure(exception);
  }

  @Test
  public void testMutualTlsRejectsUntrustedClientCertificate() throws Exception {
    int port = startHttpsServer(true);

    // The client trusts the server but presents a certificate that the
    // server does not trust.
    HttpClient client =
        createHttpsClient(
            testResource("test-client-truststore.p12"),
            testResource("test-untrusted-client-keystore.p12"));

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    assertHandshakeFailure(exception);
  }

  @Test
  public void testClientRejectsUntrustedServerCertificate() throws Exception {
    int port = startHttpsServer(false);

    HttpClient client =
        createHttpsClient(testResource("test-untrusted-server-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    assertHandshakeFailure(exception);
  }

  @Test
  public void testHttpsWithoutClientAuthentication() throws Exception {
    int port = startHttpsServer(false);

    HttpClient client = createHttpsClient(testResource("test-client-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpServletResponse.SC_OK, response.statusCode());
    assertEquals(TEST_RESPONSE, response.body());
  }

  @Test
  public void testHttpWithoutCustomTlsConfiguration() throws Exception {
    int port = RESTUtils.findAvailablePort(5000, 6000);

    Config config = new Config(false) {};
    config.set(JettyServerConfig.WEBSERVER_HTTP_PORT, port);

    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(config);

    jettyServer.initialize(serverConfig, "test", false);
    jettyServer.start();
    jettyServer.addServlet(createTestServlet(), TEST_PATH);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + TEST_PATH))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpServletResponse.SC_OK, response.statusCode());
    assertEquals(TEST_RESPONSE, response.body());
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
    jettyServer.addServlet(createTestServlet(), TEST_PATH);

    return port;
  }

  private static HttpClient createHttpsClient(Path trustStorePath, Path clientKeyStorePath)
      throws Exception {
    KeyStore trustStore = loadStore(trustStorePath);

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");

    if (clientKeyStorePath == null) {
      sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
    } else {
      KeyStore clientKeyStore = loadStore(clientKeyStorePath);

      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(clientKeyStore, TEST_STORE_PASSWORD_CHARS);

      sslContext.init(
          keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
    }

    return HttpClient.newBuilder()
        .sslContext(sslContext)
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }

  private static HttpRequest createHttpsRequest(int port) {
    return HttpRequest.newBuilder()
        .uri(URI.create("https://localhost:" + port + TEST_PATH))
        .timeout(Duration.ofSeconds(10))
        .GET()
        .build();
  }

  private static KeyStore loadStore(Path path) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(TEST_STORE_TYPE);

    try (InputStream inputStream = Files.newInputStream(path)) {
      keyStore.load(inputStream, TEST_STORE_PASSWORD_CHARS);
    }

    return keyStore;
  }

  private static Path testResource(String filename) throws Exception {
    return Path.of(
        Objects.requireNonNull(
                TestJettyServer.class.getResource("/tls/" + filename),
                "Missing TLS test resource: " + filename)
            .toURI());
  }

  private static HttpServlet createTestServlet() {
    return new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        response.getWriter().write(TEST_RESPONSE);
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
