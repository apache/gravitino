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

import static org.apache.gravitino.server.web.TestTlsServerUtils.TEST_STORE_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHttpsServerAuthentication {

  private static final String TEST_STORE_TYPE = "PKCS12";
  private static final char[] TEST_STORE_PASSWORD_CHARS = TEST_STORE_PASSWORD.toCharArray();

  private static final String TEST_PATH = "/tls-test";
  private static final String TEST_RESPONSE = "success";
  private static final String SERVLET_RESPONSE = "success";

  private JettyServer jettyServer;

  @BeforeEach
  public void setUp() {
    jettyServer = new JettyServer();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

  @Test
  public void testClientAuthRejectsMissingTrustStore() throws Exception {
    Config config = new Config(false) {};

    config.set(JettyServerConfig.ENABLE_HTTPS, true);
    config.set(JettyServerConfig.WEBSERVER_HTTPS_PORT, RESTUtils.findAvailablePort(6000, 7000));

    config.set(
        JettyServerConfig.SSL_KEYSTORE_PATH,
        TestTlsServerUtils.testResource("test-server-keystore.p12").toString());
    config.set(JettyServerConfig.SSL_KEYSTORE_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.SSL_MANAGER_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.ENABLE_CLIENT_AUTH, true);

    assertThrows(IllegalArgumentException.class, () -> JettyServerConfig.fromConfig(config));
  }

  @Test
  public void testMutualTlsAcceptsTrustedClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(jettyServer, true, createTestServlet(), TEST_PATH);

    HttpClient client =
        createHttpsClient(
            TestTlsServerUtils.testResource("test-client-truststore.p12"),
            TestTlsServerUtils.testResource("test-trusted-client-keystore.p12"));

    HttpRequest request = createHttpsRequest(port);

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpServletResponse.SC_OK, response.statusCode());
    assertEquals(TEST_RESPONSE, response.body());
  }

  @Test
  public void testMutualTlsRejectsMissingClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(jettyServer, true, createTestServlet(), TEST_PATH);

    HttpClient client =
        createHttpsClient(TestTlsServerUtils.testResource("test-client-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    TestTlsServerUtils.assertHandshakeFailure(exception);
  }

  @Test
  public void testMutualTlsRejectsUntrustedClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(jettyServer, true, createTestServlet(), TEST_PATH);

    HttpClient client =
        createHttpsClient(
            TestTlsServerUtils.testResource("test-client-truststore.p12"),
            TestTlsServerUtils.testResource("test-untrusted-client-keystore.p12"));

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    TestTlsServerUtils.assertHandshakeFailure(exception);
  }

  @Test
  public void testClientRejectsUntrustedServerCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(jettyServer, false, createTestServlet(), TEST_PATH);

    HttpClient client =
        createHttpsClient(
            TestTlsServerUtils.testResource("test-untrusted-server-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    Exception exception =
        assertThrows(
            Exception.class, () -> client.send(request, HttpResponse.BodyHandlers.ofString()));

    TestTlsServerUtils.assertHandshakeFailure(exception);
  }

  @Test
  public void testHttpsWithoutClientAuthentication() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(jettyServer, false, createTestServlet(), TEST_PATH);

    HttpClient client =
        createHttpsClient(TestTlsServerUtils.testResource("test-client-truststore.p12"), null);

    HttpRequest request = createHttpsRequest(port);

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpServletResponse.SC_OK, response.statusCode());
    assertEquals(TEST_RESPONSE, response.body());
  }

  @Test
  public void testHttpWithoutCustomTlsConfiguration() throws Exception {
    int port = TestTlsServerUtils.startHttpServer(jettyServer, createTestServlet(), TEST_PATH);

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

  private static HttpServlet createTestServlet() {
    return new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        response.getWriter().write(SERVLET_RESPONSE);
      }
    };
  }
}
