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

import static org.apache.gravitino.server.web.TestTlsServerUtils.TEST_STORE_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.TestTlsServerUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHTTPClientTLS {
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
  public void testMutualTlsAcceptsTrustedClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, true, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .keyStore(
                TestTlsServerUtils.testResource("test-trusted-client-keystore.p12"),
                TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testMutualTlsRejectsMissingClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, true, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testMutualTlsRejectsUntrustedClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, true, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .keyStore(
                TestTlsServerUtils.testResource("test-untrusted-client-keystore.p12"),
                TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testHttpsWithoutClientAuthentication() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource("test-client-truststore.p12"), TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testClientRejectsUntrustedServerCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource("test-untrusted-server-truststore.p12"),
                TEST_STORE_PASSWORD)
            .build();

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testHttpWithoutCustomTlsConfiguration() throws Exception {
    int port =
        TestTlsServerUtils.startHttpServer(jettyServer, createTestServlet(), TEST_SERVLET_PATH);

    try (HTTPClient client =
        HTTPClient.builder(ImmutableMap.of()).uri("http://localhost:" + port).build()) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testInvalidTrustStorePathFailsAtBuild() {
    Path missingPath = Path.of("does-not-exist.p12");

    assertThrows(
        IllegalArgumentException.class,
        () -> TLSConfigurers.builder().trustStore(missingPath, TEST_STORE_PASSWORD).build());
  }

  @Test
  public void testWrongTrustStorePasswordFailsAtBuild() throws Exception {
    Path trustStore = TestTlsServerUtils.testResource("test-client-truststore.p12");

    assertThrows(
        IllegalArgumentException.class,
        () -> TLSConfigurers.builder().trustStore(trustStore, "wrong-password").build());
  }

  private static HTTPClient createHttpsClient(int port, TLSConfigurer tlsConfigurer) {
    return HTTPClient.builder(ImmutableMap.of())
        .uri("https://localhost:" + port)
        .withTlsConfigurer(tlsConfigurer)
        .build();
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
}
