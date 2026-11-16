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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
  private static final String TEST_CLIENT_TRUSTSTORE = "test-client-truststore.p12";
  private static final String TEST_TRUSTED_CLIENT_KEYSTORE = "test-trusted-client-keystore.p12";
  private static final String TEST_UNTRUSTED_CLIENT_KEYSTORE = "test-untrusted-client-keystore.p12";

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
        TLSConfigurers.withMutualTls(
            TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE),
            TEST_STORE_PASSWORD,
            TestTlsServerUtils.testResource(TEST_TRUSTED_CLIENT_KEYSTORE),
            TEST_STORE_PASSWORD);

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testMutualTlsRejectsMissingClientCertificate() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, true, createTestServlet(), TEST_SERVLET_PATH);

    // Creates a TLSConfigurer that only has the trust store, but no client key store, to simulate a
    // client that does not present a certificate.
    TLSConfigurer tlsConfigurer =
        TLSConfigurers.withTrustStore(
            TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD);

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
        TLSConfigurers.withMutualTls(
            TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE),
            TEST_STORE_PASSWORD,
            TestTlsServerUtils.testResource(TEST_UNTRUSTED_CLIENT_KEYSTORE),
            TEST_STORE_PASSWORD);

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
        TLSConfigurers.withTrustStore(
            TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD);

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
        TLSConfigurers.withTrustStore(
            TestTlsServerUtils.testResource("test-untrusted-server-truststore.p12"),
            TEST_STORE_PASSWORD);

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  /*
   * The current test server uses a self-signed certificate that is not trusted by the JVM's
   * default truststore. Therefore, a successful keystore-only mTLS handshake cannot be
   * demonstrated with the existing test fixtures.
   *
   * This test verifies that withKeyStore() preserves default JVM certificate verification
   * and rejects the untrusted server certificate.
   *
   * A future test fixture using a server certificate trusted by the test JVM should allow
   * a successful handshake to be tested without configuring a custom client truststore.
   */

  /*
   * TODO: Add a successful keystore-only mTLS test when a test server certificate
   * trusted by the default JVM truststore is available.
   *
   * The future test should:
   * 1. Start an HTTPS server requiring client authentication with a certificate
   *    trusted by the test JVM.
   * 2. Configure the client using only withKeyStore().
   * 3. Verify that the HTTPS request succeeds.
   *
   * Example client configuration:
   *
   * TLSConfigurer tlsConfigurer =
   *     TLSConfigurers.withKeyStore(
   *         TestTlsServerUtils.testResource(TEST_TRUSTED_CLIENT_KEYSTORE),
   *         TEST_STORE_PASSWORD);
   *
   * try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
   *   client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
   * }
   */

  @Test
  public void testMutualTLSWithDefaultTrustStoreRejectsUntrustedServer() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, true, createTestServlet(), TEST_SERVLET_PATH);

    TLSConfigurer tlsConfigurer =
        TLSConfigurers.withKeyStore(
            TestTlsServerUtils.testResource(TEST_TRUSTED_CLIENT_KEYSTORE), TEST_STORE_PASSWORD);

    try (HTTPClient client = createHttpsClient(port, tlsConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testHttpsWithDefaultTlsRejectsUntrustedServer() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH);

    try (HTTPClient client =
        HTTPClient.builder(ImmutableMap.of()).uri("https://localhost:" + port).build()) {

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

  @Test
  public void testTlsProtocolRestrictionAllowsMatchingProtocol() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH, "TLSv1.2", null);

    TLSConfigurer allowedConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD)
            .protocols("TLSv1.2")
            .build();

    try (HTTPClient client = createHttpsClient(port, allowedConfigurer)) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testTlsProtocolRestrictionRejectsMismatchedProtocol() throws Exception {
    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH, "TLSv1.2", null);

    TLSConfigurer restrictedConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD)
            .protocols("TLSv1.3")
            .build();

    try (HTTPClient client = createHttpsClient(port, restrictedConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testTlsCipherSuiteRestrictionAllowsMatchingCipher() throws Exception {
    String serverCipher = "TLS_AES_128_GCM_SHA256";

    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH, "TLSv1.3", serverCipher);

    TLSConfigurer allowedConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD)
            .protocols("TLSv1.3")
            .cipherSuites(serverCipher)
            .build();

    try (HTTPClient client = createHttpsClient(port, allowedConfigurer)) {
      client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {});
    }
  }

  @Test
  public void testTlsCipherSuiteRestrictionRejectsMismatchedCipher() throws Exception {
    String serverCipher = "TLS_AES_128_GCM_SHA256";
    String clientCipher = "TLS_AES_256_GCM_SHA384";

    int port =
        TestTlsServerUtils.startHttpsServer(
            jettyServer, false, createTestServlet(), TEST_SERVLET_PATH, "TLSv1.3", serverCipher);

    TLSConfigurer restrictedConfigurer =
        TLSConfigurers.builder()
            .trustStore(
                TestTlsServerUtils.testResource(TEST_CLIENT_TRUSTSTORE), TEST_STORE_PASSWORD)
            .protocols("TLSv1.3")
            .cipherSuites(clientCipher)
            .build();

    try (HTTPClient client = createHttpsClient(port, restrictedConfigurer)) {
      RESTException exception =
          assertThrows(
              RESTException.class,
              () -> client.head(TEST_CLIENT_PATH, ImmutableMap.of(), response -> {}));

      TestTlsServerUtils.assertHandshakeFailure(exception);
    }
  }

  @Test
  public void testUnsupportedTlsProtocolFailsAtBuild() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> TLSConfigurers.builder().protocols("TLSv9.9").build());

    assertEquals("Unsupported TLS protocol: TLSv9.9", exception.getMessage());
  }

  @Test
  public void testUnsupportedTlsCipherSuiteFailsAtBuild() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> TLSConfigurers.builder().cipherSuites("INVALID_CIPHER").build());

    assertEquals("Unsupported TLS cipher suite: INVALID_CIPHER", exception.getMessage());
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
