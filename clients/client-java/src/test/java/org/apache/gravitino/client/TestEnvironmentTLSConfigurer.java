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

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.rest.RESTResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.logging.MockServerLogger;
import org.mockserver.socket.tls.KeyStoreFactory;

class TestEnvironmentTLSConfigurer {

  @Test
  void testEmptyEnvironmentUsesJvmDefaults() {
    Assertions.assertFalse(EnvironmentTLSConfigurer.fromEnvironment(ImmutableMap.of()).isPresent());
    Assertions.assertNull(HTTPClient.resolveTLSConfigurer(null, ImmutableMap.of()));
  }

  @Test
  void testStorePathAndPasswordMustBeConfiguredTogether() {
    IllegalArgumentException keyStoreException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                EnvironmentTLSConfigurer.fromEnvironment(
                    ImmutableMap.of(EnvironmentTLSConfigurer.KEY_STORE_PASSWORD, "secret")));
    Assertions.assertTrue(
        keyStoreException.getMessage().contains(EnvironmentTLSConfigurer.KEY_STORE_PATH));

    IllegalArgumentException trustStoreException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                EnvironmentTLSConfigurer.fromEnvironment(
                    ImmutableMap.of(EnvironmentTLSConfigurer.TRUST_STORE_PATH, "/missing")));
    Assertions.assertTrue(
        trustStoreException.getMessage().contains(EnvironmentTLSConfigurer.TRUST_STORE_PASSWORD));
  }

  @Test
  void testInvalidStoreDoesNotExposePassword() {
    String password = "do-not-log-this-password";
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                EnvironmentTLSConfigurer.fromEnvironment(
                    ImmutableMap.of(
                        EnvironmentTLSConfigurer.KEY_STORE_PATH,
                        "/missing/client.p12",
                        EnvironmentTLSConfigurer.KEY_STORE_PASSWORD,
                        password)));

    Assertions.assertTrue(exception.getMessage().contains("/missing/client.p12"));
    Assertions.assertFalse(exception.getMessage().contains(password));
  }

  @Test
  void testExplicitConfigurerTakesPrecedenceOverEnvironment() {
    TLSConfigurer explicitConfigurer = new TLSConfigurer() {};
    Map<String, String> invalidEnvironment =
        ImmutableMap.of(EnvironmentTLSConfigurer.KEY_STORE_PASSWORD, "secret");

    Assertions.assertSame(
        explicitConfigurer,
        HTTPClient.resolveTLSConfigurer(explicitConfigurer, invalidEnvironment));
  }

  @Test
  void testMutualTlsHandshakeRequiresClientCertificate() throws IOException {
    Configuration configuration =
        Configuration.configuration()
            .tlsMutualAuthenticationRequired(true)
            .livenessHttpGetPath("/mtls");
    KeyStoreFactory keyStoreFactory =
        new KeyStoreFactory(configuration, new MockServerLogger(getClass()));
    Path keyStorePath = Paths.get(keyStoreFactory.keyStoreFileName).toAbsolutePath();
    String previousTrustStore = System.getProperty("javax.net.ssl.trustStore");
    ClientAndServer server = null;

    try {
      keyStoreFactory.loadOrCreateKeyStore();
      restoreSystemProperty("javax.net.ssl.trustStore", previousTrustStore);

      server = startClientAndServer(configuration);

      Map<String, String> trustOnlyEnvironment =
          ImmutableMap.of(
              EnvironmentTLSConfigurer.TRUST_STORE_PATH,
              keyStorePath.toString(),
              EnvironmentTLSConfigurer.TRUST_STORE_PASSWORD,
              KeyStoreFactory.KEY_STORE_PASSWORD,
              EnvironmentTLSConfigurer.TRUST_STORE_TYPE,
              KeyStoreFactory.KEY_STORE_TYPE);
      TLSConfigurer trustOnlyConfigurer =
          HTTPClient.resolveTLSConfigurer(null, trustOnlyEnvironment);
      Assertions.assertNotNull(trustOnlyConfigurer);

      try (HTTPClient client = newClient(server, trustOnlyConfigurer)) {
        RESTException exception =
            Assertions.assertThrows(
                RESTException.class,
                () -> client.get("mtls", StatusResponse.class, ImmutableMap.of(), error -> {}));
        Assertions.assertTrue(
            exception.getMessage().contains("Failed to execute request"), exception::toString);
      }

      Map<String, String> mutualTlsEnvironment =
          ImmutableMap.<String, String>builder()
              .putAll(trustOnlyEnvironment)
              .put(EnvironmentTLSConfigurer.KEY_STORE_PATH, keyStorePath.toString())
              .put(EnvironmentTLSConfigurer.KEY_STORE_PASSWORD, KeyStoreFactory.KEY_STORE_PASSWORD)
              .put(EnvironmentTLSConfigurer.KEY_STORE_TYPE, KeyStoreFactory.KEY_STORE_TYPE)
              .build();
      TLSConfigurer mutualTlsConfigurer =
          HTTPClient.resolveTLSConfigurer(null, mutualTlsEnvironment);
      Assertions.assertNotNull(mutualTlsConfigurer);

      try (HTTPClient client = newClient(server, mutualTlsConfigurer)) {
        client.get("mtls", StatusResponse.class, ImmutableMap.of(), error -> {});
      }
    } finally {
      if (server != null) {
        server.stop();
      }
      restoreSystemProperty("javax.net.ssl.trustStore", previousTrustStore);
      Files.deleteIfExists(keyStorePath);
    }
  }

  private static HTTPClient newClient(ClientAndServer server, TLSConfigurer tlsConfigurer) {
    return HTTPClient.builder(ImmutableMap.of())
        .uri(String.format("https://localhost:%d", server.getPort()))
        .withTlsConfigurer(tlsConfigurer)
        .build();
  }

  private static void restoreSystemProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class StatusResponse implements RESTResponse {
    @Override
    public void validate() throws IllegalArgumentException {}
  }
}
