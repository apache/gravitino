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
package org.apache.gravitino.kms.transit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsAuthenticationException;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientContract;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestOpenBaoTransitKmsClient extends TestKmsClientContract {

  private static final String SOURCE = "primary";
  private static final String USABLE_KEY = "customer key";
  private static final String MISSING_KEY = "missing-key";
  private static final String USABLE_RESPONSE =
      "{\"data\":{\"type\":\"aes256-gcm96\",\"supports_encryption\":true,"
          + "\"supports_decryption\":true,\"soft_deleted\":false}}";

  @TempDir private Path tempDir;

  private final AtomicInteger responseStatus = new AtomicInteger(200);
  private final AtomicReference<String> responseBody = new AtomicReference<>(USABLE_RESPONSE);
  private final AtomicReference<String> acceptedToken = new AtomicReference<>();
  private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

  private HttpServer server;
  private Path tokenFile;
  private OpenBaoTransitKmsClient client;

  @BeforeEach
  void startServer() throws IOException {
    tokenFile = tempDir.resolve("openbao-read-only-token");
    Files.write(tokenFile, "read-only-token\n".getBytes(StandardCharsets.UTF_8));

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::respond);
    server.start();
    client = createClient("custom/transit", tokenFile);
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Override
  protected KmsClient client() {
    return client;
  }

  @Override
  protected KmsReference usableKey() {
    return reference(USABLE_KEY);
  }

  @Override
  protected KmsReference missingKey() {
    return reference(MISSING_KEY);
  }

  @Test
  void readsKeyPropertiesUsingMetadataOnlyGet() {
    client.getKeyProperties(usableKey());

    assertEquals(1, requests.size());
    RecordedRequest request = requests.get(0);
    assertEquals("GET", request.method);
    assertEquals("/v1/custom/transit/keys/customer%20key", request.rawPath);
    assertEquals("read-only-token", request.token);
  }

  @Test
  void reportsSoftDeletedKeyAsMissing() {
    responseBody.set(
        "{\"data\":{\"supports_encryption\":true,\"supports_decryption\":true,"
            + "\"soft_deleted\":true}}");

    KmsKeyProperties properties = client.getKeyProperties(usableKey());

    assertFalse(properties.present());
    assertFalse(properties.enabled());
    assertFalse(properties.supportsWrapping());
    assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void treatsMissingSoftDeletedFieldAsEnabledForOlderServers() {
    responseBody.set("{\"data\":{\"supports_encryption\":true,\"supports_decryption\":true}}");

    assertTrue(client.getKeyProperties(usableKey()).enabled());
  }

  @Test
  void mapsTransitOperationSupport() {
    responseBody.set(
        "{\"data\":{\"supports_encryption\":false,\"supports_decryption\":true,"
            + "\"soft_deleted\":false}}");

    KmsKeyProperties properties = client.getKeyProperties(usableKey());

    assertFalse(properties.supportsWrapping());
    assertTrue(properties.supportsUnwrapping());
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 405, 429, 500, 502, 503})
  void mapsProviderErrorsToConnectionFailures(int statusCode) {
    responseStatus.set(statusCode);

    assertThrows(ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403})
  void mapsRejectedCredentialsToAuthenticationFailures(int statusCode) {
    responseStatus.set(statusCode);

    assertThrows(KmsAuthenticationException.class, () -> client.getKeyProperties(usableKey()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not-json",
        "{}",
        "{\"data\":{}}",
        "{\"data\":{\"supports_encryption\":\"true\",\"supports_decryption\":true}}",
        "{\"data\":{\"supports_encryption\":true,\"supports_decryption\":\"true\"}}",
        "{\"data\":{\"supports_encryption\":true,\"supports_decryption\":true,"
            + "\"soft_deleted\":\"false\"}}"
      })
  void mapsMalformedResponsesToConnectionFailures(String body) {
    responseBody.set(body);

    assertThrows(ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  @Test
  void cachesTokenAcrossSuccessfulRequests() throws IOException {
    client.getKeyProperties(usableKey());
    Files.write(tokenFile, "rotated-token".getBytes(StandardCharsets.UTF_8));
    client.getKeyProperties(usableKey());

    assertEquals("read-only-token", requests.get(0).token);
    assertEquals("read-only-token", requests.get(1).token);
  }

  @Test
  void reloadsTokenAndRetriesOnceAfterAuthenticationFailure() throws IOException {
    client.getKeyProperties(usableKey());
    Files.write(tokenFile, "rotated-token".getBytes(StandardCharsets.UTF_8));
    acceptedToken.set("rotated-token");

    assertTrue(client.getKeyProperties(usableKey()).present());

    assertEquals(3, requests.size());
    assertEquals("read-only-token", requests.get(1).token);
    assertEquals("rotated-token", requests.get(2).token);
  }

  @Test
  void loadsTokenLazilyOnFirstRequest() throws IOException {
    Files.write(tokenFile, "before-first-request".getBytes(StandardCharsets.UTF_8));

    client.getKeyProperties(usableKey());

    assertEquals("before-first-request", requests.get(0).token);
  }

  @Test
  void closesPooledHttpClient() {
    client.close();

    assertThrows(ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  @Test
  void rejectsInvalidKeyIdWithoutCallingOpenBao() {
    assertThrows(
        IllegalArgumentException.class, () -> client.getKeyProperties(reference("nested/key")));
    assertTrue(requests.isEmpty());
  }

  @Test
  void mapsUnreadableTokenAndUnavailableServiceToConnectionFailures() {
    OpenBaoTransitKmsClient missingTokenClient =
        createClient("transit", tempDir.resolve("missing-token"));
    assertThrows(
        ConnectionFailedException.class, () -> missingTokenClient.getKeyProperties(usableKey()));

    server.stop(0);
    server = null;
    assertThrows(ConnectionFailedException.class, () -> client.getKeyProperties(usableKey()));
  }

  private OpenBaoTransitKmsClient createClient(String transitMount, Path clientTokenFile) {
    return new OpenBaoTransitKmsClient(
        SOURCE,
        URI.create(String.format("http://127.0.0.1:%s", server.getAddress().getPort())),
        transitMount,
        clientTokenFile);
  }

  private KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.OPENBAO_TRANSIT, SOURCE, keyId);
  }

  private void respond(HttpExchange exchange) throws IOException {
    requests.add(
        new RecordedRequest(
            exchange.getRequestMethod(),
            exchange.getRequestURI().getRawPath(),
            exchange.getRequestHeaders().getFirst("X-Vault-Token")));

    String requestToken = exchange.getRequestHeaders().getFirst("X-Vault-Token");
    int status;
    if (exchange.getRequestURI().getRawPath().endsWith("/" + MISSING_KEY)) {
      status = 404;
    } else if (acceptedToken.get() != null && !acceptedToken.get().equals(requestToken)) {
      status = 403;
    } else {
      status = responseStatus.get();
    }
    byte[] response = responseBody.get().getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }

  private static final class RecordedRequest {
    private final String method;
    private final String rawPath;
    private final String token;

    private RecordedRequest(String method, String rawPath, String token) {
      this.method = method;
      this.rawPath = rawPath;
      this.token = token;
    }
  }
}
