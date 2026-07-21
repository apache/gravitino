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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientContract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestVaultTransitKmsClient extends TestKmsClientContract {

  private static final String SOURCE = "primary";
  private static final String USABLE_KEY = "customer-key";
  private static final String MISSING_KEY = "missing-key";

  @TempDir private Path tempDir;

  private final AtomicReference<String> requestedPath = new AtomicReference<>();
  private final AtomicReference<String> requestedToken = new AtomicReference<>();

  private HttpServer server;
  private VaultTransitKmsClient client;

  @BeforeEach
  void startServer() throws IOException {
    Path tokenFile = tempDir.resolve("vault-token");
    Files.write(tokenFile, "read-only-token".getBytes(StandardCharsets.UTF_8));

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::respond);
    server.start();
    client =
        new VaultTransitKmsClient(
            SOURCE,
            URI.create(String.format("http://127.0.0.1:%s", server.getAddress().getPort())),
            "transit",
            tokenFile);
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
  void readsVaultTransitKeyMetadata() {
    client.getKeyProperties(usableKey());

    assertEquals("/v1/transit/keys/customer-key", requestedPath.get());
    assertEquals("read-only-token", requestedToken.get());
  }

  private KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.VAULT_TRANSIT, SOURCE, keyId);
  }

  private void respond(HttpExchange exchange) throws IOException {
    requestedPath.set(exchange.getRequestURI().getRawPath());
    requestedToken.set(exchange.getRequestHeaders().getFirst("X-Vault-Token"));

    boolean missing = exchange.getRequestURI().getRawPath().endsWith("/" + MISSING_KEY);
    byte[] response =
        ("{\"data\":{\"type\":\"aes256-gcm96\",\"supports_encryption\":true,"
                + "\"supports_decryption\":true}}")
            .getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(missing ? 404 : 200, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }
}
