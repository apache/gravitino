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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientFactoryContract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOpenBaoTransitKmsClientFactory extends TestKmsClientFactoryContract {

  private static final String SOURCE = "primary";

  @TempDir private Path tempDir;

  private final AtomicReference<String> requestedPath = new AtomicReference<>();

  private HttpServer server;
  private Path tokenFile;

  @BeforeEach
  void startServer() throws IOException {
    tokenFile = tempDir.resolve("openbao-token");
    Files.write(tokenFile, "read-only-token".getBytes(StandardCharsets.UTF_8));

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::respond);
    server.start();
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Override
  protected KmsClientFactory factory() {
    return new OpenBaoTransitKmsClientFactory();
  }

  @Override
  protected KmsApi expectedApi() {
    return KmsApi.OPENBAO_TRANSIT;
  }

  @Test
  void createsWorkingClientWithDefaultMount() {
    KmsClient client = factory().create(SOURCE, properties());

    client.getKeyProperties(new KmsReference(KmsApi.OPENBAO_TRANSIT, SOURCE, "customer-key"));

    assertEquals("/v1/transit/keys/customer-key", requestedPath.get());
  }

  @Test
  void createsWorkingClientWithCustomMount() {
    Map<String, String> properties = properties();
    properties.put(OpenBaoTransitKmsClientFactory.TRANSIT_MOUNT, "team/transit");
    KmsClient client = factory().create(SOURCE, properties);

    client.getKeyProperties(new KmsReference(KmsApi.OPENBAO_TRANSIT, SOURCE, "customer-key"));

    assertEquals("/v1/team/transit/keys/customer-key", requestedPath.get());
  }

  @Test
  void rejectsMissingRequiredConfiguration() {
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, null));

    Map<String, String> missingAddress = properties();
    missingAddress.remove(OpenBaoTransitKmsClientFactory.SERVICE_ADDRESS);
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, missingAddress));

    Map<String, String> missingToken = properties();
    missingToken.remove(OpenBaoTransitKmsClientFactory.TOKEN_FILE);
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, missingToken));
  }

  @Test
  void rejectsInvalidSourceAndUnknownConfiguration() {
    assertThrows(IllegalArgumentException.class, () -> factory().create(" ", properties()));

    Map<String, String> properties = properties();
    properties.put("token", "secret");
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, properties));
  }

  @Test
  void rejectsInvalidServiceAddress() {
    assertInvalidServiceAddress("file:///tmp/openbao");
    assertInvalidServiceAddress("http://user@localhost");
    assertInvalidServiceAddress("http://localhost/openbao");
    assertInvalidServiceAddress("http://localhost?namespace=team");
    assertInvalidServiceAddress("http://localhost#fragment");
  }

  @Test
  void rejectsInvalidMountAndTokenFile() {
    for (String mount : new String[] {"", "/transit", "transit/", "team//transit", ".", ".."}) {
      Map<String, String> properties = properties();
      properties.put(OpenBaoTransitKmsClientFactory.TRANSIT_MOUNT, mount);
      assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, properties));
    }

    Map<String, String> properties = properties();
    properties.put(OpenBaoTransitKmsClientFactory.TOKEN_FILE, "relative-token");
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, properties));
  }

  private Map<String, String> properties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        OpenBaoTransitKmsClientFactory.SERVICE_ADDRESS,
        String.format("http://127.0.0.1:%s", server.getAddress().getPort()));
    properties.put(OpenBaoTransitKmsClientFactory.TOKEN_FILE, tokenFile.toString());
    properties.put(OpenBaoTransitKmsClientFactory.CREDENTIAL_METHOD, "token_file");
    return properties;
  }

  private void assertInvalidServiceAddress(String address) {
    Map<String, String> properties = properties();
    properties.put(OpenBaoTransitKmsClientFactory.SERVICE_ADDRESS, address);
    assertThrows(IllegalArgumentException.class, () -> factory().create(SOURCE, properties));
  }

  private void respond(HttpExchange exchange) throws IOException {
    requestedPath.set(exchange.getRequestURI().getRawPath());
    byte[] response =
        ("{\"data\":{\"supports_encryption\":true,\"supports_decryption\":true,"
                + "\"soft_deleted\":false}}")
            .getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }
}
