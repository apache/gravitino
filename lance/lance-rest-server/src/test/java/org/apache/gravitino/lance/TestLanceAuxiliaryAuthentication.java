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
package org.apache.gravitino.lance;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.server.authentication.Authenticator;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerTestUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestLanceAuxiliaryAuthentication {
  private final Map<String, Object> savedEnvironment = new HashMap<>();
  private Object savedAuthenticators;
  private LanceRESTService service;

  @BeforeEach
  void saveEnvironment() throws Exception {
    for (String field :
        List.of(
            "config",
            "metricsSystem",
            "eventBus",
            "catalogDispatcher",
            "schemaDispatcher",
            "tableDispatcher")) {
      savedEnvironment.put(field, FieldUtils.readField(GravitinoEnv.getInstance(), field, true));
    }
    savedAuthenticators =
        FieldUtils.readField(ServerAuthenticator.getInstance(), "authenticators", true);
  }

  @AfterEach
  void restoreEnvironment() throws Exception {
    try {
      if (service != null) {
        service.serviceStop();
      }
    } finally {
      for (Map.Entry<String, Object> entry : savedEnvironment.entrySet()) {
        FieldUtils.writeField(GravitinoEnv.getInstance(), entry.getKey(), entry.getValue(), true);
      }
      FieldUtils.writeField(
          ServerAuthenticator.getInstance(), "authenticators", savedAuthenticators, true);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Basic YWxpY2U6cmVhbC1wYXNzd29yZA==",
        "Bearer caller-token",
        "Negotiate caller-token"
      })
  void testAuthenticatedNamespaceAndTableCallsDoNotNeedLoopbackCredentials(String authorization)
      throws Exception {
    // The real AuthenticationFilter runs against a protocol-specific test authenticator. This
    // exercises service wiring and identity propagation, not OAuth or Kerberos token validation.
    Authenticator authenticator = Mockito.mock(Authenticator.class);
    Mockito.when(authenticator.isDataFromToken()).thenReturn(true);
    Mockito.when(authenticator.supportsToken(Mockito.any()))
        .thenAnswer(call -> call.getArgument(0) != null);
    Mockito.when(authenticator.authenticateToken(Mockito.any()))
        .thenAnswer(
            call -> {
              String header = new String((byte[]) call.getArgument(0), StandardCharsets.UTF_8);
              if (!authorization.equals(header)) {
                throw new UnauthorizedException("Invalid caller credentials");
              }
              return new UserPrincipal("alice", header);
            });
    FieldUtils.writeField(
        ServerAuthenticator.getInstance(), "authenticators", List.of(authenticator), true);

    Config config = new Config(false) {};
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "metricsSystem",
        Mockito.mock(MetricsSystem.class, Mockito.RETURNS_DEEP_STUBS),
        true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "eventBus", new EventBus(List.of()), true);

    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    Mockito.when(catalog.provider()).thenReturn("lakehouse-generic");
    Mockito.when(catalog.name()).thenReturn("catalog");
    CatalogDispatcher catalogs = Mockito.mock(CatalogDispatcher.class);
    Mockito.when(catalogs.loadCatalog(Mockito.any()))
        .thenAnswer(
            call -> {
              assertCaller();
              return catalog;
            });
    SchemaDispatcher schemas = Mockito.mock(SchemaDispatcher.class);
    Mockito.when(schemas.schemaExists(Mockito.any()))
        .thenAnswer(
            call -> {
              assertCaller();
              return true;
            });
    TableDispatcher tables = Mockito.mock(TableDispatcher.class);
    Mockito.when(tables.tableExists(Mockito.any()))
        .thenAnswer(
            call -> {
              assertCaller();
              return true;
            });
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", catalogs, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemas, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tables, true);

    service = new LanceRESTService();
    service.serviceInit(
        Map.of(
            "httpPort", "0",
            "host", "127.0.0.1",
            "gravitino-metalake", "metalake",
            // An unusable HTTP target ensures that success requires the local dispatcher path.
            "gravitino-uri", "http://127.0.0.1:0"),
        true);
    service.serviceStart();
    JettyServer server = (JettyServer) FieldUtils.readField(service, "server", true);
    int port =
        ((ServerConnector)
                JettyServerTestUtils.getServletContextHandler(server)
                    .getServer()
                    .getConnectors()[0])
            .getLocalPort();
    HttpClient client = HttpClient.newHttpClient();
    for (String path :
        List.of("namespace/catalog$schema/exists", "table/catalog$schema$table/exists")) {
      URI uri = URI.create("http://127.0.0.1:" + port + "/lance/v1/" + path);
      HttpResponse<String> response =
          client.send(
              HttpRequest.newBuilder(uri)
                  .header("Authorization", authorization)
                  .POST(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      Assertions.assertEquals(200, response.statusCode(), response.body());

      Mockito.clearInvocations(catalogs, schemas, tables);
      HttpResponse<String> rejected =
          client.send(
              HttpRequest.newBuilder(uri)
                  .header("Authorization", "Basic bGFuY2UtcmVzdC1zZXJ2ZXI6ZHVtbXk=")
                  .POST(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      Assertions.assertEquals(401, rejected.statusCode(), rejected.body());
      Mockito.verifyNoInteractions(catalogs, schemas, tables);
    }
  }

  private static void assertCaller() {
    Assertions.assertEquals("alice", PrincipalUtils.getCurrentUserName());
  }
}
