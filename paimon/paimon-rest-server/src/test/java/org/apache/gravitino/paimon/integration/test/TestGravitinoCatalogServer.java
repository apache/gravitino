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
package org.apache.gravitino.paimon.integration.test;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.VersionDTO;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.SecretsResponse;
import org.apache.gravitino.dto.responses.VersionResponse;
import org.apache.gravitino.json.JsonUtils;

/** Minimal authenticated Gravitino HTTP fixture for real client and engine integration tests. */
public class TestGravitinoCatalogServer implements AutoCloseable {
  private final HttpServer server;
  private final AtomicInteger catalogLoads = new AtomicInteger();

  /**
   * Starts a fixture exposing a registered filesystem Paimon catalog.
   *
   * @param warehouse shared engine and service warehouse
   * @throws Exception if the fixture cannot bind its local socket
   */
  public TestGravitinoCatalogServer(Path warehouse) throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AuditDTO audit = AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build();
    server.createContext(
        "/",
        exchange -> {
          String authorization = exchange.getRequestHeaders().getFirst("Authorization");
          if (authorization == null || !authorization.startsWith("Basic ")) {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          String path = exchange.getRequestURI().getPath();
          Object response;
          if (path.equals("/api/version")) {
            response = new VersionResponse(new VersionDTO("2.0.0-SNAPSHOT", "test", "test"));
          } else if (path.equals("/api/metalakes/ml")) {
            response =
                new MetalakeResponse(
                    MetalakeDTO.builder()
                        .withName("ml")
                        .withProperties(Map.of())
                        .withAudit(audit)
                        .build());
          } else if (path.equals("/api/metalakes/ml/catalogs/default_catalog")) {
            catalogLoads.incrementAndGet();
            response =
                new CatalogResponse(
                    CatalogDTO.builder()
                        .withName("default_catalog")
                        .withType(CatalogDTO.Type.RELATIONAL)
                        .withProvider("lakehouse-paimon")
                        .withAudit(audit)
                        .withProperties(
                            Map.of(
                                "catalog-backend",
                                "filesystem",
                                "warehouse",
                                warehouse.toUri().toString()))
                        .build());
          } else if (path.endsWith("/secrets")) {
            response = new SecretsResponse(Map.of());
          } else if (path.endsWith("/credentials")) {
            response = new CredentialResponse(new CredentialDTO[0]);
          } else {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
          }
          byte[] body = JsonUtils.objectMapper().writeValueAsBytes(response);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
  }

  /**
   * @return the fixture's HTTP URI
   */
  public String uri() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /**
   * @return number of authenticated catalog loads
   */
  public int catalogLoads() {
    return catalogLoads.get();
  }

  /** Stops the HTTP fixture and releases its listening socket. */
  @Override
  public void close() {
    server.stop(0);
  }
}
