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

package org.apache.gravitino.spark.connector.plugin.restcatalog.lance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLanceRESTCatalogProvider {

  private HttpServer server;
  private String serverUri;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    serverUri = "http://localhost:" + server.getAddress().getPort() + "/lance/";
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void testListsAllCatalogPages() {
    AtomicInteger requests = new AtomicInteger();
    server.createContext(
        "/lance/v1/namespace/$/list",
        exchange -> {
          int request = requests.getAndIncrement();
          if (request == 0) {
            assertFalse(exchange.getRequestURI().getQuery().contains("page_token"));
            respond(exchange, 200, "{\"namespaces\":[\"catalog_b\"],\"page_token\":\"next\"}");
          } else {
            assertTrue(exchange.getRequestURI().getQuery().contains("page_token=next"));
            respond(exchange, 200, "{\"namespaces\":[\"catalog_a\"]}");
          }
        });
    server.start();

    List<String> catalogs =
        new LanceRESTCatalogProvider().listCatalogs(serverUri, Collections.emptyMap());

    assertEquals(2, requests.get());
    assertEquals(Arrays.asList("catalog_b", "catalog_a"), catalogs);
  }

  @Test
  void testRejectsRepeatedPageToken() {
    server.createContext(
        "/lance/v1/namespace/$/list",
        exchange ->
            respond(exchange, 200, "{\"namespaces\":[\"catalog_a\"],\"page_token\":\"repeat\"}"));
    server.start();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> new LanceRESTCatalogProvider().listCatalogs(serverUri, Collections.emptyMap()));

    assertTrue(exception.getMessage().contains("repeated page token"));
  }

  @Test
  void testGeneratedCatalogConfiguration() {
    LanceRESTCatalogProvider provider = new LanceRESTCatalogProvider();

    Map<String, String> properties =
        provider.generatedCatalogProperties("http://localhost:9101/lance", "catalog_a");

    assertEquals("lance", provider.format());
    assertEquals("org.lance.spark.LanceNamespaceSparkCatalog", provider.catalogClassName());
    assertEquals("rest", properties.get("impl"));
    assertEquals("http://localhost:9101/lance", properties.get("uri"));
    assertEquals("catalog_a", properties.get("parent"));
    assertEquals(3, properties.size());
    assertEquals(
        "org.lance.spark.extensions.LanceSparkSessionExtensions", provider.sparkExtensions()[0]);
  }

  private static void respond(HttpExchange exchange, int statusCode, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(statusCode, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
