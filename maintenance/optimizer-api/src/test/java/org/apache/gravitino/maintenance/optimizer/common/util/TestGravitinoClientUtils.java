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

package org.apache.gravitino.maintenance.optimizer.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Test;

class TestGravitinoClientUtils {

  @Test
  void testFetchSchemaSeparatorFromConfigsEndpoint() throws Exception {
    assertThrows(
        IllegalArgumentException.class, () -> GravitinoClientUtils.fetchSchemaSeparator(null));
    assertThrows(
        IllegalArgumentException.class, () -> GravitinoClientUtils.fetchSchemaSeparator("  "));

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/configs",
        exchange -> {
          byte[] body = "{\"gravitino.schema.separator\":\"|\"}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      assertEquals("|", GravitinoClientUtils.fetchSchemaSeparator(uri));
      assertEquals("|", GravitinoClientUtils.fetchSchemaSeparator(uri + "/"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void testNormalizeUpdaterConnectionOptionsPromotesAliases() {
    Map<String, String> options = new HashMap<>();
    options.put("gravitino_uri", " http://localhost:8090 ");
    options.put("metalake", " ml ");
    options.put("auth_type", "basic");
    Map<String, String> normalized =
        GravitinoClientUtils.normalizeUpdaterConnectionOptions(options);
    assertEquals("http://localhost:8090", normalized.get(OptimizerConfig.GRAVITINO_URI));
    assertEquals("ml", normalized.get(OptimizerConfig.GRAVITINO_METALAKE));
    assertEquals("basic", normalized.get(OptimizerConfig.AUTH_TYPE));
  }

  @Test
  void testFetchSchemaSeparatorRejectsMissingKey() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext(
        "/configs",
        exchange -> {
          byte[] body = "{\"unrelated\":\"x\"}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      assertThrows(
          IllegalArgumentException.class, () -> GravitinoClientUtils.fetchSchemaSeparator(uri));
    } finally {
      server.stop(0);
    }
  }
}
