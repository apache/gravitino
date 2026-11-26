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

package org.apache.gravitino.spark.connector.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.spark.SparkConf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests custom request headers through the Spark connector's real HTTP client. */
public class TestGravitinoRequestHeaders {

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testRequestHeaders(boolean basicAuth, boolean customHeaders) throws Exception {
    List<Headers> requests = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            requests.add(exchange.getRequestHeaders());
            String body =
                exchange.getRequestURI().getPath().equals("/api/version")
                    ? "{\"code\":0,\"version\":{\"version\":\"99.0.0\","
                        + "\"compileDate\":\"test\",\"gitCommit\":\"test\"}}"
                    : exchange.getRequestURI().getPath().endsWith("/catalogs")
                        ? "{\"code\":0,\"catalogs\":[]}"
                        : "{\"code\":0,\"metalake\":{\"name\":\"test\",\"audit\":{}}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
          } finally {
            exchange.close();
          }
        });
    server.start();
    String uri = "http://127.0.0.1:" + server.getAddress().getPort();
    try {
      SparkConf conf = new SparkConf(false);
      conf.set("spark.sql.gravitino.client.socketTimeoutMs", "3000");
      if (basicAuth) {
        conf.set("spark.sql.gravitino.authType", "basic");
        conf.set("spark.sql.gravitino.basic.username", "alice");
        conf.set("spark.sql.gravitino.basic.password", "secret");
      }
      if (customHeaders) {
        conf.set("spark.sql.gravitino.client.request.header.Authorization", "custom-auth");
        conf.set("spark.sql.gravitino.client.request.header.X-Job-Id", "job-123");
        conf.set("spark.sql.gravitino.client.request.header.X-Execution-Id", "run:456");
      }
      Map<String, String> clientConfig = GravitinoDriverPlugin.extractGravitinoClientConfig(conf);
      assertEquals(ImmutableMap.of("gravitino.client.socketTimeoutMs", "3000"), clientConfig);
      try (GravitinoClient client =
          GravitinoDriverPlugin.createGravitinoClient(uri, "test", conf, "alice", clientConfig)) {
        assertEquals(0, client.listCatalogsInfo(false).length);
        // Repeat the request to verify that the client reuses the configured headers.
        assertEquals(0, client.listCatalogsInfo(false).length);
      }
      assertFalse(requests.isEmpty());
      for (Headers headers : requests) {
        assertNotEquals("custom-auth", headers.getFirst("Authorization"));
        if (customHeaders) {
          assertEquals("job-123", headers.getFirst("X-Job-Id"));
          assertEquals("run:456", headers.getFirst("X-Execution-Id"));
        } else {
          assertNull(headers.getFirst("X-Job-Id"));
          assertNull(headers.getFirst("X-Execution-Id"));
        }
        if (basicAuth) {
          assertEquals("Basic YWxpY2U6c2VjcmV0", headers.getFirst("Authorization"));
        }
      }
    } finally {
      server.stop(0);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  void testBlankHeaderName(String name) {
    SparkConf conf = new SparkConf(false);
    conf.set("spark.sql.gravitino.client.request.header." + name, "value");
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1",
                    "test",
                    conf,
                    "alice",
                    GravitinoDriverPlugin.extractGravitinoClientConfig(conf)));
    assertTrue(error.getMessage().contains("header name must not be blank"));
    assertTrue(error.getMessage().contains("spark.sql.gravitino.client.request.header." + name));
  }
}
