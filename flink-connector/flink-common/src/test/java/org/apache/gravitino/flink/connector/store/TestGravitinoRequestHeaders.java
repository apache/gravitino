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

package org.apache.gravitino.flink.connector.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests custom request headers through the Flink catalog store configuration and HTTP client. */
public class TestGravitinoRequestHeaders {

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  @SuppressWarnings("deprecation")
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
    GravitinoCatalogStoreFactory factory = new GravitinoCatalogStoreFactory();
    try {
      Configuration conf = new Configuration();
      String prefix = "table.catalog-store.gravitino.gravitino.";
      conf.setString("table.catalog-store.kind", "gravitino");
      conf.setString(prefix + "uri", uri);
      conf.setString(prefix + "metalake", "test");
      conf.setString(prefix + "client.socketTimeoutMs", "3000");
      if (basicAuth) {
        conf.setString(prefix + "client.auth.type", "basic");
        conf.setString(prefix + "client.basic.username", "alice");
        conf.setString(prefix + "client.basic.password", "secret");
      }
      if (customHeaders) {
        conf.setString(prefix + "client.request.header.X-Job-Id", "job-123");
        conf.setString(prefix + "client.request.header.X-Execution-Id", "run:456");
      }
      factory.open(
          TableFactoryUtil.buildCatalogStoreFactoryContext(conf, getClass().getClassLoader()));
      assertEquals(
          "3000",
          GravitinoCatalogManager.get()
              .getGravitinoClientConfig()
              .get("gravitino.client.socketTimeoutMs"));
      assertFalse(requests.isEmpty());
      for (Headers headers : requests) {
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
      factory.close();
      server.stop(0);
    }
  }
}
