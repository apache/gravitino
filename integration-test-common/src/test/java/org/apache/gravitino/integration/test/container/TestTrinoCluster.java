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
package org.apache.gravitino.integration.test.container;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

/**
 * Tests how {@link TrinoCluster} reads Trino responses and waits for a coordinator; a stub HTTP
 * server stands in for Trino, so nothing here starts a container.
 */
public class TestTrinoCluster {

  @Test
  void testInfoSaysWhetherTheNodeHasStarted() {
    Assertions.assertFalse(TrinoCluster.hasStarted("{\"coordinator\":true,\"starting\":true}"));
    Assertions.assertTrue(
        TrinoCluster.hasStarted("{\"coordinator\":true, \"starting\" : false,\"uptime\":\"1s\"}"));
    Assertions.assertFalse(TrinoCluster.hasStarted(""));
  }

  @Test
  void testCoordinatorIsReadyOnceStartedWithTheCatalogAndEveryWorker() throws IOException {
    AtomicInteger infoCalls = new AtomicInteger();
    List<String> queries = new ArrayList<>();
    HttpServer trino =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    // Still starting on the first call; then up with the catalog and two active workers.
    trino.createContext(
        "/v1/info",
        exchange -> respond(exchange, "{\"starting\":" + (infoCalls.incrementAndGet() == 1) + "}"));
    trino.createContext(
        "/v1/statement",
        exchange -> {
          String sql = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          queries.add(sql);
          int count = sql.contains("system.runtime.nodes") ? 2 : 1;
          respond(exchange, "{\"id\":\"q\",\"data\":[[" + count + "]]}");
        });
    trino.start();
    try {
      WaitStrategyTarget target = Mockito.mock(WaitStrategyTarget.class);
      Mockito.when(target.getHost()).thenReturn(InetAddress.getLoopbackAddress().getHostAddress());
      Mockito.when(target.getMappedPort(TrinoCluster.TRINO_PORT))
          .thenReturn(trino.getAddress().getPort());

      TrinoCluster.coordinatorReadiness(2).waitUntilReady(target);

      Assertions.assertEquals(2, infoCalls.get());
      Assertions.assertEquals(2, queries.size(), queries.toString());
      Assertions.assertTrue(queries.get(0).contains("catalog_name = 'gravitino'"), queries.get(0));
      Assertions.assertTrue(queries.get(1).contains("state = 'active'"), queries.get(1));
    } finally {
      trino.stop(0);
    }
  }

  @Test
  void testPrivateClusterGivesItsCoordinatorARunName() {
    Assertions.assertEquals("trino", TrinoCluster.builder().coordinatorHost());
    String name = TrinoCluster.builder().withPrivateNames(true).coordinatorHost();
    Assertions.assertTrue(name.startsWith("gravitino-trino-" + RunNames.runId() + "-"), name);
    Assertions.assertNotEquals(
        name, TrinoCluster.builder().withPrivateNames(true).coordinatorHost());
  }

  private static void respond(HttpExchange exchange, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  @Test
  void testQueuedPageHasNextUriAndNoValue() {
    String queued =
        "{\"id\":\"q1\",\"infoUri\":\"http://localhost:8080/ui/query.html?q1\","
            + "\"nextUri\":\"http://localhost:8080/v1/statement/queued/q1/x/1\","
            + "\"stats\":{\"state\":\"QUEUED\"}}";

    Assertions.assertEquals(
        Optional.of("http://localhost:8080/v1/statement/queued/q1/x/1"),
        TrinoCluster.nextUri(queued));
    Assertions.assertEquals(Optional.empty(), TrinoCluster.firstValue(queued));
  }

  @Test
  void testDataPageHasTheCount() {
    String data =
        "{\"id\":\"q1\",\"nextUri\":\"http://localhost:8080/v1/statement/executing/q1/y/2\","
            + "\"columns\":[{\"name\":\"_col0\",\"type\":\"bigint\"}],\"data\": [ [2] ],"
            + "\"stats\":{\"state\":\"RUNNING\"}}";

    Assertions.assertEquals(Optional.of(2), TrinoCluster.firstValue(data));
  }

  @Test
  void testLastPageHasNoNextUri() {
    String finished = "{\"id\":\"q1\",\"stats\":{\"state\":\"FINISHED\"}}";

    Assertions.assertEquals(Optional.empty(), TrinoCluster.nextUri(finished));
    Assertions.assertEquals(Optional.empty(), TrinoCluster.firstValue(finished));
  }
}
