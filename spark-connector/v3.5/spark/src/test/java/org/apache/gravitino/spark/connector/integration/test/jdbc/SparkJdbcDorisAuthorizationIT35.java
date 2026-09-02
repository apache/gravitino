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
package org.apache.gravitino.spark.connector.integration.test.jdbc;

import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_URL;
import static org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants.GRAVITINO_JDBC_USER;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Verifies that Gravitino denies Doris writes before any configured physical endpoint is used. */
public class SparkJdbcDorisAuthorizationIT35 extends BaseIT {

  private static final String METALAKE = "doris_authorization";
  private static final String CATALOG = "jdbc_doris_authorization";
  private static final String DATABASE = "denied_database";
  private static final String TABLE = "denied_table";
  private static final String ADMIN_USER = "doris_authorization_admin";
  private static final String DENIED_USER = "doris_authorization_denied";
  private static final String ROLE = "doris_authorization_role";
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  private CountingHttpEndpoint httpEndpoint;
  private CountingTcpEndpoint tcpEndpoint;
  private SparkSession spark;
  private String unusedPassword;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    httpEndpoint = CountingHttpEndpoint.start();
    tcpEndpoint = CountingTcpEndpoint.start();
    unusedPassword = "it-" + UUID.randomUUID().toString().replace("-", "");
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            ADMIN_USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.CACHE_ENABLED.getKey(),
            "false",
            Configs.AUTHENTICATORS.getKey(),
            "simple",
            Configs.SERVICE_ADMINS.getKey(),
            ADMIN_USER));
    super.startIntegrationTest();

    GravitinoMetalake metalake = createAuthorizationMetadata();
    metalake.grantRolesToUser(ImmutableList.of(ROLE), DENIED_USER);

    setEnv("SPARK_USER", DENIED_USER);
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(
                GravitinoSparkConfig.GRAVITINO_URI,
                String.format("http://127.0.0.1:%d", getGravitinoServerPort()))
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Doris authorization-before-I/O integration test")
            .config(sparkConf)
            .getOrCreate();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (spark != null) {
      spark.close();
    }
    setEnv("SPARK_USER", AuthConstants.ANONYMOUS_USER);
    try {
      super.stopIntegrationTest();
    } finally {
      if (httpEndpoint != null) {
        httpEndpoint.close();
      }
      if (tcpEndpoint != null) {
        tcpEndpoint.close();
      }
    }
  }

  @Test
  void testModifyDenialPrecedesAllConfiguredDorisEndpoints() {
    httpEndpoint.reset();
    tcpEndpoint.reset();
    SparkSession deniedSession = spark.newSession();
    CatalogManager catalogManager = deniedSession.sessionState().catalogManager();
    try {
      ForbiddenException failure =
          Assertions.assertThrows(
              ForbiddenException.class,
              () ->
                  deniedSession
                      .sql(
                          "INSERT INTO "
                              + CATALOG
                              + "."
                              + DATABASE
                              + "."
                              + TABLE
                              + " VALUES (1, 'denied')")
                      .collectAsList());
      assertSecretNotExposed(failure, unusedPassword);
      await()
          .during(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(3))
          .untilAsserted(
              () -> {
                Assertions.assertEquals(0, httpEndpoint.requestCount());
                Assertions.assertEquals(0, tcpEndpoint.connectionCount());
              });
    } finally {
      catalogManager.reset();
    }
  }

  private GravitinoMetalake createAuthorizationMetadata() {
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.addUser(DENIED_USER);
    Map<String, String> properties = new HashMap<>();
    properties.put(GRAVITINO_JDBC_URL, "jdbc:mysql://127.0.0.1:" + tcpEndpoint.port() + "/");
    properties.put(GRAVITINO_JDBC_USER, "unused-user");
    properties.put(GRAVITINO_JDBC_PASSWORD, unusedPassword);
    properties.put(GRAVITINO_JDBC_DRIVER, JDBC_DRIVER);
    properties.put("doris-fenodes", "127.0.0.1:" + httpEndpoint.port());
    properties.put("doris-query-port", Integer.toString(tcpEndpoint.port()));
    properties.put("doris-write-mode", "batch");
    metalake.createCatalog(CATALOG, Catalog.Type.RELATIONAL, "jdbc-doris", "", properties);

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            CATALOG,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow(),
                Privileges.ModifyTable.deny()));
    metalake.createRole(ROLE, new HashMap<>(), ImmutableList.of(catalogObject));
    return metalake;
  }

  private static void assertSecretNotExposed(Throwable failure, String secret) {
    Throwable current = failure;
    while (current != null) {
      Assertions.assertFalse(String.valueOf(current.getMessage()).contains(secret));
      current = current.getCause();
    }
  }

  private static final class CountingHttpEndpoint implements AutoCloseable {

    private final HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();

    private static CountingHttpEndpoint start() throws IOException {
      return new CountingHttpEndpoint();
    }

    private CountingHttpEndpoint() throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/", this::reject);
      server.start();
    }

    private int port() {
      return server.getAddress().getPort();
    }

    private int requestCount() {
      return requests.get();
    }

    private void reset() {
      requests.set(0);
    }

    private void reject(HttpExchange exchange) throws IOException {
      requests.incrementAndGet();
      byte[] body = "unexpected Doris FE request".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(503, body.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(body);
      } finally {
        exchange.close();
      }
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static final class CountingTcpEndpoint implements AutoCloseable {

    private final ServerSocket server;
    private final AtomicInteger connections = new AtomicInteger();
    private final Thread acceptThread;

    private static CountingTcpEndpoint start() throws IOException {
      return new CountingTcpEndpoint();
    }

    private CountingTcpEndpoint() throws IOException {
      server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      acceptThread = new Thread(this::acceptConnections, "doris-authorization-tcp-counter");
      acceptThread.setDaemon(true);
      acceptThread.start();
    }

    private int port() {
      return server.getLocalPort();
    }

    private int connectionCount() {
      return connections.get();
    }

    private void reset() {
      connections.set(0);
    }

    private void acceptConnections() {
      while (!server.isClosed()) {
        try (Socket ignored = server.accept()) {
          connections.incrementAndGet();
        } catch (SocketException e) {
          if (!server.isClosed()) {
            throw new IllegalStateException("Doris authorization TCP counter failed", e);
          }
        } catch (IOException e) {
          throw new IllegalStateException("Doris authorization TCP counter failed", e);
        }
      }
    }

    @Override
    public void close() throws IOException {
      server.close();
      try {
        acceptThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
