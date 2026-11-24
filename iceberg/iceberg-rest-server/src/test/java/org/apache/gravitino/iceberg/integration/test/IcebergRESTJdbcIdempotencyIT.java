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
package org.apache.gravitino.iceberg.integration.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Configs;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises durable idempotency through the Iceberg REST auxiliary service in the H2, MySQL, and
 * PostgreSQL lanes. H2 uses file-backed databases. In the Docker lanes the Iceberg catalog uses its
 * own tables in the database provisioned for the entity store.
 */
public class IcebergRESTJdbcIdempotencyIT extends BaseIT {

  private static final String CATALOG_URI = GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.URI;
  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String DIFFERENT_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";
  private static final String QUERY_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073991";
  private static final int TIMEOUT_SECONDS = 30;
  private static final ObjectMapper MAPPER = IcebergObjectMapper.getInstance();

  @TempDir private static Path dataDirectory;

  private final Client httpClient =
      ClientBuilder.newBuilder()
          .connectTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .readTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .build();
  private boolean serverNeedsTeardown;

  /** Starts the auxiliary service with the same databases on every start within this test class. */
  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Assumptions.assumeTrue(
        !usesDockerBackend() || Boolean.parseBoolean(System.getenv("dockerTest")),
        "The MySQL and PostgreSQL integration lanes require Docker tests to be enabled");
    ignoreIcebergAuxRestService = false;
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED, "true");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE, "jdbc");
    customConfigs.put(GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.CATALOG_BACKEND, "jdbc");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.WAREHOUSE,
        dataDirectory.resolve("warehouse").toUri().toString());
    startAndAwaitIcebergRest();
  }

  /** Configures the databases once so restarting the server keeps the catalog and reservations. */
  @Override
  protected void configureJdbcBackend() throws SQLException {
    // Restart restores the original configs before starting the server again.
    if (customConfigs.containsKey(CATALOG_URI)) {
      return;
    }
    if (usesDockerBackend()) {
      super.configureJdbcBackend();
      configureIcebergCatalog(
          customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY),
          customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY),
          customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY),
          customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY));
      return;
    }

    // BaseIT allocates a new default entity-store path on each start.
    customConfigs.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY,
        "jdbc:h2:file:"
            + dataDirectory.resolve("entity-store")
            + ";MODE=MYSQL;AUTO_SERVER=FALSE;WRITE_DELAY=0");
    // Iceberg looks up the catalog's schema columns by their lowercase names on restart.
    configureIcebergCatalog(
        "jdbc:h2:file:" + dataDirectory.resolve("iceberg-catalog") + ";DATABASE_TO_LOWER=TRUE",
        "org.h2.Driver",
        "iceberg",
        "iceberg");
  }

  /** Stops the active server, if any, and releases the HTTP client. */
  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    try {
      stopTestServer();
    } finally {
      httpClient.close();
    }
  }

  @Test
  void testReplaysOriginalResponseAfterServerRestart() throws Exception {
    String namespace = "durable_replay";
    JsonNode original;
    try (Response first = createNamespace(namespace, KEY, "?a=1&b=2")) {
      original = assertStatus(200, first);
      Assertions.assertEquals(namespace, original.at("/namespace/0").asText());
    }

    restartTestServer();

    try (Response retry = createNamespace(namespace, KEY, "?b=2&a=1")) {
      Assertions.assertEquals(original, assertStatus(200, retry));
    }

    // A different key executes the mutation, proving the catalog still contains the namespace.
    try (Response duplicate = createNamespace(namespace, DIFFERENT_KEY, "?a=1&b=2")) {
      Assertions.assertEquals(
          "AlreadyExistsException", assertStatus(409, duplicate).at("/error/type").asText());
    }
  }

  @Test
  void testRejectsSameKeyWithDifferentQuery() throws IOException {
    String namespace = "query_binding";
    JsonNode original;
    try (Response first = createNamespace(namespace, QUERY_KEY, "?tag=first")) {
      original = assertStatus(200, first);
    }

    try (Response conflict = createNamespace(namespace, QUERY_KEY, "?tag=second")) {
      Assertions.assertEquals(
          "IdempotencyConflictException", assertStatus(409, conflict).at("/error/type").asText());
      Assertions.assertNull(conflict.getHeaderString(HttpHeaders.RETRY_AFTER));
    }

    try (Response retry = createNamespace(namespace, QUERY_KEY, "?tag=first")) {
      Assertions.assertEquals(original, assertStatus(200, retry));
    }
  }

  private void startAndAwaitIcebergRest() throws Exception {
    serverNeedsTeardown = true;
    startServer();
    Awaitility.await()
        .alias("Iceberg REST config endpoint")
        .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
        .pollInterval(Duration.ofMillis(250))
        .ignoreExceptionsInstanceOf(ProcessingException.class)
        .until(
            () -> {
              try (Response response =
                  httpClient.target(getIcebergRestServiceUri()).path("v1/config").request().get()) {
                return response.getStatus() == 200;
              }
            });
  }

  private boolean usesDockerBackend() {
    String backend = getJDBCBackend();
    return "mysql".equalsIgnoreCase(backend) || "postgresql".equalsIgnoreCase(backend);
  }

  private void configureIcebergCatalog(String url, String driver, String user, String password) {
    customConfigs.put(CATALOG_URI, url);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_DRIVER, driver);
    customConfigs.put(GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_USER, user);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_PASSWORD, password);
  }

  private void restartTestServer() throws Exception {
    // BaseIT clears its configuration map during shutdown.
    Map<String, String> configs = Map.copyOf(customConfigs);
    stopTestServer();
    customConfigs.putAll(configs);
    startAndAwaitIcebergRest();
  }

  private void stopTestServer() throws IOException, InterruptedException {
    if (serverNeedsTeardown) {
      serverNeedsTeardown = false;
      super.stopIntegrationTest();
    }
  }

  private Response createNamespace(String namespace, String key, String query) throws IOException {
    CreateNamespaceRequest payload =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    return httpClient
        .target(getIcebergRestServiceUri() + "v1/namespaces" + query)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header("Idempotency-Key", key)
        .post(Entity.json(MAPPER.writeValueAsString(payload)));
  }

  private static JsonNode assertStatus(int expected, Response response) throws IOException {
    String body = response.readEntity(String.class);
    Assertions.assertEquals(expected, response.getStatus(), () -> body);
    return MAPPER.readTree(body);
  }
}
