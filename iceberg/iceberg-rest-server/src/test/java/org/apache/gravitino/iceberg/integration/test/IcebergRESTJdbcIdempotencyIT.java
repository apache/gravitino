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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.gravitino.Configs;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Exercises durable idempotency through the Iceberg REST auxiliary service and a real database. */
@EnabledIfEnvironmentVariable(named = "jdbcBackend", matches = "(?i)h2")
public class IcebergRESTJdbcIdempotencyIT extends BaseIT {

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String DIFFERENT_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";
  private static final String QUERY_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073991";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final ObjectMapper MAPPER = IcebergObjectMapper.getInstance();

  @TempDir private static Path dataDirectory;

  private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

  /** Starts the auxiliary service with the same databases on every start within this test class. */
  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    ignoreIcebergAuxRestService = false;
    // BaseIT allocates a new default entity-store path on every start. An explicit URL keeps
    // reservations on disk across the restart and disables H2 auto-server for this single server.
    customConfigs.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY,
        "jdbc:h2:file:"
            + dataDirectory.resolve("entity-store")
            + ";MODE=MYSQL;AUTO_SERVER=FALSE;WRITE_DELAY=0");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
        IcebergConstants.STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME);
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_IDEMPOTENCY_ENABLED, "true");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE, "jdbc");
    customConfigs.put(GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.CATALOG_BACKEND, "jdbc");
    // Iceberg looks up the catalog's schema columns by their lowercase names on restart.
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.URI,
        "jdbc:h2:file:" + dataDirectory.resolve("iceberg-catalog") + ";DATABASE_TO_LOWER=TRUE");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.h2.Driver");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_USER, "iceberg");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_JDBC_PASSWORD, "iceberg");
    customConfigs.put(
        GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.WAREHOUSE,
        dataDirectory.resolve("warehouse").toUri().toString());
    super.startIntegrationTest();
  }

  @Test
  void testReplaysOriginalResponseAfterServerRestart() throws Exception {
    String namespace = "durable_replay";
    HttpResponse<String> first = createNamespace(namespace, KEY, "?a=1&b=2");
    assertStatus(200, first);
    Assertions.assertEquals(namespace, MAPPER.readTree(first.body()).at("/namespace/0").asText());

    stopIntegrationTest();
    startIntegrationTest();

    HttpResponse<String> retry = createNamespace(namespace, KEY, "?b=2&a=1");
    assertStatus(200, retry);
    Assertions.assertEquals(MAPPER.readTree(first.body()), MAPPER.readTree(retry.body()));

    // A different key executes the mutation, proving the catalog still contains the namespace.
    HttpResponse<String> duplicate = createNamespace(namespace, DIFFERENT_KEY, "?a=1&b=2");
    assertStatus(409, duplicate);
    Assertions.assertEquals(
        "AlreadyExistsException", MAPPER.readTree(duplicate.body()).at("/error/type").asText());
  }

  @Test
  void testRejectsSameKeyWithDifferentQuery() throws IOException, InterruptedException {
    String namespace = "query_binding";
    assertStatus(200, createNamespace(namespace, QUERY_KEY, "?tag=first"));

    HttpResponse<String> conflict = createNamespace(namespace, QUERY_KEY, "?tag=second");
    assertStatus(409, conflict);
    Assertions.assertEquals(
        "IdempotencyConflictException",
        MAPPER.readTree(conflict.body()).at("/error/type").asText());
  }

  private HttpResponse<String> createNamespace(String namespace, String key, String query)
      throws IOException, InterruptedException {
    CreateNamespaceRequest payload =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    HttpRequest httpRequest =
        HttpRequest.newBuilder(URI.create(getIcebergRestServiceUri() + "v1/namespaces" + query))
            .timeout(TIMEOUT)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", key)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(payload)))
            .build();
    return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
  }

  private static void assertStatus(int expected, HttpResponse<String> response) {
    Assertions.assertEquals(expected, response.statusCode(), response::body);
  }
}
