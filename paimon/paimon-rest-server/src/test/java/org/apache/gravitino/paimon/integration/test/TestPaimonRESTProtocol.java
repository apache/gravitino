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

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.paimon.RESTService;
import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.ResourcePaths;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.ListTablesGloballyResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises error contracts and named JDBC catalogs through the real HTTP server. */
public class TestPaimonRESTProtocol {

  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @TempDir static Path directory;
  private static RESTService service;
  private static URI base;

  @BeforeAll
  static void start() throws Exception {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    base = URI.create("http://127.0.0.1:" + port + "/paimon/");
    Map<String, String> config = new HashMap<>();
    config.put("host", "127.0.0.1");
    config.put("httpPort", String.valueOf(port));
    config.put("warehouse", directory.resolve("filesystem").toUri().toString());
    config.put("token", "protocol-test");
    config.put("catalog.sales.catalog-backend", "jdbc");
    config.put("catalog.sales.jdbc-driver", "org.sqlite.JDBC");
    config.put("catalog.sales.uri", "jdbc:sqlite:" + directory.resolve("catalog.db"));
    config.put("catalog.sales.warehouse", directory.resolve("sales").toUri().toString());
    config.put("catalog.storage.warehouse", directory.resolve("storage").toUri().toString());
    config.put("catalog.storage.data-token.enabled", "true");
    config.put("catalog.storage.fs.s3a.access-key", "test-access-key");
    config.put("catalog.storage.fs.s3a.secret-key", "test-secret-key");
    service = new RESTService();
    service.serviceInit(config, false);
    service.serviceStart();
  }

  @AfterAll
  static void stop() throws Exception {
    if (service != null) {
      service.serviceStop();
    }
  }

  @Test
  void testPaimon21UnsupportedResourcePaths() throws Exception {
    ResourcePaths paths =
        ResourcePaths.forCatalogProperties(Options.fromMap(Map.of("prefix", "default_catalog")));
    for (String path :
        List.of(
            paths.replaceTable("db", "t"),
            paths.partitions("db", "t"),
            paths.dropPartitions("db", "t"),
            paths.listPartitionsByFilter("db", "t"))) {
      assertError(request("POST", path, "{}"), 501);
    }
    assertError(request("GET", paths.schemas("db", "t"), ""), 501);
  }

  @Test
  void testGlobalPatternsAndPaginationUseTheSameKey() throws Exception {
    try (Catalog catalog = client("sales")) {
      for (String database : List.of("sales", "sales-eu")) {
        catalog.createDatabase(database, false);
        for (String table : List.of("t1", "t2")) {
          catalog.createTable(
              Identifier.create(database, table),
              Schema.newBuilder().column("id", DataTypes.INT()).build(),
              false);
        }
      }
    }
    List<String> names = new ArrayList<>();
    String token = null;
    for (int page = 0; page < 6; page++) {
      String path =
          "v1/sales/tables?maxResults=1&databaseNamePattern=sales%25"
              + (token == null ? "" : "&pageToken=" + RESTUtil.encodeString(token));
      HttpResponse<String> response = request("GET", path, "");
      Assertions.assertEquals(200, response.statusCode(), response.body());
      ListTablesGloballyResponse result =
          RESTApi.fromJson(response.body(), ListTablesGloballyResponse.class);
      for (Identifier identifier : result.getTables()) {
        names.add(identifier.getFullName());
      }
      token = result.getNextPageToken();
      if (token == null) {
        break;
      }
    }
    Assertions.assertNull(token);
    Assertions.assertEquals(List.of("sales-eu.t1", "sales-eu.t2", "sales.t1", "sales.t2"), names);
    assertGlobalNames("databaseNamePattern=sales", List.of("sales.t1", "sales.t2"));
    assertGlobalNames("tableNamePattern=t2", List.of("sales-eu.t2", "sales.t2"));
    assertGlobalNames("databaseNamePattern=sales&tableNamePattern=t2", List.of("sales.t2"));
    assertGlobalNames("databaseNamePattern=missing", List.of());
    assertGlobalNames(
        "databaseNamePattern=sales%25&tableNamePattern=",
        List.of("sales-eu.t1", "sales-eu.t2", "sales.t1", "sales.t2"));
    assertError(request("GET", "v1/sales/tables?tableNamePattern=bad%25middle", ""), 400);
    assertError(request("GET", "v1/sales/tables?databaseNamePattern=bad%25middle", ""), 400);
  }

  @Test
  void testGlobalPaginationDistinguishesDotsInDatabaseAndTableNames() throws Exception {
    List<Identifier> expected =
        List.of(
            Identifier.create("cursor", "eu.t"),
            Identifier.create("cursor.eu", "t"),
            Identifier.create("cursor", "eu%2Et"),
            Identifier.create("cursor", "t+report"),
            Identifier.create("cursor", "daily report"));
    try (Catalog catalog = client("sales")) {
      catalog.createDatabase("cursor", false);
      catalog.createDatabase("cursor.eu", false);
      for (Identifier identifier : expected) {
        catalog.createTable(
            identifier, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      }
      for (int size : List.of(1, 2, 3)) {
        List<Identifier> actual = new ArrayList<>();
        List<String> tokens = new ArrayList<>();
        String token = null;
        do {
          PagedList<Identifier> page =
              catalog.listTablesPagedGlobally("cursor%", null, size, token);
          actual.addAll(page.getElements());
          token = page.getNextPageToken();
          if (token != null) {
            Assertions.assertFalse(tokens.contains(token), "Cursor must advance on every page");
            tokens.add(token);
          }
        } while (token != null);
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertTrue(actual.containsAll(expected), actual.toString());
      }
    }
  }

  @Test
  void testSpaceAndLiteralPlusNamesRemainDistinct() throws Exception {
    try (Catalog catalog = client("sales")) {
      catalog.createDatabase("encoded", false);
      for (String name : List.of("daily report", "daily+report", "daily%2Breport")) {
        catalog.createTable(
            Identifier.create("encoded", name),
            Schema.newBuilder().column("id", DataTypes.INT()).option("marker", name).build(),
            false);
      }
    }
    for (String name : List.of("daily report", "daily+report", "daily%2Breport")) {
      HttpResponse<String> response =
          request("GET", "v1/sales/databases/encoded/tables/" + RESTUtil.encodeString(name), "");
      Assertions.assertEquals(200, response.statusCode(), response.body());
      GetTableResponse table = RESTApi.fromJson(response.body(), GetTableResponse.class);
      Assertions.assertEquals(name, table.getSchema().options().get("marker"));
    }
  }

  @Test
  void testTableTypeFilterIsAppliedBeforePagination() throws Exception {
    try (Catalog catalog = client("sales")) {
      catalog.createDatabase("typed", false);
      catalog.createTable(
          Identifier.create("typed", "ordinary"),
          Schema.newBuilder().column("id", DataTypes.INT()).build(),
          false);
      catalog.createTable(
          Identifier.create("typed", "explicit"),
          Schema.newBuilder().column("id", DataTypes.INT()).option("type", "table").build(),
          false);
    }
    HttpResponse<String> response =
        request("GET", "v1/sales/databases/typed/tables?tableType=format-table&maxResults=1", "");
    Assertions.assertEquals(200, response.statusCode(), response.body());
    ListTablesResponse result = RESTApi.fromJson(response.body(), ListTablesResponse.class);
    Assertions.assertTrue(result.getTables().isEmpty());
    Assertions.assertNull(result.getNextPageToken());
    response = request("GET", "v1/sales/databases/typed/tables?tableType=table", "");
    Assertions.assertEquals(200, response.statusCode(), response.body());
    Assertions.assertEquals(
        List.of("explicit", "ordinary"),
        RESTApi.fromJson(response.body(), ListTablesResponse.class).getTables());
  }

  private static void assertGlobalNames(String query, List<String> expected) throws Exception {
    HttpResponse<String> response = request("GET", "v1/sales/tables?" + query, "");
    Assertions.assertEquals(200, response.statusCode(), response.body());
    ListTablesGloballyResponse result =
        RESTApi.fromJson(response.body(), ListTablesGloballyResponse.class);
    List<String> names = new ArrayList<>();
    for (Identifier identifier : result.getTables()) {
      names.add(identifier.getFullName());
    }
    Assertions.assertEquals(expected, names);
  }

  @ParameterizedTest
  @CsvSource({
    "GET,views",
    "POST,views",
    "POST,views/rename",
    "GET,functions",
    "POST,functions",
    "GET,policies",
    "GET,permissions",
    "GET,databases/db/views",
    "POST,databases/db/views/v",
    "DELETE,databases/db/views/v",
    "GET,databases/db/view-details",
    "GET,databases/db/functions",
    "POST,databases/db/functions/f",
    "GET,databases/db/function-details",
    "GET,databases/db/table-details",
    "GET,databases/db/tables/t/consumers",
    "POST,databases/db/tables/t/consumers/reset",
    "POST,databases/db/tables/t/auth",
    "GET,databases/db/tables/t/permissions",
    "POST,databases/db/tables/t/rollback-schema",
    "GET,databases/db/tables/t/partitions"
  })
  void testKnownUnsupportedEndpointsReturnPaimon501(String method, String path) throws Exception {
    assertError(request(method, "v1/default_catalog/" + path, "{}"), 501);
    assertError(request(method, "v1/" + path, "{}"), 501);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not-a-resource",
        "databases/db/not-a-resource",
        "databases/db/tables/t/not-a-resource"
      })
  void testUnknownPathsStillReturn404(String path) throws Exception {
    assertError(request("GET", "v1/default_catalog/" + path, ""), 404);
  }

  @Test
  void testIllegalSqlPatternWithPercentReturnsJson400() throws Exception {
    HttpResponse<String> response =
        request("GET", "v1/default_catalog/databases?databaseNamePattern=bad%25middle", "");
    assertError(response, 400);
    Assertions.assertTrue(response.body().contains("bad%middle"));
  }

  @Test
  void testMalformedJsonWithPercentReturnsJson400() throws Exception {
    assertError(request("POST", "v1/default_catalog/databases", "{\"%s\":"), 400);
  }

  @Test
  void testTableStorageCredentialsAreOptInAndNormalized() throws Exception {
    Schema schema =
        new Schema(
            List.of(new DataField(0, "id", DataTypes.INT())), List.of(), List.of(), Map.of(), "");
    Identifier identifier = Identifier.create("storage_db", "events");
    try (Catalog enabled = client("storage");
        Catalog disabled = client(null)) {
      for (Catalog catalog : List.of(enabled, disabled)) {
        catalog.createDatabase(identifier.getDatabaseName(), false);
        catalog.createTable(identifier, schema, false);
      }
    }
    String route = "/databases/storage_db/tables/events/token";
    HttpResponse<String> response = request("GET", "v1/storage" + route, "");
    Assertions.assertEquals(200, response.statusCode(), response.body());
    GetTableTokenResponse token = RESTApi.fromJson(response.body(), GetTableTokenResponse.class);
    Assertions.assertEquals(
        Map.of("s3.access-key", "test-access-key", "s3.secret-key", "test-secret-key"),
        token.getToken());
    assertError(request("GET", "v1/default_catalog" + route, ""), 501);
  }

  @Test
  void testClientDoesNotMistakeUnsupportedViewsForMissingDatabase() throws Exception {
    try (Catalog client = client(null)) {
      client.createDatabase("existing_db", false);
      Assertions.assertThrows(NotImplementedException.class, () -> client.listViews("existing_db"));
      Assertions.assertThrows(
          NotImplementedException.class, () -> client.listFunctions("existing_db"));
    }
  }

  @Test
  void testUnknownWarehouseDoesNotRouteToDefaultCatalog() throws Exception {
    assertError(request("GET", "v1/config?warehouse=slaes", ""), 404);
    assertError(request("GET", "v1/slaes/databases", ""), 404);
    try (Catalog defaultCatalog = client(null)) {
      Assertions.assertNotNull(defaultCatalog.listDatabases());
    }
  }

  @Test
  void testNamedJdbcCatalogPersistsInJdbcBackend() throws Exception {
    try (Catalog client = client("sales")) {
      client.createDatabase("sales_db", false);
      Assertions.assertTrue(client.listDatabases().contains("sales_db"));
    }
    Options nativeOptions =
        Options.fromMap(
            Map.of(
                "metastore",
                "jdbc",
                "uri",
                "jdbc:sqlite:" + directory.resolve("catalog.db"),
                "warehouse",
                directory.resolve("sales").toUri().toString()));
    try (Catalog nativeCatalog =
        CatalogFactory.createCatalog(CatalogContext.create(nativeOptions))) {
      Assertions.assertTrue(nativeCatalog.listDatabases().contains("sales_db"));
    }
    try (Catalog defaultCatalog = client(null)) {
      Assertions.assertFalse(defaultCatalog.listDatabases().contains("sales_db"));
    }
  }

  private static Catalog client(String warehouse) {
    Map<String, String> options =
        new HashMap<>(
            Map.of(
                "metastore",
                "rest",
                "uri",
                base.toString(),
                "token.provider",
                "bear",
                "token",
                "protocol-test"));
    if (warehouse != null) {
      options.put("warehouse", warehouse);
    }
    return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)));
  }

  private static HttpResponse<String> request(String method, String path, String body)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(base.resolve(path.startsWith("/") ? path.substring(1) : path))
            .timeout(Duration.ofSeconds(20))
            .header("Authorization", "Bearer protocol-test")
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static void assertError(HttpResponse<String> response, int expected) throws Exception {
    Assertions.assertEquals(expected, response.statusCode(), response.body());
    Assertions.assertTrue(
        response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
    Assertions.assertEquals(
        expected, RESTApi.fromJson(response.body(), ErrorResponse.class).getCode());
  }
}
