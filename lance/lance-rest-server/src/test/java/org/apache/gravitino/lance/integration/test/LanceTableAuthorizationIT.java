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
package org.apache.gravitino.lance.integration.test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.RegisterTableRequest;

/** Verifies that read-only Lance REST table operations are authorized and filtered. */
public class LanceTableAuthorizationIT extends BaseIT {

  private static final String ADMIN = "lance_table_authz_admin";
  private static final String READER = "lance_table_authz_reader";
  private static final String PROBER = "lance_table_authz_prober";
  private static final String CATALOG = "lance_table_authz_catalog";
  private static final String SCHEMA = "lance_table_authz_schema";

  // The hidden table sorts first so that a listing filtered after pagination rather than before it
  // would return an empty first page instead of the visible table.
  private static final String HIDDEN_TABLE = "a_hidden_table";
  private static final String VISIBLE_TABLE = "b_visible_table";
  private static final String MISSING_TABLE = "c_missing_table";
  private static final String DELIMITER = ".";

  @TempDir private static Path tempDir;

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    ignoreLanceAuxRestService = false;
    customConfigs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    customConfigs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    customConfigs.put("SimpleAuthUserName", ADMIN);
    super.startIntegrationTest();

    String metalakeName = getLanceRESTServerMetalakeName();
    client.createMetalake(metalakeName, "Lance table authorization tests", null);
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    metalake.addUser(READER);
    metalake.addUser(PROBER);

    createNamespace(CATALOG);
    createNamespace(id(CATALOG, SCHEMA));
    registerTable(VISIBLE_TABLE);
    registerTable(HIDDEN_TABLE);

    SecurableObject catalogScope = SecurableObjects.ofCatalog(CATALOG, new ArrayList<>());
    SecurableObject schemaScope =
        SecurableObjects.ofSchema(catalogScope, SCHEMA, new ArrayList<>());

    // The reader may select exactly one table, so the other one must stay invisible even though
    // the reader can reach the schema holding it.
    grant(
        metalake,
        "lance_table_authz_reader_role",
        READER,
        List.of(
            SecurableObjects.ofCatalog(
                CATALOG, new ArrayList<>(List.of(Privileges.UseCatalog.allow()))),
            SecurableObjects.ofSchema(
                catalogScope, SCHEMA, new ArrayList<>(List.of(Privileges.UseSchema.allow()))),
            SecurableObjects.ofTable(
                schemaScope,
                VISIBLE_TABLE,
                new ArrayList<>(List.of(Privileges.SelectTable.allow())))));

    grant(
        metalake,
        "lance_table_authz_prober_role",
        PROBER,
        List.of(
            SecurableObjects.ofCatalog(
                CATALOG, new ArrayList<>(List.of(Privileges.UseCatalog.allow()))),
            SecurableObjects.ofSchema(
                catalogScope,
                SCHEMA,
                new ArrayList<>(
                    List.of(Privileges.UseSchema.allow(), Privileges.CreateTable.allow())))));
  }

  @AfterAll
  public void clean() throws Exception {
    try {
      if (client != null) {
        client.dropMetalake(getLanceRESTServerMetalakeName(), true);
      }
    } finally {
      super.stopIntegrationTest();
    }
  }

  @Test
  public void testTableReadRequiresTablePrivileges() throws Exception {
    assertStatus(200, table(READER, VISIBLE_TABLE, "describe"));
    assertStatus(200, table(READER, VISIBLE_TABLE, "exists"));

    HttpResponse<String> denied = table(READER, HIDDEN_TABLE, "describe");
    assertStatus(403, denied);
    // A denied describe must not leak the table location or any other stored property.
    Assertions.assertFalse(denied.body().contains(location(HIDDEN_TABLE)), denied.body());
    assertStatus(403, table(READER, HIDDEN_TABLE, "exists"));

    assertStatus(200, table(ADMIN, HIDDEN_TABLE, "describe"));
  }

  @Test
  public void testCreateTablePrivilegeProbesButDoesNotRead() throws Exception {
    // Clients probe before creating, so CREATE_TABLE authorizes the probe but not a read.
    assertStatus(200, table(PROBER, VISIBLE_TABLE, "exists"));
    assertStatus(403, table(PROBER, VISIBLE_TABLE, "describe"));
  }

  @Test
  public void testInaccessibleAndMissingTablesAreIndistinguishable() throws Exception {
    // Both are forbidden for the reader, so the endpoint cannot be used to probe for existence.
    assertStatus(403, table(READER, HIDDEN_TABLE, "describe"));
    assertStatus(403, table(READER, MISSING_TABLE, "describe"));

    // The privileged caller does see the difference.
    assertStatus(200, table(ADMIN, HIDDEN_TABLE, "describe"));
    assertStatus(404, table(ADMIN, MISSING_TABLE, "describe"));
  }

  @Test
  public void testTablesAreFilteredBeforePagination() throws Exception {
    Assertions.assertEquals(List.of(VISIBLE_TABLE), listTables(READER, 1));
    Assertions.assertEquals(List.of(VISIBLE_TABLE), listTables(READER, 10));
    Assertions.assertEquals(List.of(HIDDEN_TABLE, VISIBLE_TABLE), listTables(ADMIN, 10));
  }

  private List<String> listTables(String user, int limit) throws Exception {
    HttpRequest request =
        request(user, "/v1/namespace/" + id(CATALOG, SCHEMA) + "/table/list", "&limit=" + limit)
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    assertStatus(200, response);
    List<String> tables =
        new ArrayList<>(
            ObjectMapperProvider.objectMapper()
                .readValue(response.body(), ListTablesResponse.class)
                .getTables());
    tables.sort(String::compareTo);
    return tables;
  }

  private void grant(
      GravitinoMetalake metalake, String role, String user, List<SecurableObject> objects) {
    metalake.createRole(role, new HashMap<>(), objects);
    metalake.grantRolesToUser(List.of(role), user);
  }

  private String id(String... levels) {
    return String.join(DELIMITER, levels);
  }

  private String location(String tableName) {
    return tempDir.resolve(tableName).toString() + "/";
  }

  private void createNamespace(String namespaceId) throws Exception {
    CreateNamespaceRequest body = new CreateNamespaceRequest();
    for (String level : namespaceId.split("\\" + DELIMITER)) {
      body.addIdItem(level);
    }
    assertStatus(200, send(ADMIN, "/v1/namespace/" + namespaceId + "/create", body));
  }

  private void registerTable(String tableName) throws Exception {
    RegisterTableRequest body = new RegisterTableRequest();
    body.setId(List.of(CATALOG, SCHEMA, tableName));
    body.setLocation(location(tableName));
    assertStatus(
        200, send(ADMIN, "/v1/table/" + id(CATALOG, SCHEMA, tableName) + "/register", body));
  }

  private HttpResponse<String> table(String user, String tableName, String operation)
      throws Exception {
    HttpRequest request =
        request(user, "/v1/table/" + id(CATALOG, SCHEMA, tableName) + "/" + operation, "")
            .POST(HttpRequest.BodyPublishers.ofString("{}"))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> send(String user, String path, Object body) throws Exception {
    HttpRequest request =
        request(user, path, "")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    ObjectMapperProvider.objectMapper().writeValueAsString(body)))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder request(String user, String path, String extraQuery) {
    String credentials =
        Base64.getEncoder().encodeToString((user + ":dummy").getBytes(StandardCharsets.UTF_8));
    return HttpRequest.newBuilder()
        .uri(
            URI.create(
                String.format(
                    "http://localhost:%d/lance%s?delimiter=%s%s",
                    getLanceRESTServerPort(), path, DELIMITER, extraQuery)))
        .header(
            AuthConstants.HTTP_HEADER_AUTHORIZATION,
            AuthConstants.AUTHORIZATION_BASIC_HEADER + credentials)
        .header(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, "ALL")
        .header("Content-Type", "application/json");
  }

  private void assertStatus(int expected, HttpResponse<String> response) {
    Assertions.assertEquals(expected, response.statusCode(), "Unexpected body: " + response.body());
  }
}
