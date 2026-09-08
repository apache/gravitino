/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.ListNamespacesResponse;

/** Verifies namespace authorization and list filtering through auxiliary-mode Lance REST. */
public class LanceNamespaceAuthorizationIT extends BaseIT {

  private static final String ADMIN = "lance_authz_admin";
  private static final String USER = "lance_authz_user";
  private static final String WRITER = "lance_authz_writer";
  private static final String VISIBLE_CATALOG = "lance_authz_visible_catalog";
  private static final String HIDDEN_CATALOG = "lance_authz_hidden_catalog";
  private static final String VISIBLE_SCHEMA = "lance_authz_visible_schema";
  private static final String HIDDEN_SCHEMA = "lance_authz_hidden_schema";
  private static final String WRITER_CATALOG = "lance_authz_writer_catalog";
  private static final String WRITER_SCHEMA = "lance_authz_writer_schema";
  private static final String MISSING_CATALOG = "lance_authz_missing_catalog";
  private static final String MARKER_PROPERTY = "lance-authz-marker";
  private static final String DELIMITER = ".";

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
    client.createMetalake(metalakeName, "Lance authorization tests", null);
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    metalake.addUser(USER);
    metalake.addUser(WRITER);
    createNamespace(ADMIN, VISIBLE_CATALOG);
    createNamespace(ADMIN, HIDDEN_CATALOG);
    createNamespace(ADMIN, id(VISIBLE_CATALOG, VISIBLE_SCHEMA));
    createNamespace(ADMIN, id(VISIBLE_CATALOG, HIDDEN_SCHEMA));

    grant(
        metalake,
        "lance_authz_catalog_role",
        SecurableObjects.ofCatalog(
            VISIBLE_CATALOG, new ArrayList<>(List.of(Privileges.UseCatalog.allow()))));
    grant(
        metalake,
        "lance_authz_schema_role",
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog(VISIBLE_CATALOG, new ArrayList<>()),
            VISIBLE_SCHEMA,
            new ArrayList<>(List.of(Privileges.UseSchema.allow()))));

    // The writer is a separate user so that granting create privileges cannot change what the
    // read-only user above is allowed to see.
    metalake.createRole(
        "lance_authz_writer_role",
        new HashMap<>(),
        List.of(
            SecurableObjects.ofMetalake(
                metalakeName, new ArrayList<>(List.of(Privileges.CreateCatalog.allow()))),
            SecurableObjects.ofCatalog(
                VISIBLE_CATALOG,
                new ArrayList<>(
                    List.of(Privileges.UseCatalog.allow(), Privileges.CreateSchema.allow())))));
    metalake.grantRolesToUser(List.of("lance_authz_writer_role"), WRITER);
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
  public void testNamespaceAuthorization() throws Exception {
    assertStatus(403, post(USER, HIDDEN_CATALOG, "describe"));
    assertStatus(200, post(USER, VISIBLE_CATALOG, "describe"));
    assertStatus(403, post(USER, id(VISIBLE_CATALOG, HIDDEN_SCHEMA), "exists"));
    assertStatus(200, post(USER, id(VISIBLE_CATALOG, VISIBLE_SCHEMA), "exists"));

    List<String> catalogs = list(USER, "");
    Assertions.assertTrue(catalogs.contains(VISIBLE_CATALOG));
    Assertions.assertFalse(catalogs.contains(HIDDEN_CATALOG));
    List<String> schemas = list(USER, VISIBLE_CATALOG);
    Assertions.assertTrue(schemas.contains(VISIBLE_SCHEMA));
    Assertions.assertFalse(schemas.contains(HIDDEN_SCHEMA));

    Assertions.assertTrue(list(ADMIN, "").containsAll(List.of(VISIBLE_CATALOG, HIDDEN_CATALOG)));
    assertStatus(200, post(ADMIN, HIDDEN_CATALOG, "describe"));
  }

  @Test
  public void testCreateNamespaceRequiresCreatePrivilege() throws Exception {
    // The read-only user holds no create privilege at either level.
    assertStatus(403, create(USER, "lance_authz_denied_catalog", null, Map.of()));
    assertStatus(
        403, create(USER, id(VISIBLE_CATALOG, "lance_authz_denied_schema"), null, Map.of()));

    assertStatus(200, create(WRITER, WRITER_CATALOG, null, Map.of()));
    assertStatus(200, create(WRITER, id(VISIBLE_CATALOG, WRITER_SCHEMA), null, Map.of()));

    // Creating assigns ownership, so the creator can drop what it created.
    assertStatus(200, drop(WRITER, id(VISIBLE_CATALOG, WRITER_SCHEMA), null, null));
    assertStatus(200, drop(WRITER, WRITER_CATALOG, null, "cascade"));
  }

  @Test
  public void testCreatePrivilegeCannotOverwriteOrDropAnotherOwnersNamespace() throws Exception {
    Map<String, String> marker = Map.of(MARKER_PROPERTY, "overwritten");
    assertStatus(403, create(WRITER, VISIBLE_CATALOG, "overwrite", marker));
    assertStatus(403, create(WRITER, id(VISIBLE_CATALOG, VISIBLE_SCHEMA), "overwrite", marker));

    // A denied overwrite must not have reached the metadata store.
    Assertions.assertFalse(properties(ADMIN, VISIBLE_CATALOG).containsKey(MARKER_PROPERTY));
    Assertions.assertFalse(
        properties(ADMIN, id(VISIBLE_CATALOG, VISIBLE_SCHEMA)).containsKey(MARKER_PROPERTY));

    // exist_ok is a create, not a modification, so the create privilege is enough for it.
    assertStatus(200, create(WRITER, VISIBLE_CATALOG, "exist_ok", Map.of()));
    Assertions.assertFalse(properties(ADMIN, VISIBLE_CATALOG).containsKey(MARKER_PROPERTY));

    assertStatus(403, drop(WRITER, id(VISIBLE_CATALOG, HIDDEN_SCHEMA), null, null));
    assertStatus(200, post(ADMIN, id(VISIBLE_CATALOG, HIDDEN_SCHEMA), "exists"));
  }

  @Test
  public void testDropConcealsNamespacesTheCallerMayNotSee() throws Exception {
    // A namespace the caller cannot drop is reported as forbidden whether or not it exists, so a
    // caller cannot probe for existence through the drop endpoint.
    assertStatus(403, drop(WRITER, MISSING_CATALOG, "skip", null));
    assertStatus(403, drop(USER, HIDDEN_CATALOG, "skip", null));
  }

  private void grant(GravitinoMetalake metalake, String role, SecurableObject object) {
    metalake.createRole(role, new HashMap<>(), List.of(object));
    metalake.grantRolesToUser(List.of(role), USER);
  }

  private String id(String... levels) {
    return String.join(DELIMITER, levels);
  }

  private List<String> list(String user, String namespaceId) throws Exception {
    String path =
        namespaceId.isEmpty() ? "/v1/namespace/list" : "/v1/namespace/" + namespaceId + "/list";
    HttpResponse<String> response =
        httpClient.send(request(user, path).GET().build(), HttpResponse.BodyHandlers.ofString());
    assertStatus(200, response);
    return new ArrayList<>(
        ObjectMapperProvider.objectMapper()
            .readValue(response.body(), ListNamespacesResponse.class)
            .getNamespaces());
  }

  private void createNamespace(String user, String namespaceId) throws Exception {
    CreateNamespaceRequest body = new CreateNamespaceRequest();
    for (String level : namespaceId.split("\\" + DELIMITER)) {
      body.addIdItem(level);
    }
    HttpRequest request =
        request(user, "/v1/namespace/" + namespaceId + "/create")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    ObjectMapperProvider.objectMapper().writeValueAsString(body)))
            .build();
    assertStatus(200, httpClient.send(request, HttpResponse.BodyHandlers.ofString()));
  }

  private HttpResponse<String> create(
      String user, String namespaceId, String mode, Map<String, String> properties)
      throws Exception {
    CreateNamespaceRequest body = new CreateNamespaceRequest();
    for (String level : namespaceId.split("\\" + DELIMITER)) {
      body.addIdItem(level);
    }
    body.setMode(mode);
    body.setProperties(new HashMap<>(properties));
    return send(user, "/v1/namespace/" + namespaceId + "/create", body);
  }

  private HttpResponse<String> drop(String user, String namespaceId, String mode, String behavior)
      throws Exception {
    DropNamespaceRequest body = new DropNamespaceRequest();
    body.setMode(mode);
    body.setBehavior(behavior);
    return send(user, "/v1/namespace/" + namespaceId + "/drop", body);
  }

  private Map<String, String> properties(String user, String namespaceId) throws Exception {
    HttpResponse<String> response = post(user, namespaceId, "describe");
    assertStatus(200, response);
    Map<String, String> properties =
        ObjectMapperProvider.objectMapper()
            .readValue(response.body(), DescribeNamespaceResponse.class)
            .getProperties();
    return properties == null ? Map.of() : properties;
  }

  private HttpResponse<String> send(String user, String path, Object body) throws Exception {
    HttpRequest request =
        request(user, path)
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    ObjectMapperProvider.objectMapper().writeValueAsString(body)))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(String user, String namespaceId, String operation)
      throws Exception {
    HttpRequest request =
        request(user, "/v1/namespace/" + namespaceId + "/" + operation)
            .POST(HttpRequest.BodyPublishers.ofString("{}"))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder request(String user, String path) {
    String credentials =
        Base64.getEncoder().encodeToString((user + ":dummy").getBytes(StandardCharsets.UTF_8));
    return HttpRequest.newBuilder()
        .uri(
            URI.create(
                String.format(
                    "http://localhost:%d/lance%s?delimiter=%s",
                    getLanceRESTServerPort(), path, DELIMITER)))
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
