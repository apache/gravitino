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
import org.lance.namespace.model.ListNamespacesResponse;

/** Verifies namespace authorization and list filtering through auxiliary-mode Lance REST. */
public class LanceNamespaceAuthorizationIT extends BaseIT {

  private static final String ADMIN = "lance_authz_admin";
  private static final String USER = "lance_authz_user";
  private static final String VISIBLE_CATALOG = "lance_authz_visible_catalog";
  private static final String HIDDEN_CATALOG = "lance_authz_hidden_catalog";
  private static final String VISIBLE_SCHEMA = "lance_authz_visible_schema";
  private static final String HIDDEN_SCHEMA = "lance_authz_hidden_schema";
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
