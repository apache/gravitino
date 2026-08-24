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

import com.google.common.collect.ImmutableList;
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
import org.apache.gravitino.authorization.Privilege;
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

/**
 * Verifies namespace read authorization of the auxiliary-mode Lance REST service.
 *
 * <p>A caller only sees the catalogs and schemas it is allowed to access, and a request on a
 * namespace it may not read is rejected before the operation runs.
 */
public class LanceNamespaceAuthorizationIT extends BaseIT {

  private static final String ADMIN_USER = "lance_authz_admin";
  private static final String NORMAL_USER = "lance_authz_user";
  private static final String DELIMITER = ".";

  private static final String VISIBLE_CATALOG = "lance_authz_visible_catalog";
  private static final String HIDDEN_CATALOG = "lance_authz_hidden_catalog";
  private static final String VISIBLE_SCHEMA = "lance_authz_visible_schema";
  private static final String HIDDEN_SCHEMA = "lance_authz_hidden_schema";

  private static final String USE_CATALOG_ROLE = "lance_authz_use_catalog_role";
  private static final String USE_SCHEMA_ROLE = "lance_authz_use_schema_role";

  private final HttpClient httpClient = HttpClient.newHttpClient();

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreLanceAuxRestService = false;
    customConfigs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN_USER);
    customConfigs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    customConfigs.put("SimpleAuthUserName", ADMIN_USER);
    super.startIntegrationTest();

    String metalakeName = getLanceRESTServerMetalakeName();
    client.createMetalake(metalakeName, "metalake for lance authorization tests", null);
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    metalake.addUser(NORMAL_USER);

    // The fixtures are created through Lance REST as the admin, so they are lakehouse catalogs the
    // Lance service accepts.
    createNamespace(ADMIN_USER, VISIBLE_CATALOG);
    createNamespace(ADMIN_USER, HIDDEN_CATALOG);
    createNamespace(ADMIN_USER, namespaceId(VISIBLE_CATALOG, VISIBLE_SCHEMA));
    createNamespace(ADMIN_USER, namespaceId(VISIBLE_CATALOG, HIDDEN_SCHEMA));

    grantRole(
        metalake,
        USE_CATALOG_ROLE,
        SecurableObjects.ofCatalog(VISIBLE_CATALOG, privileges(Privileges.UseCatalog.allow())));
    grantRole(
        metalake,
        USE_SCHEMA_ROLE,
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog(VISIBLE_CATALOG, new ArrayList<>()),
            VISIBLE_SCHEMA,
            privileges(Privileges.UseSchema.allow())));
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
  public void testDescribeNamespaceIsRejectedWithoutPrivilege() throws Exception {
    HttpResponse<String> response = describeNamespace(NORMAL_USER, HIDDEN_CATALOG);

    Assertions.assertEquals(403, response.statusCode(), "Unexpected body: " + response.body());
  }

  @Test
  public void testDescribeNamespaceIsAllowedWithUseCatalog() throws Exception {
    HttpResponse<String> response = describeNamespace(NORMAL_USER, VISIBLE_CATALOG);

    Assertions.assertEquals(200, response.statusCode(), "Unexpected body: " + response.body());
  }

  @Test
  public void testNamespaceExistsIsRejectedWithoutPrivilege() throws Exception {
    HttpResponse<String> response =
        post(NORMAL_USER, namespaceId(VISIBLE_CATALOG, HIDDEN_SCHEMA), "exists");

    Assertions.assertEquals(403, response.statusCode(), "Unexpected body: " + response.body());
  }

  @Test
  public void testNamespaceExistsIsAllowedWithUseSchema() throws Exception {
    HttpResponse<String> response =
        post(NORMAL_USER, namespaceId(VISIBLE_CATALOG, VISIBLE_SCHEMA), "exists");

    Assertions.assertEquals(200, response.statusCode(), "Unexpected body: " + response.body());
  }

  @Test
  public void testListNamespacesHidesUnauthorizedCatalogs() throws Exception {
    List<String> namespaces = listNamespaces(NORMAL_USER, "");

    Assertions.assertTrue(
        namespaces.contains(VISIBLE_CATALOG), "The authorized catalog must be listed");
    Assertions.assertFalse(
        namespaces.contains(HIDDEN_CATALOG), "An unauthorized catalog must not be listed");
  }

  @Test
  public void testListNamespacesHidesUnauthorizedSchemas() throws Exception {
    List<String> namespaces = listNamespaces(NORMAL_USER, VISIBLE_CATALOG);

    Assertions.assertTrue(
        namespaces.contains(VISIBLE_SCHEMA), "The authorized schema must be listed");
    Assertions.assertFalse(
        namespaces.contains(HIDDEN_SCHEMA), "An unauthorized schema must not be listed");
  }

  @Test
  public void testAdminStillSeesEveryNamespace() throws Exception {
    List<String> catalogs = listNamespaces(ADMIN_USER, "");

    Assertions.assertTrue(catalogs.contains(VISIBLE_CATALOG));
    Assertions.assertTrue(catalogs.contains(HIDDEN_CATALOG));
    Assertions.assertEquals(200, describeNamespace(ADMIN_USER, HIDDEN_CATALOG).statusCode());
  }

  private void grantRole(GravitinoMetalake metalake, String roleName, SecurableObject object) {
    metalake.createRole(roleName, new HashMap<>(), ImmutableList.of(object));
    metalake.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  private List<Privilege> privileges(Privilege... privileges) {
    return new ArrayList<>(List.of(privileges));
  }

  private String namespaceId(String... levels) {
    return String.join(DELIMITER, levels);
  }

  private List<String> listNamespaces(String user, String namespaceId) throws Exception {
    String path =
        namespaceId.isEmpty() ? "/v1/namespace/list" : "/v1/namespace/" + namespaceId + "/list";
    HttpResponse<String> response = send(request(user, path).GET().build());
    Assertions.assertEquals(200, response.statusCode(), "Unexpected body: " + response.body());
    ListNamespacesResponse listResponse =
        ObjectMapperProvider.objectMapper()
            .readValue(response.body(), ListNamespacesResponse.class);
    return new ArrayList<>(listResponse.getNamespaces());
  }

  private HttpResponse<String> describeNamespace(String user, String namespaceId) throws Exception {
    return post(user, namespaceId, "describe");
  }

  private void createNamespace(String user, String namespaceId) throws Exception {
    CreateNamespaceRequest createRequest = new CreateNamespaceRequest();
    for (String level : namespaceId.split("\\" + DELIMITER)) {
      createRequest.addIdItem(level);
    }
    HttpResponse<String> response =
        send(
            request(user, "/v1/namespace/" + namespaceId + "/create")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        ObjectMapperProvider.objectMapper().writeValueAsString(createRequest)))
                .build());
    Assertions.assertEquals(200, response.statusCode(), "Unexpected body: " + response.body());
  }

  private HttpResponse<String> post(String user, String namespaceId, String operation)
      throws Exception {
    return send(
        request(user, "/v1/namespace/" + namespaceId + "/" + operation)
            .POST(HttpRequest.BodyPublishers.ofString("{}"))
            .build());
  }

  private HttpRequest.Builder request(String user, String path) {
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((user + ":dummy").getBytes(StandardCharsets.UTF_8));
    String separator = path.contains("?") ? "&" : "?";
    return HttpRequest.newBuilder()
        .uri(URI.create(lanceRestServiceUrl() + path + separator + "delimiter=" + DELIMITER))
        .header(AuthConstants.HTTP_HEADER_AUTHORIZATION, authHeader)
        .header(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, "ALL")
        .header("Content-Type", "application/json");
  }

  private HttpResponse<String> send(HttpRequest request) throws Exception {
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private String lanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
