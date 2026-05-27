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
package org.apache.gravitino.idp.integration.test;

import static org.apache.gravitino.integration.test.util.BaseIT.setEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.ChangePasswordRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * End-to-end tests for built-in IdP REST APIs on an embedded Gravitino server.
 *
 * <p>Extends {@link BaseIT} for shared integration-test utilities; starts {@link GravitinoServer}
 * in-process (same approach as {@link org.apache.gravitino.idp.web.rest.TestIdpRestExtension})
 * because the IdP plugin must be on the server classpath.
 */
public class IdpRestApiIT extends BaseIT {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";
  private static final String BASIC_AUTHENTICATOR_CLASS =
      "org.apache.gravitino.idp.auth.BasicAuthenticator";
  private static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String GROUP1 = "group1";
  private static final String USER_PASSWORD = "Passw0rd-For-User1";
  private static final String UPDATED_PASSWORD = "Passw0rd-For-User2";

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private GravitinoServer gravitinoServer;
  private ServerConfig gravitinoServerConfig;
  private IdpUserGroupManager idpUserGroupManager;
  private Path h2Path;
  private String apiBase;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    setEnv(INITIAL_ADMIN_PASSWORD_ENV, ADMIN_PASSWORD);

    int httpPort = RESTUtils.findAvailablePort(5000, 6000);
    h2Path = Files.createTempDirectory("gravitino_idp_rest_api_it_");

    gravitinoServerConfig = newServerConfig(httpPort);
    gravitinoServer = new GravitinoServer(gravitinoServerConfig, GravitinoEnv.getInstance());
    gravitinoServer.initialize();
    gravitinoServer.start();

    serverUri = String.format("http://localhost:%d", httpPort);
    apiBase = serverUri + "/api";
    idpUserGroupManager =
        IdpUserGroupManager.getInstance(
            gravitinoServerConfig, GravitinoEnv.getInstance().idGenerator());
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    if (gravitinoServer != null) {
      gravitinoServer.stop();
      gravitinoServer = null;
    }
    if (idpUserGroupManager != null) {
      idpUserGroupManager.close();
      idpUserGroupManager = null;
    }
    if (h2Path != null) {
      FileUtils.deleteDirectory(h2Path.toFile());
    }
  }

  @Test
  public void testIdpRestApis() throws Exception {
    assertEquals(200, get("/version", ADMIN, ADMIN_PASSWORD).statusCode());
    assertEquals(401, get("/idp/users/" + USER1, null, null).statusCode());

    postUser(USER2, USER_PASSWORD);
    assertEquals(403, get("/idp/users/" + USER2, USER2, USER_PASSWORD).statusCode());
    deleteUser(USER2);

    postUser(USER1, USER_PASSWORD);
    IdpUserResponse user = getUser(USER1);
    assertEquals(USER1, user.getUser().name());
    assertTrue(user.getUser().groups().isEmpty());

    changePassword(USER1, UPDATED_PASSWORD);
    assertEquals(USER1, getUser(USER1).getUser().name());

    assertTrue(deleteUser(USER1));
    assertEquals(
        ErrorConstants.NOT_FOUND_CODE,
        errorCode(get("/idp/users/" + USER1, ADMIN, ADMIN_PASSWORD)));

    postUser(USER1, USER_PASSWORD);
    postUser(USER2, USER_PASSWORD);

    IdpGroupResponse group = postGroup(GROUP1);
    assertEquals(GROUP1, group.getGroup().name());
    assertTrue(group.getGroup().users().isEmpty());

    group =
        putMembership(GROUP1, new GroupMembershipChangeRequest(new String[] {USER1, USER2}, null));
    assertEquals(List.of(USER1, USER2), group.getGroup().users());

    group =
        putMembership(GROUP1, new GroupMembershipChangeRequest(null, new String[] {USER1, USER2}));
    assertTrue(group.getGroup().users().isEmpty());

    assertTrue(deleteGroup(GROUP1, false));

    deleteUser(USER1);
    deleteUser(USER2);
  }

  private ServerConfig newServerConfig(int httpPort) {
    ImmutableMap<String, String> configMap =
        ImmutableMap.<String, String>builder()
            .put(
                GravitinoServer.WEBSERVER_CONF_PREFIX
                    + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
                String.valueOf(httpPort))
            .put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE)
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
                String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path))
            .put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER.getKey(), "org.h2.Driver")
            .put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER.getKey(), "root")
            .put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD.getKey(), "123456")
            .put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS.getKey(), "100")
            .put(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS.getKey(), "1000")
            .put(Configs.STORE_DELETE_AFTER_TIME.getKey(), String.valueOf(20 * 60 * 1000L))
            .put(Configs.CACHE_ENABLED.getKey(), "false")
            .put(Configs.ENABLE_AUTHORIZATION.getKey(), "false")
            .put(Configs.AUTHENTICATORS.getKey(), BASIC_AUTHENTICATOR_CLASS)
            .put(Configs.SERVICE_ADMINS.getKey(), ADMIN)
            .put(Configs.REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_EXTENSION_PACKAGE)
            .build();

    ServerConfig config = new ServerConfig();
    config.loadFromMap(configMap, key -> true);
    ServerConfig spyConfig = Mockito.spy(config);
    Mockito.when(
            spyConfig.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
    return spyConfig;
  }

  private HttpResponse<String> get(String path, String username, String password) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(URI.create(apiBase + path)).header("Accept", ACCEPT).GET();
    if (username != null) {
      builder.header("Authorization", basicAuth(username, password));
    }
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private void postUser(String username, String password) throws Exception {
    HttpResponse<String> response = post("/idp/users", new AddUserRequest(username, password));
    assertEquals(200, response.statusCode(), response.body());
    JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class).validate();
  }

  private IdpUserResponse getUser(String username) throws Exception {
    HttpResponse<String> response = get("/idp/users/" + username, ADMIN, ADMIN_PASSWORD);
    assertEquals(200, response.statusCode(), response.body());
    IdpUserResponse userResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class);
    userResponse.validate();
    return userResponse;
  }

  private void changePassword(String username, String password) throws Exception {
    HttpResponse<String> response =
        put("/idp/users/" + username, new ChangePasswordRequest(password));
    assertEquals(200, response.statusCode(), response.body());
  }

  private boolean deleteUser(String username) throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            authorized(ADMIN, ADMIN_PASSWORD)
                .uri(URI.create(apiBase + "/idp/users/" + username))
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private IdpGroupResponse postGroup(String groupName) throws Exception {
    HttpResponse<String> response = post("/idp/groups", new AddGroupRequest(groupName));
    assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private IdpGroupResponse putMembership(String groupName, GroupMembershipChangeRequest request)
      throws Exception {
    HttpResponse<String> response = put("/idp/groups/" + groupName + "/users", request);
    assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private boolean deleteGroup(String groupName, boolean force) throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            authorized(ADMIN, ADMIN_PASSWORD)
                .uri(URI.create(apiBase + "/idp/groups/" + groupName + "?force=" + force))
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private HttpResponse<String> post(String path, Object body) throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + path))
            .header("Content-Type", ACCEPT)
            .POST(jsonBody(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> put(String path, Object body) throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + path))
            .header("Content-Type", ACCEPT)
            .PUT(jsonBody(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpRequest.BodyPublisher jsonBody(Object body) throws Exception {
    return HttpRequest.BodyPublishers.ofString(JsonUtils.objectMapper().writeValueAsString(body));
  }

  private static HttpRequest.Builder authorized(String username, String password) {
    return HttpRequest.newBuilder()
        .header("Accept", ACCEPT)
        .header("Authorization", basicAuth(username, password));
  }

  private static String basicAuth(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static int errorCode(HttpResponse<String> response) throws Exception {
    return JsonUtils.objectMapper().readTree(response.body()).get("code").asInt();
  }
}
