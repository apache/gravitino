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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.ChangePasswordRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.idp.web.rest.feature.IdpRESTFeature;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for built-in IdP REST APIs.
 *
 * <p>Run with {@code -PjdbcBackend=h2}, {@code mysql}, or {@code postgresql} to exercise the same
 * API surface against each relational backend.
 */
public class IdpRESTApiIT extends BaseIT {

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String GROUP1 = "group1";
  private static final String USER_PASSWORD = "Passw0rd-For-User1";
  private static final String UPDATED_PASSWORD = "Passw0rd-For-User2";
  private static final String MISSING_USER = "missing-user";
  private static final String MISSING_GROUP = "missing-group";

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static String apiBase;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    setEnv(IdpRESTFeature.INITIAL_ADMIN_PASSWORD_ENV, ADMIN_PASSWORD);
    ensureDeployInitialAdminPasswordInDistributionEnv();
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(Configs.STORE_DELETE_AFTER_TIME.getKey(), String.valueOf(20 * 60 * 1000L));
    configs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    configs.put(
        Configs.AUTHENTICATORS.getKey(),
        AuthenticatorType.BASIC.name().toLowerCase()
            + ","
            + AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), "service1");
    configs.put(OAuthConfig.DEFAULT_SIGN_KEY.getKey(), oauthPublicSignKey());
    configs.put(OAuthConfig.DEFAULT_SERVER_URI.getKey(), "test");
    configs.put(OAuthConfig.DEFAULT_TOKEN_PATH.getKey(), "test");
    configs.put(
        Configs.REST_API_EXTENSION_PACKAGES.getKey(), IdpRESTFeature.IDP_REST_EXTENSION_PACKAGE);
    registerCustomConfigs(configs);
    super.startIntegrationTest();
    apiBase = serverUri + "/api";
  }

  private static void ensureDeployInitialAdminPasswordInDistributionEnv() throws IOException {
    if (!ITUtils.DEPLOY_TEST_MODE.equals(System.getProperty(ITUtils.TEST_MODE))) {
      return;
    }
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (StringUtils.isBlank(gravitinoHome)) {
      return;
    }
    Path envFile = Paths.get(gravitinoHome, "conf", "gravitino-env.sh");
    String exportLine =
        "export GRAVITINO_INITIAL_ADMIN_PASSWORD='" + ADMIN_PASSWORD.replace("'", "'\\''") + "'";
    String existing =
        Files.exists(envFile) ? Files.readString(envFile, StandardCharsets.UTF_8) : "";
    if (existing.contains("GRAVITINO_INITIAL_ADMIN_PASSWORD")) {
      return;
    }
    String separator = existing.endsWith(System.lineSeparator()) ? "" : System.lineSeparator();
    Files.writeString(
        envFile,
        existing + separator + exportLine + System.lineSeparator(),
        StandardCharsets.UTF_8);
  }

  @Test
  void testIdpAuthorization() throws Exception {
    Assertions.assertEquals(200, get("/version", ADMIN, ADMIN_PASSWORD).statusCode());
    // No Authorization: OAuth rejects the request before the IdP filter runs.
    Assertions.assertEquals(401, get("/idp/users/" + USER1, null, null).statusCode());

    postUser(USER2, USER_PASSWORD);
    assertError(
        403, get("/idp/users/" + USER2, USER2, USER_PASSWORD), ErrorConstants.FORBIDDEN_CODE);
    Assertions.assertTrue(deleteUser(USER2));
  }

  @Test
  void testManageUsers() throws Exception {
    postUser(USER1, USER_PASSWORD);
    IdpUserResponse user = getUser(USER1);
    Assertions.assertEquals(USER1, user.getUser().name());
    Assertions.assertTrue(user.getUser().groups().isEmpty());

    assertError(
        400,
        post("/idp/users", new AddUserRequest("", USER_PASSWORD)),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        400,
        post("/idp/users", new AddUserRequest(USER1, " ")),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        400,
        post("/idp/users", new AddUserRequest("a".repeat(129), USER_PASSWORD)),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        409,
        post("/idp/users", new AddUserRequest(USER1, USER_PASSWORD)),
        ErrorConstants.ALREADY_EXISTS_CODE);

    changePassword(USER1, UPDATED_PASSWORD);
    Assertions.assertEquals(USER1, getUser(USER1).getUser().name());

    assertError(
        404,
        get("/idp/users/" + MISSING_USER, ADMIN, ADMIN_PASSWORD),
        ErrorConstants.NOT_FOUND_CODE);
    assertError(
        404,
        put("/idp/users/" + MISSING_USER, new ChangePasswordRequest(UPDATED_PASSWORD)),
        ErrorConstants.NOT_FOUND_CODE);
    assertError(
        400,
        put("/idp/users/" + USER1, new ChangePasswordRequest(" ")),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);

    Assertions.assertTrue(deleteUser(USER1));
    assertError(
        404, get("/idp/users/" + USER1, ADMIN, ADMIN_PASSWORD), ErrorConstants.NOT_FOUND_CODE);
    Assertions.assertFalse(deleteUser(USER1));
  }

  @Test
  void testManageGroups() throws Exception {
    postUser(USER1, USER_PASSWORD);
    postUser(USER2, USER_PASSWORD);

    IdpGroupResponse group = postGroup(GROUP1);
    Assertions.assertEquals(GROUP1, group.getGroup().name());
    Assertions.assertTrue(group.getGroup().users().isEmpty());

    assertError(
        409, post("/idp/groups", new AddGroupRequest(GROUP1)), ErrorConstants.ALREADY_EXISTS_CODE);
    assertError(
        400,
        post("/idp/groups", new AddGroupRequest("a".repeat(129))),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        404,
        get("/idp/groups/" + MISSING_GROUP, ADMIN, ADMIN_PASSWORD),
        ErrorConstants.NOT_FOUND_CODE);

    group =
        putMembership(GROUP1, new GroupMembershipChangeRequest(new String[] {USER1, USER2}, null));
    Assertions.assertEquals(Set.of(USER1, USER2), Set.copyOf(group.getGroup().users()));

    assertError(405, deleteGroupResponse(GROUP1, false), ErrorConstants.UNSUPPORTED_OPERATION_CODE);

    group =
        putMembership(GROUP1, new GroupMembershipChangeRequest(null, new String[] {USER1, USER2}));
    Assertions.assertTrue(group.getGroup().users().isEmpty());

    Assertions.assertTrue(deleteGroup(GROUP1, false));
    Assertions.assertFalse(deleteGroup(GROUP1, false));

    deleteUser(USER1);
    deleteUser(USER2);
  }

  @Test
  void testGroupMembership() throws Exception {
    postUser(USER1, USER_PASSWORD);
    postGroup(GROUP1);

    assertError(
        400,
        put("/idp/groups/" + GROUP1 + "/users", new GroupMembershipChangeRequest(null, null)),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        400,
        put(
            "/idp/groups/" + GROUP1 + "/users",
            new GroupMembershipChangeRequest(new String[] {}, new String[] {})),
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE);
    assertError(
        404,
        put(
            "/idp/groups/" + GROUP1 + "/users",
            new GroupMembershipChangeRequest(new String[] {MISSING_USER}, null)),
        ErrorConstants.NOT_FOUND_CODE);
    assertError(
        404,
        put(
            "/idp/groups/" + MISSING_GROUP + "/users",
            new GroupMembershipChangeRequest(new String[] {USER1}, null)),
        ErrorConstants.NOT_FOUND_CODE);

    IdpGroupResponse group =
        putMembership(GROUP1, new GroupMembershipChangeRequest(new String[] {USER1}, null));
    Assertions.assertEquals(List.of(USER1), group.getGroup().users());

    Assertions.assertTrue(deleteGroup(GROUP1, true));
    deleteUser(USER1);
  }

  private static HttpResponse<String> get(String path, String username, String password)
      throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(URI.create(apiBase + path)).header("Accept", ACCEPT).GET();
    if (StringUtils.isNotBlank(username)) {
      builder.header("Authorization", basicAuth(username, password));
    }
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static void postUser(String username, String password) throws Exception {
    HttpResponse<String> response = post("/idp/users", new AddUserRequest(username, password));
    Assertions.assertEquals(200, response.statusCode(), response.body());
    JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class).validate();
  }

  private static IdpUserResponse getUser(String username) throws Exception {
    HttpResponse<String> response = get("/idp/users/" + username, ADMIN, ADMIN_PASSWORD);
    Assertions.assertEquals(200, response.statusCode(), response.body());
    IdpUserResponse userResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class);
    userResponse.validate();
    return userResponse;
  }

  private static void changePassword(String username, String password) throws Exception {
    HttpResponse<String> response =
        put("/idp/users/" + username, new ChangePasswordRequest(password));
    Assertions.assertEquals(200, response.statusCode(), response.body());
  }

  private static boolean deleteUser(String username) throws Exception {
    HttpResponse<String> response = deleteUserResponse(username);
    Assertions.assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private static HttpResponse<String> deleteUserResponse(String username) throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + "/idp/users/" + username))
            .DELETE()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static IdpGroupResponse postGroup(String groupName) throws Exception {
    HttpResponse<String> response = post("/idp/groups", new AddGroupRequest(groupName));
    Assertions.assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private static IdpGroupResponse putMembership(
      String groupName, GroupMembershipChangeRequest request) throws Exception {
    HttpResponse<String> response = put("/idp/groups/" + groupName + "/users", request);
    Assertions.assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private static boolean deleteGroup(String groupName, boolean force) throws Exception {
    HttpResponse<String> response = deleteGroupResponse(groupName, force);
    Assertions.assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private static HttpResponse<String> deleteGroupResponse(String groupName, boolean force)
      throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + "/idp/groups/" + groupName + "?force=" + force))
            .DELETE()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpResponse<String> post(String path, Object body) throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + path))
            .header("Content-Type", ACCEPT)
            .POST(jsonBody(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpResponse<String> put(String path, Object body) throws Exception {
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

  private static void assertError(int expectedStatus, HttpResponse<String> response, int errorCode)
      throws Exception {
    Assertions.assertEquals(expectedStatus, response.statusCode(), response.body());
    Assertions.assertEquals(errorCode, errorCode(response));
  }

  private static int errorCode(HttpResponse<String> response) throws Exception {
    return JsonUtils.objectMapper().readTree(response.body()).get("code").asInt();
  }

  private static String oauthPublicSignKey() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return Base64.getEncoder().encodeToString(generator.generateKeyPair().getPublic().getEncoded());
  }
}
