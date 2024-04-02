/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;

import com.datastrato.gravitino.client.DefaultOAuth2TokenProvider;
import com.datastrato.gravitino.client.ErrorHandlers;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.dto.responses.OAuth2ErrorResponse;
import com.datastrato.gravitino.dto.responses.OAuth2TokenResponse;
import com.datastrato.gravitino.exceptions.BadRequestException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;

public class TestOauth2Client extends TestGvfsBase {
  private static final String normal_path = "token/test";
  private static final String invalid_path = "token/invalid";
  private static final String credential = "xx:xx";
  private static final String scope = "test";

  @BeforeAll
  public static void setup() {
    Oauth2MockServerBase.setup();
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
        Oauth2MockServerBase.serverUri());
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, normal_path);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
        credential);
    conf.set(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY, scope);
  }

  @AfterAll
  public static void teardown() {
    Oauth2MockServerBase.tearDown();
  }

  @Test
  public void testAuthNormally() {
    DefaultOAuth2TokenProvider authDataProvider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(Oauth2MockServerBase.serverUri())
            .withCredential(credential)
            .withPath(normal_path)
            .withScope(scope)
            .build();

    HTTPClient client =
        HTTPClient.builder(new HashMap<>())
            .uri(Oauth2MockServerBase.serverUri())
            .withAuthDataProvider(authDataProvider)
            .build();

    OAuth2TokenResponse response =
        client.get(
            normal_path,
            OAuth2TokenResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.restErrorHandler());
    assertNotNull(response);
    assertEquals(Oauth2MockServerBase.accessToken(), response.getAccessToken());
    assertEquals("2", response.getIssuedTokenType());
    assertEquals("bearer", response.getTokenType());
    assertEquals("test", response.getScope());
    assertEquals(1, response.getExpiresIn());
  }

  @Test
  public void testFileSystemAuthConfigs() throws IOException {
    // init conf
    Configuration configuration = new Configuration();
    configuration.set(
        String.format(
            "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
        "true");
    configuration.set("fs.gvfs.impl", GVFS_IMPL_CLASS);
    configuration.set("fs.AbstractFileSystem.gvfs.impl", GVFS_ABSTRACT_IMPL_CLASS);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        GravitinoMockServerBase.serverUri());

    // set auth type, but do not set other configs
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE);
    assertThrows(
        IllegalArgumentException.class, () -> managedFilesetPath.getFileSystem(configuration));

    // set oauth server uri, but do not set other configs
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
        Oauth2MockServerBase.serverUri());
    assertThrows(
        IllegalArgumentException.class, () -> managedFilesetPath.getFileSystem(configuration));

    // set oauth server path, but do not set other configs
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, normal_path);
    assertThrows(
        IllegalArgumentException.class, () -> managedFilesetPath.getFileSystem(configuration));

    // set oauth credential, but do not set other configs
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
        credential);
    assertThrows(
        IllegalArgumentException.class, () -> managedFilesetPath.getFileSystem(configuration));

    // set oauth scope, all configs are set
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY, scope);
    assertNotNull(managedFilesetPath.getFileSystem(configuration));
  }

  @Test
  public void testFileSystemAuthUnauthorized() {
    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    OAuth2ErrorResponse respBody = new OAuth2ErrorResponse("invalid_client", "invalid");
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
      Oauth2MockServerBase.mockServer()
          .when(request().withPath("/" + invalid_path), Times.exactly(1))
          .respond(mockResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, invalid_path);
    // should throw UnauthorizedException
    assertThrows(
        UnauthorizedException.class, () -> managedFilesetPath.getFileSystem(configuration));

    // test wrong client secret
    mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    OAuth2ErrorResponse response =
        new OAuth2ErrorResponse("invalid_client", "invalid client secret");
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(response);
      mockResponse = mockResponse.withBody(respJson);
      Map<String, String> bodyMap = new HashMap<>();
      bodyMap.put("grant_type", "client_credentials");
      bodyMap.put("client_id", "xx");
      bodyMap.put("client_secret", "xx");
      bodyMap.put("scope", scope);
      Oauth2MockServerBase.mockServer()
          .when(
              request()
                  .withPath("/" + invalid_path)
                  // mock the fetch token request body
                  .withBody(
                      new StringEntity(RESTUtils.encodeFormData(bodyMap)).getContentEncoding()),
              Times.exactly(1))
          .respond(mockResponse);
      assertThrows(
          UnauthorizedException.class, () -> managedFilesetPath.getFileSystem(configuration));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFileSystemAuthBadRequest() {
    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    OAuth2ErrorResponse respBody = new OAuth2ErrorResponse("invalid_grant", "invalid");
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
      Oauth2MockServerBase.mockServer()
          .when(request().withPath("/" + invalid_path), Times.exactly(1))
          .respond(mockResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY, invalid_path);
    // should throw BadRequestException
    assertThrows(BadRequestException.class, () -> managedFilesetPath.getFileSystem(configuration));
  }
}
