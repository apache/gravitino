/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.client.DefaultOAuth2TokenProvider;
import com.datastrato.gravitino.client.ErrorHandlers;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.dto.responses.OAuth2ErrorResponse;
import com.datastrato.gravitino.dto.responses.OAuth2TokenResponse;
import com.datastrato.gravitino.exceptions.BadRequestException;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rest.RESTUtils;
import com.datastrato.gravitino.server.authentication.OAuthConfig;
import com.datastrato.gravitino.server.authentication.ServerAuthenticator;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestOauth2Client extends TestGvfsBase {
  private static final String normal_path = "token/test";
  private static final String invalid_path = "token/invalid";
  private static final String credential = "xx:xx";
  private static final String scope = "test";
  private static final KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
  private static final String publicKey =
      new String(
          Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);

  private static String accessToken;

  @SuppressWarnings("JavaUtilDate")
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

    accessToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 10000))
            .setAudience("service1")
            .signWith(
                Keys.keyPairFor(SignatureAlgorithm.RS256).getPrivate(), SignatureAlgorithm.RS256)
            .compact();

    // mock authentication normally
    OAuth2TokenResponse response =
        new OAuth2TokenResponse(accessToken, "2", "bearer", 1, "test", null);
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(response);
      HttpResponse mockResponse = response().withStatusCode(HttpStatus.SC_OK);
      mockResponse = mockResponse.withBody(respJson);
      Oauth2MockServerBase.mockServer()
          .when(request().withPath("/token/test"), Times.unlimited())
          .respond(mockResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
    assertEquals(accessToken, response.getAccessToken());
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
  public void testFileSystemAuthUnauthorized() throws ParseException {
    // 1. test always throw UnauthorizedException
    HttpResponse mockResponse = response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
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

    // 2. test wrong client secret
    mockResponse = response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
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

    // 3. test expired token
    Config config = new Config(false) {};
    config.set(
        new ConfigBuilder("gravitino.authenticator").stringConf().createWithDefault("simple"),
        "oauth");
    config.set(OAuthConfig.SERVICE_AUDIENCE, "service1");
    config.set(OAuthConfig.DEFAULT_SIGN_KEY, publicKey);
    config.set(OAuthConfig.ALLOW_SKEW_SECONDS, 0L);
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, invalid_path);
    config.set(OAuthConfig.DEFAULT_SERVER_URI, Oauth2MockServerBase.serverUri());
    ServerAuthenticator authenticator = ServerAuthenticator.getInstance();
    authenticator.initialize(config);

    // set the expired time
    String dateString = "2010-01-02 12:30:45";
    String pattern = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    String expiredToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(dateFormat.parse(dateString))
            .setAudience("service1")
            .signWith(
                Keys.keyPairFor(SignatureAlgorithm.RS256).getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    // mock expired token, always return an expired token
    OAuth2TokenResponse expiredResponse =
        new OAuth2TokenResponse(expiredToken, "2", "bearer", 1, "test", null);
    try {
      String expiredRespJson = JsonUtils.objectMapper().writeValueAsString(expiredResponse);
      HttpResponse httpResponse = response().withStatusCode(HttpStatus.SC_OK);
      httpResponse = httpResponse.withBody(expiredRespJson);
      Oauth2MockServerBase.mockServer()
          .when(request().withPath("/" + invalid_path), Times.unlimited())
          .respond(httpResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    // mock load metalake with expired token
    String testMetalake = "test_token";
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + testMetalake)
            .withMethod(Method.GET.name())
            .withQueryStringParameters(Collections.emptyMap());

    GravitinoMockServerBase.mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            httpRequest -> {
              List<Header> headers = httpRequest.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  byte[] tokenValue =
                      header.getValues().get(0).getValue().getBytes(StandardCharsets.UTF_8);
                  // should throw an UnauthorizedException here
                  try {
                    authenticator.authenticator().authenticateToken(tokenValue);
                  } catch (UnauthorizedException e) {
                    assertTrue(e.getMessage().contains("JWT parse error"));
                    throw e;
                  }
                }
              }
              return response().withStatusCode(HttpStatus.SC_OK);
            });
    Path newPath = new Path(managedFilesetPath.toString().replace(metalakeName, testMetalake));
    Configuration config1 = new Configuration(configuration);
    config1.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, testMetalake);
    // UnauthorizedException will be caught by the client, and the RESTException will be thrown
    assertThrows(RESTException.class, () -> newPath.getFileSystem(config1));
  }

  @Test
  public void testFileSystemAuthBadRequest() {
    HttpResponse mockResponse = response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    OAuth2ErrorResponse respBody = new OAuth2ErrorResponse("invalid_grant", "invalid");
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
      Oauth2MockServerBase.mockServer()
          .when(request().withPath("/" + invalid_path + "/bad"), Times.exactly(1))
          .respond(mockResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
        invalid_path + "/bad");
    // should throw BadRequestException
    assertThrows(BadRequestException.class, () -> managedFilesetPath.getFileSystem(configuration));
  }
}
