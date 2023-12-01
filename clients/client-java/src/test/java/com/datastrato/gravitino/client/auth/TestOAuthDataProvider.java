/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.datastrato.gravitino.dto.responses.OAuthErrorResponse;
import com.datastrato.gravitino.dto.responses.OAuthTokenResponse;
import com.datastrato.gravitino.exceptions.BadRequestException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.json.JsonUtils;
import com.google.common.collect.Maps;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.security.KeyPair;
import java.util.Date;
import java.util.Map;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;

public class TestOAuthDataProvider {

  private static final int PORT = 1082;
  private static ClientAndServer mockServer;

  @BeforeAll
  public static void beforeClass() {
    mockServer = startClientAndServer(PORT);
  }

  @AfterAll
  public static void stopServer() throws IOException {
    mockServer.stop();
  }

  @Test
  public void testProviderInitException() throws Exception {

    try (AuthDataProvider provider = new OAuthDataProvider()) {
      Map<String, String> props = Maps.newHashMap();
      Assertions.assertThrows(IllegalStateException.class, provider::hasTokenData);
      Assertions.assertThrows(IllegalArgumentException.class, () -> provider.initialize(props));
    }
  }

  @Test
  public void testAuthenticationError() throws Exception {
    try (AuthDataProvider provider = new OAuthDataProvider()) {
      Map<String, String> props = Maps.newHashMap();
      props.put(OAuth2ClientUtil.CREDENTIAL, "yy:xx");
      props.put(OAuth2ClientUtil.URI, String.format("http://127.0.0.1:%d", PORT));
      props.put(OAuth2ClientUtil.PATH, "oauth/token");
      props.put(OAuth2ClientUtil.SCOPE, "test");
      HttpResponse mockResponse =
          HttpResponse.response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      OAuthErrorResponse respBody =
          new OAuthErrorResponse(OAuth2ClientUtil.INVALID_CLIENT_ERROR, "invalid");
      String respJson = JsonUtils.objectMapper().writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      Assertions.assertThrows(UnauthorizedException.class, () -> provider.initialize(props));
      respBody = new OAuthErrorResponse(OAuth2ClientUtil.INVALID_GRANT_ERROR, "invalid");
      respJson = JsonUtils.objectMapper().writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      Assertions.assertThrows(BadRequestException.class, () -> provider.initialize(props));
    }
  }

  @Test
  public void testAuthenticationNormal() throws Exception {
    try (AuthDataProvider provider = new OAuthDataProvider()) {
      Map<String, String> props = Maps.newHashMap();
      props.put(OAuth2ClientUtil.CREDENTIAL, "yy:xx");
      props.put(OAuth2ClientUtil.URI, String.format("http://127.0.0.1:%d", PORT));
      props.put(OAuth2ClientUtil.PATH, "oauth/token");
      props.put(OAuth2ClientUtil.SCOPE, "test");
      HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
      OAuthTokenResponse response = new OAuthTokenResponse("1", "2", "3", 1, "test", null);
      String respJson = JsonUtils.objectMapper().writeValueAsString(response);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      Assertions.assertThrows(IllegalArgumentException.class, () -> provider.initialize(props));
      response = new OAuthTokenResponse("1", "2", "bearer", 1, "test", null);
      respJson = JsonUtils.objectMapper().writeValueAsString(response);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      provider.initialize(props);
      Assertions.assertTrue(provider.hasTokenData());
      Assertions.assertNull(provider.getTokenData());
      KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
      String oldAccessToken =
          Jwts.builder()
              .setSubject("gravitino")
              .setExpiration(new Date(System.currentTimeMillis() - 5))
              .setAudience("service1")
              .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
              .compact();

      response = new OAuthTokenResponse(oldAccessToken, "2", "bearer", 1, "test", null);
      respJson = JsonUtils.objectMapper().writeValueAsString(response);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      provider.initialize(props);
      String accessToken =
          Jwts.builder()
              .setSubject("gravitino")
              .setExpiration(new Date(System.currentTimeMillis() + 10000))
              .setAudience("service1")
              .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
              .compact();

      response = new OAuthTokenResponse(accessToken, "2", "bearer", 1, "test", null);
      respJson = JsonUtils.objectMapper().writeValueAsString(response);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
      Assertions.assertNotEquals(accessToken, oldAccessToken);
      Assertions.assertEquals(accessToken, new String(provider.getTokenData()));
    }
  }
}
