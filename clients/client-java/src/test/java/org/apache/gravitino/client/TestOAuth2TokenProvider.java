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

package org.apache.gravitino.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Date;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.OAuth2ErrorResponse;
import org.apache.gravitino.dto.responses.OAuth2TokenResponse;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;

@SuppressWarnings("JavaUtilDate")
public class TestOAuth2TokenProvider {

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
    DefaultOAuth2TokenProvider.Builder tokenProvider1 =
        DefaultOAuth2TokenProvider.builder().withUri("test");
    DefaultOAuth2TokenProvider.Builder tokenProvider2 =
        DefaultOAuth2TokenProvider.builder().withUri("test").withCredential("xx");
    DefaultOAuth2TokenProvider.Builder tokenProvider3 =
        DefaultOAuth2TokenProvider.builder().withUri("test").withCredential("xx").withScope("test");

    Assertions.assertThrows(IllegalArgumentException.class, () -> tokenProvider1.build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> tokenProvider2.build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> tokenProvider3.build());
  }

  @Test
  public void testAuthenticationError() throws Exception {

    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    OAuth2ErrorResponse respBody =
        new OAuth2ErrorResponse(OAuth2ClientUtil.INVALID_CLIENT_ERROR, "invalid");
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(respBody);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("yy:xx")
            .withPath("oauth/token")
            .withScope("test");
    Assertions.assertThrows(UnauthorizedException.class, builder::build);

    respBody = new OAuth2ErrorResponse(OAuth2ClientUtil.INVALID_GRANT_ERROR, "invalid");
    respJson = ObjectMapperProvider.objectMapper().writeValueAsString(respBody);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    Assertions.assertThrows(BadRequestException.class, builder::build);
  }

  @Test
  public void testAuthenticationNormal() throws Exception {
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("yy:xx")
            .withPath("oauth/token")
            .withScope("test");

    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
    OAuth2TokenResponse response = new OAuth2TokenResponse("1", "2", "3", 1, "test", null);
    String respJson = objectMapper.writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    response = new OAuth2TokenResponse("1", "2", "bearer", 1, "test", null);
    respJson = objectMapper.writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(2)).respond(mockResponse);
    OAuth2TokenProvider provider = builder.build();
    Assertions.assertTrue(provider.hasTokenData());
    Assertions.assertNotNull(provider.getTokenData());
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String oldAccessToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() - 5))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();

    response = new OAuth2TokenResponse(oldAccessToken, "2", "bearer", 1, "test", null);
    respJson = objectMapper.writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    provider = builder.build();
    String accessToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 10000))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();

    response = new OAuth2TokenResponse(accessToken, "2", "bearer", 1, "test", null);
    respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    Assertions.assertNotEquals(accessToken, oldAccessToken);
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BEARER_HEADER + accessToken,
        new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  public void testTokenNotFetchedWhenValid() throws Exception {
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("yy:xx")
            .withPath("oauth/token")
            .withScope("test");
    HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String accessToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 10000))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();

    OAuth2TokenResponse response =
        new OAuth2TokenResponse(accessToken, "2", "bearer", 1, "test", null);
    String respJson = objectMapper.writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(any(), Times.exactly(1)).respond(mockResponse);
    OAuth2TokenProvider provider = builder.build();
    String token = provider.getAccessToken();
    Assertions.assertEquals(accessToken, token);
    String oldToken = provider.getAccessToken();
    Assertions.assertEquals(accessToken, oldToken);
  }
}
