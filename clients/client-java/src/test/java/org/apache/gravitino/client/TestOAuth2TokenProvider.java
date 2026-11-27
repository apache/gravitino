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

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
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
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;

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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
    Assertions.assertThrows(IllegalArgumentException.class, builder::build);
    response = new OAuth2TokenResponse("1", "2", "bearer", 1, "test", null);
    respJson = objectMapper.writeValueAsString(response);
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(HttpRequest.request(), Times.exactly(2)).respond(mockResponse);
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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
    Assertions.assertNotEquals(accessToken, oldAccessToken);
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BEARER_HEADER + accessToken,
        new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  public void testAuthenticationPasswordGrant() throws Exception {
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    String accessToken = "access-token";
    OAuth2TokenResponse response =
        new OAuth2TokenResponse(accessToken, "2", "bearer", 1, "test", null);
    HttpResponse mockResponse =
        HttpResponse.response()
            .withStatusCode(HttpStatus.SC_OK)
            .withBody(objectMapper.writeValueAsString(response));

    mockServer.clear(HttpRequest.request().withMethod("POST").withPath("/oauth/token"));

    // The password is the last part of the credential, so it may contain ':' and leading or
    // trailing whitespace, and must be sent to the token endpoint verbatim.
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withBody(
                    ParameterBody.params(
                        Parameter.param("grant_type", "password"),
                        Parameter.param("client_id", "client-id"),
                        Parameter.param("client_secret", "client-secret"),
                        Parameter.param("username", "usera"),
                        Parameter.param("password", " pass:word "),
                        Parameter.param("scope", "test"))),
            // build() requests the token once, and getAccessToken() requests it again because
            // "access-token" is not a JWT and therefore has no expiry.
            Times.exactly(2))
        .respond(mockResponse);

    OAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("password:client-id:client-secret:usera: pass:word ")
            .withPath("oauth/token")
            .withScope("test")
            .build();

    Assertions.assertEquals(accessToken, provider.getAccessToken());
  }

  @Test
  public void testAuthenticationPasswordGrantWithClientSecretBasic() throws Exception {
    OAuth2TokenResponse response =
        new OAuth2TokenResponse("access-token", "refresh-token", "bearer", 3600, "test", null);
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);

    String basicAuth =
        "Basic "
            + Base64.getEncoder()
                .encodeToString("client-id:client-secret".getBytes(StandardCharsets.UTF_8));

    HttpRequest tokenRequest = HttpRequest.request().withMethod("POST").withPath("/oauth/token");
    mockServer.clear(tokenRequest);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withHeader("Authorization", basicAuth)
                .withBody(
                    ParameterBody.params(
                        Parameter.param("grant_type", "password"),
                        Parameter.param("username", "usera"),
                        Parameter.param("password", "pass"),
                        Parameter.param("scope", "test"))),
            Times.exactly(2))
        .respond(HttpResponse.response().withStatusCode(HttpStatus.SC_OK).withBody(respJson));

    OAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("password:client-id:client-secret:usera:pass")
            .withPath("oauth/token")
            .withScope("test")
            .withClientAuthenticationMethod(OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
            .build();

    Assertions.assertEquals("access-token", provider.getAccessToken());

    HttpRequest[] recordedRequests = mockServer.retrieveRecordedRequests(tokenRequest);
    Assertions.assertEquals(2, recordedRequests.length);
    for (HttpRequest recordedRequest : recordedRequests) {
      Assertions.assertEquals(basicAuth, recordedRequest.getFirstHeader("Authorization"));
      String requestBody = recordedRequest.getBodyAsString();
      Assertions.assertFalse(requestBody.contains("client_id"));
      Assertions.assertFalse(requestBody.contains("client_secret"));
    }
  }

  @Test
  public void testAuthenticationPasswordGrantWithEmptyClientId() throws Exception {
    OAuth2TokenResponse response =
        new OAuth2TokenResponse("access-token", "refresh-token", "bearer", 3600, "test", null);
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);

    HttpRequest tokenRequest = HttpRequest.request().withMethod("POST").withPath("/oauth/token");
    mockServer.clear(tokenRequest);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withBody(
                    ParameterBody.params(
                        Parameter.param("grant_type", "password"),
                        Parameter.param("client_secret", "client-secret"),
                        Parameter.param("username", "usera"),
                        Parameter.param("password", "pass"),
                        Parameter.param("scope", "test"))),
            Times.exactly(2))
        .respond(HttpResponse.response().withStatusCode(HttpStatus.SC_OK).withBody(respJson));

    OAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("password::client-secret:usera:pass")
            .withPath("oauth/token")
            .withScope("test")
            .build();

    Assertions.assertEquals("access-token", provider.getAccessToken());

    HttpRequest[] recordedRequests = mockServer.retrieveRecordedRequests(tokenRequest);
    Assertions.assertEquals(2, recordedRequests.length);
    for (HttpRequest recordedRequest : recordedRequests) {
      Assertions.assertFalse(recordedRequest.getBodyAsString().contains("client_id"));
    }
  }

  @Test
  public void testInvalidPasswordGrantCredential() {
    DefaultOAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withPath("oauth/token")
            .withScope("test");

    // The credential must have the password:<client_id>:<client_secret>:<username>:<password>
    // form.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> builder.withCredential("password:client-id:client-secret:username").build());
    // The username and password are mandatory for the password grant.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> builder.withCredential("password:client-id:client-secret::password").build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> builder.withCredential("password:client-id:client-secret:usera:").build());
    // An empty client secret is rejected rather than sent as an empty form value.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> builder.withCredential("password:client-id::usera:password").build());
  }

  @Test
  public void testLegacyCredentialWithColonInSecret() throws Exception {
    OAuth2TokenResponse response =
        new OAuth2TokenResponse("access-token", "refresh-token", "bearer", 3600, "test", null);
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);
    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_OK).withBody(respJson);

    mockServer.clear(HttpRequest.request().withMethod("POST").withPath("/oauth/token"));

    // Credentials that were valid before the password grant was added must keep their meaning: the
    // whole remainder after the first ':' is the client secret. In particular, a client secret
    // with two ':' must not be reinterpreted as a password grant credential.
    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withBody(
                    ParameterBody.params(
                        Parameter.param("grant_type", "client_credentials"),
                        Parameter.param("client_id", "client-id"),
                        Parameter.param("client_secret", "se:cr:et"),
                        Parameter.param("scope", "test"))),
            Times.exactly(2))
        .respond(mockResponse);

    OAuth2TokenProvider provider =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("client-id:se:cr:et")
            .withPath("oauth/token")
            .withScope("test")
            .build();

    Assertions.assertEquals("access-token", provider.getAccessToken());
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
    mockServer.when(HttpRequest.request(), Times.exactly(1)).respond(mockResponse);
    OAuth2TokenProvider provider = builder.build();
    String token = provider.getAccessToken();
    Assertions.assertEquals(accessToken, token);
    String oldToken = provider.getAccessToken();
    Assertions.assertEquals(accessToken, oldToken);
  }

  @Test
  public void testClientSecretBasicAuthentication() throws Exception {
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("clientId:clientSecret")
            .withPath("oauth/token")
            .withScope("test")
            .withClientAuthenticationMethod(OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC);

    OAuth2TokenResponse response =
        new OAuth2TokenResponse("access-token", "refresh-token", "bearer", 3600, "test", null);
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);

    String basicAuth =
        "Basic "
            + Base64.getEncoder()
                .encodeToString("clientId:clientSecret".getBytes(StandardCharsets.UTF_8));

    HttpRequest tokenRequest = HttpRequest.request().withMethod("POST").withPath("/oauth/token");

    mockServer.clear(tokenRequest);

    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_OK).withBody(respJson);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withHeader("Authorization", basicAuth),
            Times.exactly(2))
        .respond(mockResponse);

    OAuth2TokenProvider provider = builder.build();

    Assertions.assertTrue(provider.hasTokenData());
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BEARER_HEADER + "access-token",
        new String(provider.getTokenData(), StandardCharsets.UTF_8));

    HttpRequest[] recordedRequests = mockServer.retrieveRecordedRequests(tokenRequest);
    Assertions.assertEquals(2, recordedRequests.length);

    for (HttpRequest recordedRequest : recordedRequests) {
      Assertions.assertEquals(basicAuth, recordedRequest.getFirstHeader("Authorization"));

      String requestBody = recordedRequest.getBodyAsString();
      Assertions.assertFalse(requestBody.contains("client_id"));
      Assertions.assertFalse(requestBody.contains("client_secret"));
    }
  }

  @Test
  public void testClientSecretBasicRequiresClientId() {
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("clientSecret")
            .withPath("oauth/token")
            .withScope("test")
            .withClientAuthenticationMethod(OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC);

    Assertions.assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testRejectsNullClientAuthenticationMethod() {
    DefaultOAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("clientId:clientSecret")
            .withPath("oauth/token")
            .withScope("test");

    Assertions.assertThrows(
        NullPointerException.class, () -> builder.withClientAuthenticationMethod(null));
  }

  @Test
  public void testFromValueAcceptsRfcStyleNames() {
    Assertions.assertEquals(
        OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        OAuth2ClientAuthenticationMethod.fromValue("client_secret_basic"));
    Assertions.assertEquals(
        OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        OAuth2ClientAuthenticationMethod.fromValue("CLIENT_SECRET_BASIC"));
    Assertions.assertEquals(
        OAuth2ClientAuthenticationMethod.CLIENT_SECRET_POST,
        OAuth2ClientAuthenticationMethod.fromValue("client_secret_post"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> OAuth2ClientAuthenticationMethod.fromValue("basic"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> OAuth2ClientAuthenticationMethod.fromValue(""));
  }

  @Test
  public void testClientSecretBasicEncodesReservedCharacters() throws Exception {
    OAuth2TokenProvider.Builder builder =
        DefaultOAuth2TokenProvider.builder()
            .withUri(String.format("http://127.0.0.1:%d", PORT))
            .withCredential("client*@id:s ecret:/+?~")
            .withPath("oauth/token")
            .withScope("test")
            .withClientAuthenticationMethod(OAuth2ClientAuthenticationMethod.CLIENT_SECRET_BASIC);

    OAuth2TokenResponse response =
        new OAuth2TokenResponse("access-token", "refresh-token", "bearer", 3600, "test", null);
    String respJson = ObjectMapperProvider.objectMapper().writeValueAsString(response);

    String basicAuth = "Basic Y2xpZW50KiU0MGlkOnMrZWNyZXQlM0ElMkYlMkIlM0YlN0U=";

    HttpRequest tokenRequest = HttpRequest.request().withMethod("POST").withPath("/oauth/token");
    mockServer.clear(tokenRequest);

    HttpResponse mockResponse =
        HttpResponse.response().withStatusCode(HttpStatus.SC_OK).withBody(respJson);

    mockServer
        .when(
            HttpRequest.request()
                .withMethod("POST")
                .withPath("/oauth/token")
                .withHeader("Authorization", basicAuth),
            Times.exactly(2))
        .respond(mockResponse);

    OAuth2TokenProvider provider = builder.build();

    Assertions.assertTrue(provider.hasTokenData());
    Assertions.assertEquals(
        AuthConstants.AUTHORIZATION_BEARER_HEADER + "access-token",
        new String(provider.getTokenData(), StandardCharsets.UTF_8));
    HttpRequest[] recordedRequests = mockServer.retrieveRecordedRequests(tokenRequest);
    Assertions.assertEquals(2, recordedRequests.length);

    for (HttpRequest recordedRequest : recordedRequests) {
      Assertions.assertEquals(basicAuth, recordedRequest.getFirstHeader("Authorization"));

      String requestBody = recordedRequest.getBodyAsString();
      Assertions.assertFalse(requestBody.contains("client_id"));
      Assertions.assertFalse(requestBody.contains("client_secret"));
    }
  }
}
