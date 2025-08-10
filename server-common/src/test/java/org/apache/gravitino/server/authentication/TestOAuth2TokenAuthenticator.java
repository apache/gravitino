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
package org.apache.gravitino.server.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultClaims;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyPair;
import java.security.Principal;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Test;

@SuppressWarnings("JavaUtilDate")
public class TestOAuth2TokenAuthenticator {

  @Test
  public void testAuthentication() {
    OAuth2TokenAuthenticator auth2TokenAuthenticator = new OAuth2TokenAuthenticator();
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String publicKey =
        new String(
            Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);
    Config config = new Config(false) {};
    config.set(OAuthConfig.SERVICE_AUDIENCE, "service1");
    assertThrows(IllegalArgumentException.class, () -> auth2TokenAuthenticator.initialize(config));
    config.set(OAuthConfig.DEFAULT_SIGN_KEY, publicKey);
    config.set(OAuthConfig.SIGNATURE_ALGORITHM_TYPE, "RS256");
    config.set(OAuthConfig.ALLOW_SKEW_SECONDS, 6L);
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "test");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "test");
    auth2TokenAuthenticator.initialize(config);
    assertTrue(auth2TokenAuthenticator.isDataFromToken());
    Exception e =
        assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(null));
    assertEquals("Empty token authorization header", e.getMessage());
    byte[] bytes = "Xx".getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes));
    assertEquals("Invalid token authorization header", e.getMessage());
    byte[] bytes2 = AuthConstants.AUTHORIZATION_BEARER_HEADER.getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class,
            () -> {
              auth2TokenAuthenticator.authenticateToken(bytes2);
            });
    assertEquals("Blank token found", e.getMessage());
    String token1 =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    String header1 = AuthConstants.AUTHORIZATION_BEARER_HEADER + token1;
    byte[] bytes3 = header1.getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class,
            () -> {
              auth2TokenAuthenticator.authenticateToken(bytes3);
            });
    assertEquals("Found null Audience in token", e.getMessage());

    String token2 =
        Jwts.builder()
            .setSubject("gravitino")
            .setAudience("xxxx")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    String header2 = AuthConstants.AUTHORIZATION_BEARER_HEADER + token2;
    byte[] bytes4 = header2.getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes4));
    assertEquals("Audience in the token [xxxx] doesn't contain service1", e.getMessage());

    Claims audienceClaims = new DefaultClaims();
    audienceClaims.put(Claims.AUDIENCE, Lists.newArrayList("x1", "x2", "x3"));
    String token3 =
        Jwts.builder()
            .setSubject("gravitino")
            .setClaims(audienceClaims)
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    String header3 = AuthConstants.AUTHORIZATION_BEARER_HEADER + token3;
    byte[] bytes5 = header3.getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes5));
    assertEquals("Audiences in the token [x1, x2, x3] don't contain service1", e.getMessage());

    audienceClaims = new DefaultClaims();
    Map<String, String> map = Maps.newHashMap();
    map.put("k1", "v1");
    map.put("k2", "v2");
    audienceClaims.put(Claims.AUDIENCE, map);
    String token4 =
        Jwts.builder()
            .setSubject("gravitino")
            .setClaims(audienceClaims)
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    String header4 = AuthConstants.AUTHORIZATION_BEARER_HEADER + token4;
    byte[] bytes6 = header4.getBytes(StandardCharsets.UTF_8);
    e =
        assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes6));
    assertEquals("Audiences in token is not in expected format: {k1=v1, k2=v2}", e.getMessage());

    String token5 =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    assertEquals(
        "gravitino",
        auth2TokenAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BEARER_HEADER + token5)
                    .getBytes(StandardCharsets.UTF_8))
            .getName());
  }

  @Test
  public void testInitializeWithBlankServiceAudience() {
    OAuth2TokenAuthenticator authenticator = new OAuth2TokenAuthenticator();
    Config config = new Config(false) {};
    config.set(OAuthConfig.SERVICE_AUDIENCE, "");
    Key tempKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    config.set(
        OAuthConfig.DEFAULT_SIGN_KEY, Base64.getEncoder().encodeToString(tempKey.getEncoded()));
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "/token");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "http://localhost:8080");

    assertThrows(IllegalArgumentException.class, () -> authenticator.initialize(config));
  }

  @Test
  public void testInitializeWithJwksConfiguration() {
    OAuth2TokenAuthenticator authenticator = new OAuth2TokenAuthenticator();

    Config config = new Config(false) {};
    config.set(OAuthConfig.SERVICE_AUDIENCE, "test-service");
    config.set(
        OAuthConfig.JWKS_URI, "https://login.microsoftonline.com/common/discovery/v2.0/keys");
    config.set(OAuthConfig.AUTHORITY, "https://login.microsoftonline.com");
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "/token");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "http://localhost:8080");
    config.set(
        OAuthConfig.TOKEN_VALIDATOR_CLASS,
        "org.apache.gravitino.server.authentication.JwksTokenValidator");

    // Should initialize with JWKS validator
    authenticator.initialize(config);
    assertTrue(authenticator.isDataFromToken());
  }

  @Test
  public void testSupportsToken() {
    OAuth2TokenAuthenticator authenticator = new OAuth2TokenAuthenticator();

    // Test valid Bearer token
    String bearerToken = AuthConstants.AUTHORIZATION_BEARER_HEADER + "some-token";
    byte[] tokenData = bearerToken.getBytes(StandardCharsets.UTF_8);
    assertTrue(authenticator.supportsToken(tokenData));

    // Test invalid token format
    String basicToken = "Basic some-token";
    byte[] basicTokenData = basicToken.getBytes(StandardCharsets.UTF_8);
    assertTrue(!authenticator.supportsToken(basicTokenData));

    // Test null token
    assertTrue(!authenticator.supportsToken(null));

    // Test empty token data
    assertTrue(!authenticator.supportsToken(new byte[0]));
  }

  @Test
  public void testAuthenticateWithMultipleAudiencesIncludingCorrect() {
    OAuth2TokenAuthenticator authenticator = new OAuth2TokenAuthenticator();
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String publicKey = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());

    Config config = new Config(false) {};
    config.set(OAuthConfig.SERVICE_AUDIENCE, "service1");
    config.set(OAuthConfig.DEFAULT_SIGN_KEY, publicKey);
    config.set(OAuthConfig.SIGNATURE_ALGORITHM_TYPE, "RS256");
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "/token");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "http://localhost:8080");

    authenticator.initialize(config);

    Claims claims = new DefaultClaims();
    claims.put(Claims.AUDIENCE, Lists.newArrayList("other1", "service1", "other2"));
    String token =
        Jwts.builder()
            .setClaims(claims)
            .setSubject("test-user") // Set subject AFTER claims to avoid overwriting
            .setExpiration(new Date(System.currentTimeMillis() + 100000))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();

    String bearerToken = AuthConstants.AUTHORIZATION_BEARER_HEADER + token;
    byte[] tokenData = bearerToken.getBytes(StandardCharsets.UTF_8);

    Principal principal = authenticator.authenticateToken(tokenData);
    assertNotNull(principal);
    assertEquals("test-user", principal.getName());
  }

  @Test
  public void testAuthenticateWithEmptyTokenAfterBearer() {
    OAuth2TokenAuthenticator authenticator = new OAuth2TokenAuthenticator();
    Config config = new Config(false) {};
    config.set(OAuthConfig.SERVICE_AUDIENCE, "service1");
    Key tempKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    config.set(
        OAuthConfig.DEFAULT_SIGN_KEY, Base64.getEncoder().encodeToString(tempKey.getEncoded()));
    config.set(OAuthConfig.SIGNATURE_ALGORITHM_TYPE, "HS256");
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "/token");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "http://localhost:8080");

    authenticator.initialize(config);

    String bearerToken = AuthConstants.AUTHORIZATION_BEARER_HEADER + "   ";
    byte[] tokenData = bearerToken.getBytes(StandardCharsets.UTF_8);

    assertThrows(UnauthorizedException.class, () -> authenticator.authenticateToken(tokenData));
  }
}
