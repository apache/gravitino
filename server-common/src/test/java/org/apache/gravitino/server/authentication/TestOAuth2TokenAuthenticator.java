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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultClaims;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> auth2TokenAuthenticator.initialize(config));
    config.set(OAuthConfig.DEFAULT_SIGN_KEY, publicKey);
    config.set(OAuthConfig.ALLOW_SKEW_SECONDS, 6L);
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "test");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "test");
    auth2TokenAuthenticator.initialize(config);
    Assertions.assertTrue(auth2TokenAuthenticator.isDataFromToken());
    Exception e =
        Assertions.assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(null));
    Assertions.assertEquals("Empty token authorization header", e.getMessage());
    byte[] bytes = "Xx".getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes));
    Assertions.assertEquals("Invalid token authorization header", e.getMessage());
    byte[] bytes2 = AuthConstants.AUTHORIZATION_BEARER_HEADER.getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> {
              auth2TokenAuthenticator.authenticateToken(bytes2);
            });
    Assertions.assertEquals("Blank token found", e.getMessage());
    String token1 =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    String header1 = AuthConstants.AUTHORIZATION_BEARER_HEADER + token1;
    byte[] bytes3 = header1.getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> {
              auth2TokenAuthenticator.authenticateToken(bytes3);
            });
    Assertions.assertEquals("Found null Audience in token", e.getMessage());

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
        Assertions.assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes4));
    Assertions.assertEquals(
        "Audience in the token [xxxx] doesn't contain service1", e.getMessage());

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
        Assertions.assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes5));
    Assertions.assertEquals(
        "Audiences in the token [x1, x2, x3] don't contain service1", e.getMessage());

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
        Assertions.assertThrows(
            UnauthorizedException.class, () -> auth2TokenAuthenticator.authenticateToken(bytes6));
    Assertions.assertEquals(
        "Audiences in token is not in expected format: {k1=v1, k2=v2}", e.getMessage());

    String token5 =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 100))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    Assertions.assertEquals(
        "gravitino",
        auth2TokenAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BEARER_HEADER + token5)
                    .getBytes(StandardCharsets.UTF_8))
            .getName());
  }
}
