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

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.Key;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestStaticSignKeyValidator {

  private StaticSignKeyValidator validator;
  private Key hmacKey;
  private String serviceAudience = "test-service";

  @BeforeEach
  public void setUp() {
    validator = new StaticSignKeyValidator();
    hmacKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);
  }

  private Config createConfig(Map<String, String> properties) {
    Config config = new Config(false) {};
    config.loadFromMap(properties, k -> true);
    return config;
  }

  private Map<String, String> createBaseConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(
        "gravitino.authenticator.oauth.defaultSignKey",
        Base64.getEncoder().encodeToString(hmacKey.getEncoded()));
    config.put("gravitino.authenticator.oauth.signAlgorithmType", "HS256");
    config.put("gravitino.authenticator.oauth.allowSkewSecs", "60");
    // Add required configuration parameters
    config.put("gravitino.authenticator.oauth.serverUri", "http://localhost:8080");
    config.put("gravitino.authenticator.oauth.tokenPath", "/oauth/token");
    return config;
  }

  @Test
  public void testInitializeSuccess() {
    Map<String, String> config = createBaseConfig();

    validator.initialize(createConfig(config));
    // Should not throw exception
  }

  @Test
  public void testInitializeWithMissingSignKey() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.signAlgorithmType", "HS256");
    config.put("gravitino.authenticator.oauth.serverUri", "http://localhost:8080");
    config.put("gravitino.authenticator.oauth.tokenPath", "/oauth/token");

    assertThrows(IllegalArgumentException.class, () -> validator.initialize(createConfig(config)));
  }

  @Test
  public void testValidateValidToken() {
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create valid token
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("test-user", principal.getName());
  }

  @Test
  public void testValidateTokenWithMultipleAudiences() {
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create token with multiple audiences
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .claim("aud", Arrays.asList("other-service", serviceAudience, "another-service"))
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("test-user", principal.getName());
  }

  @Test
  public void testValidateTokenWithWrongAudience() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create token with wrong audience
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setAudience("wrong-audience")
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken(token, serviceAudience));
  }

  @Test
  public void testValidateTokenWithNoAudience() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create token without audience
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken(token, serviceAudience));
  }

  @Test
  public void testValidateExpiredToken() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create expired token
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now().minusSeconds(7200)))
            .setExpiration(Date.from(Instant.now().minusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken(token, serviceAudience));
  }

  @Test
  public void testValidateNullToken() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    assertThrows(UnauthorizedException.class, () -> validator.validateToken(null, serviceAudience));
  }

  @Test
  public void testValidateMalformedToken() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // JsonSyntaxException is thrown for malformed tokens due to Gson parsing
    assertThrows(
        com.google.gson.JsonSyntaxException.class,
        () -> validator.validateToken("invalid.jwt.token", serviceAudience));
  }

  @Test
  public void testValidateTokenWithWrongSignature() {
    // Setup validator
    Map<String, String> config = createBaseConfig();
    validator.initialize(createConfig(config));

    // Create token with different key
    Key differentKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(differentKey, SignatureAlgorithm.HS256)
            .compact();

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken(token, serviceAudience));
  }
}
