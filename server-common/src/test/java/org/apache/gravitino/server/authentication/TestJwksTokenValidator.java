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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.net.URL;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestJwksTokenValidator {

  private JwksTokenValidator validator;
  private String serviceAudience = "test-service";
  private String validJwksUri = "https://login.microsoftonline.com/common/discovery/v2.0/keys";

  @BeforeEach
  public void setUp() {
    validator = new JwksTokenValidator();
  }

  private Config createConfig(Map<String, String> properties) {
    Config config = new Config(false) {};
    config.loadFromMap(properties, k -> true);
    return config;
  }

  @Test
  public void testInitializeSuccess() {
    Map<String, String> properties = new HashMap<>();
    properties.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    properties.put("gravitino.authenticator.oauth.authority", "https://login.microsoftonline.com");
    properties.put("gravitino.authenticator.oauth.principalFields", "sub");
    properties.put("gravitino.authenticator.oauth.allowSkewSecs", "60");

    validator.initialize(createConfig(properties));
    // Should not throw exception
  }

  @Test
  public void testInitializeWithoutJwksUri() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.authority", "https://login.microsoftonline.com");

    assertThrows(IllegalArgumentException.class, () -> validator.initialize(createConfig(config)));
  }

  @Test
  public void testInitializeWithInvalidJwksUri() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.jwksUri", "invalid-url");

    assertThrows(IllegalArgumentException.class, () -> validator.initialize(createConfig(config)));
  }

  @Test
  public void testValidateNullToken() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    validator.initialize(createConfig(config));

    assertThrows(UnauthorizedException.class, () -> validator.validateToken(null, serviceAudience));
  }

  @Test
  public void testValidateEmptyToken() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    validator.initialize(createConfig(config));

    assertThrows(UnauthorizedException.class, () -> validator.validateToken("", serviceAudience));
  }

  @Test
  public void testCustomPrincipalFields() throws Exception {
    // Generate a test RSA key pair
    RSAKey rsaKey =
        new RSAKeyGenerator(2048).keyID("test-key-id").algorithm(JWSAlgorithm.RS256).generate();

    // Create a signed JWT token with custom principal field
    JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .subject("default-subject")
            .claim("client_id", "custom-client-123") // This should be used as principal
            .claim("email", "user@example.com")
            .audience("test-service")
            .issuer("https://test-issuer.com")
            .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
            .issueTime(Date.from(Instant.now()))
            .build();

    SignedJWT signedJWT =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key-id").build(), claimsSet);

    JWSSigner signer = new RSASSASigner(rsaKey);
    signedJWT.sign(signer);

    String tokenString = signedJWT.serialize();

    // Mock the JWKSourceBuilder to return our test key
    try (MockedStatic<JWKSourceBuilder> mockedBuilder = mockStatic(JWKSourceBuilder.class)) {
      @SuppressWarnings("unchecked")
      JWKSource<SecurityContext> mockJwkSource = mock(JWKSource.class);
      @SuppressWarnings("unchecked")
      JWKSourceBuilder<SecurityContext> mockBuilder = mock(JWKSourceBuilder.class);

      mockedBuilder.when(() -> JWKSourceBuilder.create(any(URL.class))).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockJwkSource);

      // Configure the mock to return our test key
      when(mockJwkSource.get(any(), any())).thenReturn(Arrays.asList(rsaKey));

      // Initialize validator with custom principal field
      Map<String, String> config = new HashMap<>();
      config.put(
          "gravitino.authenticator.oauth.jwksUri", "https://test-jwks.com/.well-known/jwks.json");
      config.put("gravitino.authenticator.oauth.authority", "https://test-issuer.com");
      config.put(
          "gravitino.authenticator.oauth.principalFields",
          "client_id"); // Use client_id instead of default 'sub'
      config.put("gravitino.authenticator.oauth.allowSkewSecs", "120");

      validator.initialize(createConfig(config));
      Principal result = validator.validateToken(tokenString, "test-service");

      assertNotNull(result);
      assertEquals("custom-client-123", result.getName()); // Should be client_id value, not subject
      assertEquals(UserPrincipal.class, result.getClass());
    }
  }

  @Test
  public void testPrincipalFieldFallback() throws Exception {
    // Generate a test RSA key pair
    RSAKey rsaKey =
        new RSAKeyGenerator(2048).keyID("test-key-id").algorithm(JWSAlgorithm.RS256).generate();

    // Create a signed JWT token without the first configured principal field, but with other
    // configured fields
    JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .subject("fallback-subject")
            .claim(
                "email", "user@example.com") // This should be used as fallback from configuration
            .audience("test-service")
            .issuer("https://test-issuer.com")
            .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
            .issueTime(Date.from(Instant.now()))
            .build();

    SignedJWT signedJWT =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key-id").build(), claimsSet);

    JWSSigner signer = new RSASSASigner(rsaKey);
    signedJWT.sign(signer);

    String tokenString = signedJWT.serialize();

    // Mock the JWKSourceBuilder to return our test key
    try (MockedStatic<JWKSourceBuilder> mockedBuilder = mockStatic(JWKSourceBuilder.class)) {
      @SuppressWarnings("unchecked")
      JWKSource<SecurityContext> mockJwkSource = mock(JWKSource.class);
      @SuppressWarnings("unchecked")
      JWKSourceBuilder<SecurityContext> mockBuilder = mock(JWKSourceBuilder.class);

      mockedBuilder.when(() -> JWKSourceBuilder.create(any(URL.class))).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockJwkSource);

      // Configure the mock to return our test key
      when(mockJwkSource.get(any(), any())).thenReturn(Arrays.asList(rsaKey));

      Map<String, String> config = new HashMap<>();
      config.put(
          "gravitino.authenticator.oauth.jwksUri", "https://test-jwks.com/.well-known/jwks.json");
      config.put("gravitino.authenticator.oauth.authority", "https://test-issuer.com");
      config.put(
          "gravitino.authenticator.oauth.principalFields",
          "non_existent_field,email"); // Should fall back to email since first field doesn't exist
      config.put("gravitino.authenticator.oauth.allowSkewSecs", "120");

      validator.initialize(createConfig(config));

      Principal result = validator.validateToken(tokenString, "test-service");

      assertNotNull(result);
      assertEquals("user@example.com", result.getName()); // Should use email as fallback
      assertEquals(UserPrincipal.class, result.getClass());
    }
  }

  @Test
  public void testValidateTokenWithMockJwks() throws Exception {
    // Generate a test RSA key pair
    RSAKey rsaKey =
        new RSAKeyGenerator(2048).keyID("test-key-id").algorithm(JWSAlgorithm.RS256).generate();

    // Create a signed JWT token
    JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .subject("test-user")
            .audience("test-service")
            .issuer("https://test-issuer.com")
            .expirationTime(Date.from(Instant.now().plusSeconds(3600)))
            .issueTime(Date.from(Instant.now()))
            .build();

    SignedJWT signedJWT =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key-id").build(), claimsSet);

    JWSSigner signer = new RSASSASigner(rsaKey);
    signedJWT.sign(signer);

    String tokenString = signedJWT.serialize();

    // Mock the JWKSourceBuilder to return our test key
    try (MockedStatic<JWKSourceBuilder> mockedBuilder = mockStatic(JWKSourceBuilder.class)) {
      @SuppressWarnings("unchecked")
      JWKSource<SecurityContext> mockJwkSource = mock(JWKSource.class);
      @SuppressWarnings("unchecked")
      JWKSourceBuilder<SecurityContext> mockBuilder = mock(JWKSourceBuilder.class);

      mockedBuilder.when(() -> JWKSourceBuilder.create(any(URL.class))).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockJwkSource);

      // Configure the mock to return our test key
      when(mockJwkSource.get(any(), any())).thenReturn(Arrays.asList(rsaKey));

      // Initialize validator
      Map<String, String> config = new HashMap<>();
      config.put(
          "gravitino.authenticator.oauth.jwksUri", "https://test-jwks.com/.well-known/jwks.json");
      config.put("gravitino.authenticator.oauth.authority", "https://test-issuer.com");
      config.put("gravitino.authenticator.oauth.principalFields", "sub");
      config.put("gravitino.authenticator.oauth.allowSkewSecs", "60");

      validator.initialize(createConfig(config));

      // Test token validation
      Principal result = validator.validateToken(tokenString, "test-service");

      assertNotNull(result);
      assertEquals("test-user", result.getName());
    }
  }

  @Test
  public void testValidateTokenWithInvalidToken() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    validator.initialize(createConfig(config));

    // Test with malformed JWT
    assertThrows(
        UnauthorizedException.class,
        () -> validator.validateToken("invalid.jwt.token", "test-service"));
  }

  @Test
  public void testValidateTokenWithNullAudience() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    validator.initialize(createConfig(config));

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken("some.jwt.token", null));
  }

  @Test
  public void testPostInitializationState() {
    Map<String, String> properties = new HashMap<>();
    properties.put("gravitino.authenticator.oauth.jwksUri", validJwksUri);
    properties.put("gravitino.authenticator.oauth.authority", "https://login.microsoftonline.com");
    properties.put("gravitino.authenticator.oauth.principalFields", "custom_field");
    properties.put("gravitino.authenticator.oauth.allowSkewSecs", "300");

    validator.initialize(createConfig(properties));

    // After initialization, the validator should be ready to validate tokens
    // We can test this by trying to validate a token and checking the error type
    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () -> validator.validateToken("invalid.token", "test-service"));

    // The exception should indicate JWT parsing/validation error, not initialization error
    assertNotNull(exception.getMessage());
    // The message should not contain "not initialized" or similar
    assertTrue(exception.getMessage().toLowerCase().contains("jwt"));
  }
}
