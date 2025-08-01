/*
* Licensed to the Apache Software Foundation (ASF) under one
* or mor    props.put(
       "gravitino.authenticator.oauth.tokenValidatorClass",
       "org.apache.gravitino.server.authentication.DefaultSignKeyValidator");

   Config config = createConfig(props);
   OAuthTokenValidator validator = OAuthTokenValidatorFactory.createValidator(config);
   assertNotNull(validator);
   assertTrue(validator instanceof DefaultSignKeyValidator);butor license agreements.  See the NOTICE file
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.Key;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOAuthTokenValidatorFactory {

  private String validSignKey;

  @BeforeEach
  public void setUp() {
    // Generate a secure key that meets JJWT requirements (256+ bits for HS256)
    Key hmacKey = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    validSignKey = Base64.getEncoder().encodeToString(hmacKey.getEncoded());
  }

  private Config createConfig(Map<String, String> properties) {
    Config config = new Config(false) {};
    config.loadFromMap(properties, k -> true);
    return config;
  }

  @Test
  public void testCreateDefaultValidator() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put("gravitino.authenticator.oauth.defaultSignKey", validSignKey);
    props.put("gravitino.authenticator.oauth.signAlgorithmType", "HS256");
    props.put("gravitino.authenticator.oauth.tokenPath", "/token");
    props.put("gravitino.authenticator.oauth.serverUri", "http://localhost:8080");

    Config config = createConfig(props);
    OAuthTokenValidator validator = OAuthTokenValidatorFactory.createValidator(config);
    assertNotNull(validator);
    assertTrue(validator instanceof StaticSignKeyValidator);
  }

  @Test
  public void testCreateJwksValidator() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put(
        "gravitino.authenticator.oauth.jwksUri",
        "https://login.microsoftonline.com/common/discovery/v2.0/keys");
    props.put(
        "gravitino.authenticator.oauth.tokenValidatorClass",
        "org.apache.gravitino.server.authentication.JwksTokenValidator");

    Config config = createConfig(props);
    OAuthTokenValidator validator = OAuthTokenValidatorFactory.createValidator(config);
    assertNotNull(validator);
    assertTrue(validator instanceof JwksTokenValidator);
  }

  @Test
  public void testCreateValidatorWithNullConfig() {
    assertThrows(
        IllegalArgumentException.class, () -> OAuthTokenValidatorFactory.createValidator(null));
  }

  @Test
  public void testJwksTakesPrecedenceOverDefaultKey() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put("gravitino.authenticator.oauth.defaultSignKey", validSignKey);
    props.put(
        "gravitino.authenticator.oauth.jwksUri",
        "https://login.microsoftonline.com/common/discovery/v2.0/keys");
    props.put(
        "gravitino.authenticator.oauth.tokenValidatorClass",
        "org.apache.gravitino.server.authentication.JwksTokenValidator");

    Config config = createConfig(props);
    // Should use JWKS validator since it's explicitly configured
    OAuthTokenValidator validator = OAuthTokenValidatorFactory.createValidator(config);
    assertNotNull(validator);
    assertTrue(validator instanceof JwksTokenValidator);
  }

  @Test
  public void testExplicitValidatorClassTakesPrecedence() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put("gravitino.authenticator.oauth.defaultSignKey", validSignKey);
    props.put("gravitino.authenticator.oauth.signAlgorithmType", "HS256");
    props.put("gravitino.authenticator.oauth.tokenPath", "/token");
    props.put("gravitino.authenticator.oauth.serverUri", "http://localhost:8080");
    props.put(
        "gravitino.authenticator.oauth.jwksUri",
        "https://login.microsoftonline.com/common/discovery/v2.0/keys");
    props.put(
        "gravitino.authenticator.oauth.tokenValidatorClass",
        "org.apache.gravitino.server.authentication.StaticSignKeyValidator");

    Config config = createConfig(props);
    // Should use explicit class even though JWKS URI is configured
    OAuthTokenValidator validator = OAuthTokenValidatorFactory.createValidator(config);
    assertNotNull(validator);
    assertTrue(validator instanceof StaticSignKeyValidator);
  }

  @Test
  public void testInvalidValidatorClass() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put("gravitino.authenticator.oauth.tokenValidatorClass", "com.invalid.NonExistentClass");

    Config config = createConfig(props);
    assertThrows(
        IllegalArgumentException.class, () -> OAuthTokenValidatorFactory.createValidator(config));
  }

  @Test
  public void testValidatorClassNotImplementingInterface() {
    Map<String, String> props = new HashMap<>();
    props.put("gravitino.authenticator.oauth.serviceAudience", "test-audience");
    props.put("gravitino.authenticator.oauth.tokenValidatorClass", "java.lang.String");

    Config config = createConfig(props);
    assertThrows(
        IllegalArgumentException.class, () -> OAuthTokenValidatorFactory.createValidator(config));
  }
}
