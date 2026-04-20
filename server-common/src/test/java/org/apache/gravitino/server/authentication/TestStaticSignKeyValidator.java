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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.Key;
import java.security.KeyPair;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.exceptions.TokenExpiredException;
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
        TokenExpiredException.class, () -> validator.validateToken(token, serviceAudience));
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

  @Test
  public void testValidateTokenWithEcdsaSignature() {
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.ES256);
    Map<String, String> config = createBaseConfig();
    config.put(
        "gravitino.authenticator.oauth.defaultSignKey",
        Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded()));
    config.put("gravitino.authenticator.oauth.signAlgorithmType", "ES256");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("test-user")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.ES256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("test-user", principal.getName());
  }

  @Test
  public void testPrincipalMapperWithDefaultPattern() {
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalMapper", "regex");
    config.put("gravitino.authenticator.oauth.principalMapper.regex.pattern", "(.*)");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("user@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("user@example.com", principal.getName());
  }

  @Test
  public void testPrincipalMapperExtractEmailLocalPart() {
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalMapper", "regex");
    config.put("gravitino.authenticator.oauth.principalMapper.regex.pattern", "([^@]+)@.*");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("john.doe@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("john.doe", principal.getName());
  }

  @Test
  public void testPrincipalMapperNoMatch() {
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalMapper", "regex");
    config.put("gravitino.authenticator.oauth.principalMapper.regex.pattern", "([^@]+)@.*");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("plainuser")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("plainuser", principal.getName());
  }

  @Test
  public void testPrincipalMapperKerberosPrincipal() {
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalMapper", "regex");
    config.put("gravitino.authenticator.oauth.principalMapper.regex.pattern", "([^/@]+).*");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("admin/host.example.com@EXAMPLE.COM")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("admin", principal.getName());
  }

  @Test
  public void testCustomPrincipalFieldsDefault() {
    // Test default behavior: uses "sub" claim
    Map<String, String> config = createBaseConfig();
    // Don't set principalFields, should default to ["sub"]
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("client_id", "custom-client-123")
            .claim("email", "user@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("default-subject", principal.getName());
  }

  @Test
  public void testCustomPrincipalFieldsSingleCustomField() {
    // Test using a single custom field
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalFields", "email");
    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("email", "user@example.com")
            .claim("client_id", "custom-client-123")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("user@example.com", principal.getName());
  }

  @Test
  public void testCustomPrincipalFieldsFallback() {
    // Test fallback: tries preferred_username first, then email, then sub
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalFields", "preferred_username,email,sub");
    validator.initialize(createConfig(config));

    // Token without preferred_username, should fall back to email
    String token1 =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("email", "user@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal1 = validator.validateToken(token1, serviceAudience);
    assertNotNull(principal1);
    assertEquals("user@example.com", principal1.getName());

    // Token with preferred_username, should use it
    String token2 =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("preferred_username", "john.doe")
            .claim("email", "user@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal2 = validator.validateToken(token2, serviceAudience);
    assertNotNull(principal2);
    assertEquals("john.doe", principal2.getName());

    // Token with only sub, should fall back to sub
    String token3 =
        Jwts.builder()
            .setSubject("only-subject")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal3 = validator.validateToken(token3, serviceAudience);
    assertNotNull(principal3);
    assertEquals("only-subject", principal3.getName());
  }

  @Test
  public void testCustomPrincipalFieldsNoMatch() {
    // Test when no configured field is present in token
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalFields", "preferred_username,email");
    validator.initialize(createConfig(config));

    // Token without any of the configured fields
    String token =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("client_id", "custom-client-123")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    assertThrows(
        UnauthorizedException.class, () -> validator.validateToken(token, serviceAudience));
  }

  @Test
  public void testPrincipalFieldsWithMapper() {
    // Test that principal fields and mapper work together
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.principalFields", "email");
    config.put("gravitino.authenticator.oauth.principalMapper", "regex");
    config.put(
        "gravitino.authenticator.oauth.principalMapper.regex.pattern",
        "([^@]+)@.*"); // Extract user ID part

    validator.initialize(createConfig(config));

    String token =
        Jwts.builder()
            .setSubject("default-subject")
            .claim("email", "john.doe@example.com")
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(3600)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("john.doe", principal.getName()); // Mapper extracted local part
  }

  @Test
  public void testValidateTokenWithGroups() {
    Map<String, String> config = createBaseConfig();
    config.put("gravitino.authenticator.oauth.groupsFields", "groups");
    validator.initialize(createConfig(config));

    // Create token with groups
    String token =
        Jwts.builder()
            .setSubject("test-user")
            .claim("groups", Arrays.asList("group1", "group2"))
            .setAudience(serviceAudience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(Instant.now().plusSeconds(315360000)))
            .signWith(hmacKey, SignatureAlgorithm.HS256)
            .compact();

    Principal principal = validator.validateToken(token, serviceAudience);
    assertNotNull(principal);
    assertEquals("test-user", principal.getName());

    // Check if principal is UserPrincipal and has groups
    UserPrincipal userPrincipal = assertInstanceOf(UserPrincipal.class, principal);
    assertEquals(2, userPrincipal.getGroups().size());
    assertTrue(userPrincipal.getGroups().stream().anyMatch(g -> g.getGroupname().equals("group1")));
    assertTrue(userPrincipal.getGroups().stream().anyMatch(g -> g.getGroupname().equals("group2")));
  }

  @Test
  public void testValidateRealKeycloakTokenWithGroups() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("gravitino.authenticator.oauth.groupsFields", "groups");
    config.put("gravitino.authenticator.oauth.groupMapper.regex.pattern", "^/(.*)");
    config.put("gravitino.authenticator.oauth.principalFields", "preferred_username");
    config.put("gravitino.authenticator.oauth.allowSkewSecs", "315360000");
    config.put(
        "gravitino.authenticator.oauth.defaultSignKey",
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0J4W"
            + "fjgnb/nyrkOuVefHoGu17pIpiUm/TaTeVKoZcYxphcL8HAg2jKTQYov4SvlWZ4wXHZ1FayPm5+e74W5vSGOgZzjyzMZKithGkYa"
            + "gbRuXbUeo4sw6f7t4BsAmgpAUgyBYE/1fdEghHtKHia1Er1grQVsvGVAAbYoFjh+OfcnRLYFAel+ADvX83RmsK16laxXpY+LeFk"
            + "jugOsCaD5mnGfgqQC96t9H4/QlsqJacg+9YImZWxUHtqN/itrV/VCdGVfywih6Dk9F2jxt4rL9++3xVdG/OrvFDtNW98xhKqgqF"
            + "iXLsjKPT5WmqzbGCaQi/29Cdu9QOmd/IxmVeRy0+wIDAQAB");
    config.put("gravitino.authenticator.oauth.serverUri", "http://localhost:8080");
    config.put(
        "gravitino.authenticator.oauth.tokenPath",
        "/realms/gravitinorealm/protocol/openid-connect/token");
    config.put("gravitino.authenticator.oauth.signAlgorithmType", "RS256");

    validator.initialize(createConfig(config));

    // {
    //  "exp": 1774280470,
    //  "iat": 1774280170,
    //  "jti": "bdc771ca-b186-49a9-a3cd-9f16f5e6f96a",
    //  "iss": "http://localhost:8080/realms/gravitinorealm",
    //  "aud": "gravitino-client",
    //  "sub": "9db45582-a08c-4721-be12-6e5905d37317",
    //  "typ": "ID",
    //  "azp": "gravitino-client",
    //  "sid": "1710a207-72d6-42f8-b5c7-2d2b5fa8e52a",
    //  "at_hash": "WMloUfdrj1EZdrz3LhemPA",
    //  "acr": "1",
    //  "email_verified": false,
    //  "name": "a b",
    //  "groups": [
    //    "/groupa",
    //    "/groupb"
    //  ],
    //  "preferred_username": "userb",
    //  "given_name": "a",
    //  "family_name": "b",
    //  "email": "sai@datastrato.com"
    //  }
    String token =
        "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIySHlKQVZ6STJvQlVqUXdZUWZEbEh5SW1Ec3dWUGh1akJmLUZfbjZIUm"
            + "s4In0.eyJleHAiOjE3NzQyODA0NzAsImlhdCI6MTc3NDI4MDE3MCwianRpIjoiYmRjNzcxY2EtYjE4Ni00OWE5LWEzY2QtOWYx"
            + "NmY1ZTZmOTZhIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwL3JlYWxtcy9ncmF2aXRpbm9yZWFsbSIsImF1ZCI6ImdyYX"
            + "ZpdGluby1jbGllbnQiLCJzdWIiOiI5ZGI0NTU4Mi1hMDhjLTQ3MjEtYmUxMi02ZTU5MDVkMzczMTciLCJ0eXAiOiJJRCIsImF6"
            + "cCI6ImdyYXZpdGluby1jbGllbnQiLCJzaWQiOiIxNzEwYTIwNy03MmQ2LTQyZjgtYjVjNy0yZDJiNWZhOGU1MmEiLCJhdF9oYX"
            + "NoIjoiV01sb1VmZHJqMUVaZHJ6M0xoZW1QQSIsImFjciI6IjEiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5hbWUiOiJhIGIi"
            + "LCJncm91cHMiOlsiL2dyb3VwYSIsIi9ncm91cGIiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoidXNlcmIiLCJnaXZlbl9uYW1lIj"
            + "oiYSIsImZhbWlseV9uYW1lIjoiYiIsImVtYWlsIjoic2FpQGRhdGFzdHJhdG8uY29tIn0.ZNYmcK1gFTONojvp5aWekp9eK89y"
            + "j7I1PszY4aK9y_dFF2Dp_Ec1K71LygCccem_mnopua2Ys92BwthgHrc4p1LoZ9nC98PIXrlhNw6BPazBELUgIol6HPzMQ18HvN"
            + "DO_K4pRzMK1khIcpTDgt5ikpw8-8i9KFAPyvO3ezKhIv5h4TQPdMV9RaAplP539hkuxSi4VhIlz6qYPuyoTW4QPmnqKbn7E0IE"
            + "EgVnpvQ7Y-xxoCstpFNhN_IikGDz7G7xQwBbYo4zvo4brixzRWOW-wWe9nLDTI9veYxs8Hs_ZG_JkhiCvecHNY811o_fgQ0Bz2"
            + "mWizL5ukKRnxXfL0oPVg";

    Principal principal = validator.validateToken(token, "gravitino-client");
    assertNotNull(principal);
    assertEquals("userb", principal.getName());

    // Check if principal is UserPrincipal and has groups
    UserPrincipal userPrincipal = assertInstanceOf(UserPrincipal.class, principal);
    assertEquals(2, userPrincipal.getGroups().size());
    assertTrue(userPrincipal.getGroups().stream().anyMatch(g -> g.getGroupname().equals("groupa")));
    assertTrue(userPrincipal.getGroups().stream().anyMatch(g -> g.getGroupname().equals("groupb")));
  }
}
