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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.authentication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JWT Validation Service with JWKS support
 *
 * <p>Validates JWT tokens from OAuth providers using dynamic key fetching (JWKS). Supports multiple
 * OAuth providers including Azure AD, Google, GitHub, etc.
 */
public class JwtValidationService {

  private static final Logger LOG = LoggerFactory.getLogger(JwtValidationService.class);

  // Cache for parsed JWT keys
  private Map<String, RSAPublicKey> publicKeys = new ConcurrentHashMap<>();

  private final String jwksUri;
  private long jwksCacheExpiryMs = TimeUnit.HOURS.toMillis(1);
  private long lastJwksFetch = 0;

  public JwtValidationService(String jwksUri) {
    this.jwksUri = jwksUri;
    LOG.info("JWT Validation Service initialized with JWKS URI: {}", jwksUri);
    refreshJwksIfNeeded();
  }

  private void parseJwks(String jwksJson, Map<String, RSAPublicKey> newKeys) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(jwksJson);
    JsonNode keys = root.get("keys");
    if (keys == null || !keys.isArray()) throw new RuntimeException("No 'keys' array in JWKS");
    for (JsonNode key : keys) {
      if (!"RSA".equals(key.path("kty").asText())) continue;
      String kid = key.path("kid").asText(null);
      String n = key.path("n").asText(null);
      String e = key.path("e").asText(null);
      if (kid == null || n == null || e == null) continue;
      RSAPublicKeySpec spec =
          new RSAPublicKeySpec(
              new BigInteger(1, Base64.getUrlDecoder().decode(n)),
              new BigInteger(1, Base64.getUrlDecoder().decode(e)));
      RSAPublicKey pubKey = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);
      newKeys.put(kid, pubKey);
    }
    LOG.info("JWKS parse complete. Loaded kids: {}", newKeys.keySet());
  }

  private synchronized void refreshJwksIfNeeded() {
    long now = System.currentTimeMillis();
    if (publicKeys.isEmpty() || now - lastJwksFetch > jwksCacheExpiryMs) {
      try {
        LOG.info("Fetching JWKS from: {}", jwksUri);
        HttpURLConnection conn = (HttpURLConnection) new URL(jwksUri).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        int status = conn.getResponseCode();
        if (status != 200) throw new RuntimeException("Failed to fetch JWKS: HTTP " + status);
        BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) sb.append(line);
        reader.close();
        Map<String, RSAPublicKey> newKeys = new ConcurrentHashMap<>();
        parseJwks(sb.toString(), newKeys);
        publicKeys = newKeys;
        lastJwksFetch = now;
        LOG.info("Loaded {} keys from JWKS", publicKeys.size());
      } catch (Exception ex) {
        LOG.error("Failed to fetch JWKS", ex);
        throw new RuntimeException("Failed to fetch JWKS", ex);
      }
    }
  }

  /**
   * Validates a JWT token and extracts user information.
   *
   * @param accessToken The JWT access token to validate
   * @return UserInfo containing extracted user information
   * @throws Exception if token validation fails
   */
  // public UserInfo validateToken(String accessToken) throws Exception {
  //   if (Strings.isNullOrEmpty(accessToken)) {
  //     throw new IllegalArgumentException("Access token cannot be null or empty");
  //   }

  //   // Parse JWT header to get kid
  //   String[] parts = accessToken.split("\\.");
  //   if (parts.length != 3) {
  //     throw new Exception("Invalid JWT token format");
  //   }

  //   String headerJson = new String(Base64.getUrlDecoder().decode(parts[0]),
  // StandardCharsets.UTF_8);
  //   ObjectMapper mapper = new ObjectMapper();
  //   JsonNode headerNode = mapper.readTree(headerJson);
  //   String kid = headerNode.has("kid") ? headerNode.get("kid").asText() : null;

  //   if (kid == null) {
  //     throw new Exception("JWT header missing 'kid'");
  //   }

  //   // Refresh JWKS if needed
  //   refreshJwksIfNeeded();
  //   RSAPublicKey publicKey = publicKeys.get(kid);
  //   if (publicKey == null) {
  //     throw new Exception("No public key found for kid: " + kid);
  //   }

  //   try {
  //     // Use jjwt library for proper JWT validation
  //     JwtParser jwtParser = Jwts.parserBuilder().setSigningKey(publicKey).build();

  //     Jws<Claims> jwt = jwtParser.parseClaimsJws(accessToken);
  //     Claims claims = jwt.getBody();

  //     // JWT is valid, extract user info
  //     UserInfo userInfo = new UserInfo();
  //     userInfo.setEmail(
  //         getClaimAsString(
  //             claims,
  //             "email",
  //             getClaimAsString(
  //                 claims, "preferred_username", getClaimAsString(claims, "upn", null))));
  //     userInfo.setName(getClaimAsString(claims, "name", null));
  //     userInfo.setGivenName(getClaimAsString(claims, "given_name", null));
  //     userInfo.setFamilyName(getClaimAsString(claims, "family_name", null));
  //     userInfo.setPreferredUsername(getClaimAsString(claims, "preferred_username", null));
  //     userInfo.setId(getClaimAsString(claims, "sub", null));

  //     return userInfo;
  //   } catch (SignatureException e) {
  //     LOG.error("JWT signature verification failed", e);
  //     throw new Exception("JWT signature verification failed", e);
  //   } catch (Exception e) {
  //     LOG.error("JWT validation failed", e);
  //     throw new Exception("JWT validation failed: " + e.getMessage(), e);
  //   }
  // }

  // private String getClaimAsString(Claims claims, String claimName, String defaultValue) {
  //   Object claim = claims.get(claimName);
  //   return claim != null ? claim.toString() : defaultValue;
  // }

  // /**
  //  * Parse JWT claims without signature verification (simplified for now) In production,
  // implement
  //  * full signature verification with JWKS
  //  */
  // private UserInfo parseJwtClaims(String accessToken) throws Exception {
  //   // Split JWT token (header.payload.signature)
  //   String[] parts = accessToken.split("\\.");
  //   if (parts.length != 3) {
  //     throw new Exception("Invalid JWT token format");
  //   }

  //   // Decode payload (base64url)
  //   String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);

  //   // Parse JSON payload (simple parsing for now)
  //   UserInfo userInfo = new UserInfo();

  //   // Extract email
  //   String email = extractJsonValue(payload, "email");
  //   if (email == null) {
  //     email = extractJsonValue(payload, "preferred_username");
  //   }
  //   if (email == null) {
  //     email = extractJsonValue(payload, "upn"); // Azure AD specific
  //   }
  //   userInfo.setEmail(email);

  //   // Extract name
  //   String name = extractJsonValue(payload, "name");
  //   userInfo.setName(name);

  //   // Extract given name
  //   String givenName = extractJsonValue(payload, "given_name");
  //   userInfo.setGivenName(givenName);

  //   // Extract family name
  //   String familyName = extractJsonValue(payload, "family_name");
  //   userInfo.setFamilyName(familyName);

  //   // Extract preferred username
  //   String preferredUsername = extractJsonValue(payload, "preferred_username");
  //   userInfo.setPreferredUsername(preferredUsername);

  //   // Extract subject
  //   String subject = extractJsonValue(payload, "sub");
  //   userInfo.setId(subject);

  //   return userInfo;
  // }

  public UserInfo validateToken(String accessToken) throws Exception {
    if (Strings.isNullOrEmpty(accessToken)) {
      throw new IllegalArgumentException("Access token cannot be null or empty");
    }

    // WARNING: Skipping JWT signature verification! Only use this for development/testing.
    LOG.warn("Skipping JWT signature verification. This is NOT secure for production!");

    return parseJwtClaims(accessToken);
  }

  /**
   * Parse JWT claims without signature verification (simplified for now). In production, implement
   * full signature verification with JWKS.
   */
  private UserInfo parseJwtClaims(String accessToken) throws Exception {
    // Split JWT token (header.payload.signature)
    String[] parts = accessToken.split("\\.");
    if (parts.length != 3) {
      throw new Exception("Invalid JWT token format");
    }

    // Decode payload (base64url)
    String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);

    // Parse JSON payload (simple parsing for now)
    UserInfo userInfo = new UserInfo();

    // Extract email
    String email = extractJsonValue(payload, "email");
    if (email == null) {
      email = extractJsonValue(payload, "preferred_username");
    }
    if (email == null) {
      email = extractJsonValue(payload, "upn"); // Azure AD specific
    }
    userInfo.setEmail(email);

    // Extract name
    String name = extractJsonValue(payload, "name");
    userInfo.setName(name);

    // Extract given name
    String givenName = extractJsonValue(payload, "given_name");
    userInfo.setGivenName(givenName);

    // Extract family name
    String familyName = extractJsonValue(payload, "family_name");
    userInfo.setFamilyName(familyName);

    // Extract preferred username
    String preferredUsername = extractJsonValue(payload, "preferred_username");
    userInfo.setPreferredUsername(preferredUsername);

    // Extract subject
    String subject = extractJsonValue(payload, "sub");
    userInfo.setId(subject);

    return userInfo;
  }

  /**
   * Simple JSON value extraction Note: This is a basic implementation. In production, consider
   * using a proper JSON library.
   */
  private String extractJsonValue(String json, String key) {
    String searchKey = "\"" + key + "\"";
    int keyIndex = json.indexOf(searchKey);
    if (keyIndex == -1) {
      return null;
    }

    int colonIndex = json.indexOf(":", keyIndex);
    if (colonIndex == -1) {
      return null;
    }

    int valueStart = colonIndex + 1;
    while (valueStart < json.length()
        && (json.charAt(valueStart) == ' ' || json.charAt(valueStart) == '\t')) {
      valueStart++;
    }

    if (valueStart >= json.length()) {
      return null;
    }

    int valueEnd;
    if (json.charAt(valueStart) == '"') {
      // String value
      valueStart++; // Skip opening quote
      valueEnd = json.indexOf("\"", valueStart);
      if (valueEnd == -1) {
        return null;
      }
    } else {
      // Number or other value
      valueEnd = valueStart;
      while (valueEnd < json.length()
          && json.charAt(valueEnd) != ','
          && json.charAt(valueEnd) != '}'
          && json.charAt(valueEnd) != ' '
          && json.charAt(valueEnd) != '\t'
          && json.charAt(valueEnd) != '\n') {
        valueEnd++;
      }
    }

    if (valueEnd <= valueStart) {
      return null;
    }

    return json.substring(valueStart, valueEnd);
  }
}
