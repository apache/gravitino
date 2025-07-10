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

import com.google.common.base.Preconditions;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.Principal;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.SignatureAlgorithmFamilyType;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2TokenAuthenticator provides the OAuth 2.0 authentication mechanism.
 * OAuth2TokenAuthenticator only supports to validate the format of JWT's Bearer Token.
 */
class OAuth2TokenAuthenticator implements Authenticator {

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2TokenAuthenticator.class);

  // private long allowSkewSeconds;
  // private Key defaultSigningKey;
  // private String serviceAudience;

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    LOG.warn(
        "Skipping JWT signature verification in OAuth2TokenAuthenticator. NOT SECURE for production!");
    if (tokenData == null) {
      LOG.warn("Empty token authorization header");
      throw new UnauthorizedException("Empty token authorization header");
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER)) {
      LOG.warn("Invalid token authorization header: {}", authData);
      throw new UnauthorizedException("Invalid token authorization header");
    }
    String token = authData.substring(AuthConstants.AUTHORIZATION_BEARER_HEADER.length());
    if (StringUtils.isBlank(token)) {
      LOG.warn("Blank token found");
      throw new UnauthorizedException("Blank token found");
    }
    try {
      // Parse JWT payload without signature verification
      String[] parts = token.split("\\.");
      if (parts.length != 3) {
        throw new UnauthorizedException("Invalid JWT token format");
      }
      String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);

      String principalName = null;

      // 1. unique_name
      int uniqueNameIdx = payload.indexOf("\"unique_name\"");
      if (uniqueNameIdx != -1) {
        int colon = payload.indexOf(":", uniqueNameIdx);
        int quote1 = payload.indexOf("\"", colon + 1);
        int quote2 = payload.indexOf("\"", quote1 + 1);
        if (colon != -1 && quote1 != -1 && quote2 != -1) {
          principalName = payload.substring(quote1 + 1, quote2);
        }
      }

      // 2. upn
      if (principalName == null) {
        int upnIdx = payload.indexOf("\"upn\"");
        if (upnIdx != -1) {
          int colon = payload.indexOf(":", upnIdx);
          int quote1 = payload.indexOf("\"", colon + 1);
          int quote2 = payload.indexOf("\"", quote1 + 1);
          if (colon != -1 && quote1 != -1 && quote2 != -1) {
            principalName = payload.substring(quote1 + 1, quote2);
          }
        }
      }

      // 3. email
      if (principalName == null) {
        int emailIdx = payload.indexOf("\"email\"");
        if (emailIdx != -1) {
          int colon = payload.indexOf(":", emailIdx);
          int quote1 = payload.indexOf("\"", colon + 1);
          int quote2 = payload.indexOf("\"", quote1 + 1);
          if (colon != -1 && quote1 != -1 && quote2 != -1) {
            principalName = payload.substring(quote1 + 1, quote2);
          }
        }
      }

      // 4. name
      if (principalName == null) {
        int nameIdx = payload.indexOf("\"name\"");
        if (nameIdx != -1) {
          int colon = payload.indexOf(":", nameIdx);
          int quote1 = payload.indexOf("\"", colon + 1);
          int quote2 = payload.indexOf("\"", quote1 + 1);
          if (colon != -1 && quote1 != -1 && quote2 != -1) {
            principalName = payload.substring(quote1 + 1, quote2);
          }
        }
      }

      if (principalName == null) {
        throw new UnauthorizedException("No unique_name, upn, email, or name found in JWT payload");
      }
      LOG.info(
          "JWT authentication (no signature check) successful for principal: {}", principalName);
      return new UserPrincipal(principalName);
    } catch (Exception e) {
      LOG.error("JWT parse error (no signature check): {}", e.getMessage(), e);
      throw new UnauthorizedException(e, "JWT parse error (no signature check)");
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    // this.serviceAudience = config.get(OAuthConfig.SERVICE_AUDIENCE);
    // this.allowSkewSeconds = config.get(OAuthConfig.ALLOW_SKEW_SECONDS);
    // String configuredSignKey = config.get(OAuthConfig.DEFAULT_SIGN_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_TOKEN_PATH)),
        "The path for token of the default OAuth server can't be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_SERVER_URI)),
        "The uri of the default OAuth server can't be blank");
    // String algType = config.get(OAuthConfig.SIGNATURE_ALGORITHM_TYPE);
    // this.defaultSigningKey = decodeSignKey(Base64.getDecoder().decode(configuredSignKey),
    // algType);
  }

  @Override
  public boolean supportsToken(byte[] tokenData) {
    return tokenData != null
        && new String(tokenData, StandardCharsets.UTF_8)
            .startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER);
  }

  private static Key decodeSignKey(byte[] key, String algType) {
    try {
      SignatureAlgorithmFamilyType algFamilyType =
          SignatureAlgorithmFamilyType.valueOf(SignatureAlgorithm.valueOf(algType).getFamilyName());

      if (SignatureAlgorithmFamilyType.HMAC == algFamilyType) {
        return Keys.hmacShaKeyFor(key);
      } else if (SignatureAlgorithmFamilyType.RSA == algFamilyType
          || SignatureAlgorithmFamilyType.ECDSA == algFamilyType) {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
        KeyFactory kf = KeyFactory.getInstance(algFamilyType.name());
        return kf.generatePublic(spec);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to decode key", e);
    }
    throw new IllegalArgumentException("Unsupported signature algorithm type: " + algType);
  }
}
