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
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.Principal;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.GroupMapper;
import org.apache.gravitino.auth.GroupMapperFactory;
import org.apache.gravitino.auth.PrincipalMapper;
import org.apache.gravitino.auth.PrincipalMapperFactory;
import org.apache.gravitino.auth.SignatureAlgorithmFamilyType;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static OAuth token validator that uses a pre-configured signing key for JWT validation.
 *
 * <p>This validator is designed for JWT validation scenarios where a single signing key is shared
 * between the token issuer and Gravitino. It uses the configured {@code defaultSignKey} and {@code
 * signAlgorithmType} for validation.
 */
public class StaticSignKeyValidator implements OAuthTokenValidator {
  private static final Logger LOG = LoggerFactory.getLogger(StaticSignKeyValidator.class);

  private long allowSkewSeconds;
  private Key defaultSigningKey;
  private PrincipalMapper principalMapper;
  private List<String> principalFields;
  private GroupMapper groupMapper;
  private List<String> groupFields;

  @Override
  public void initialize(Config config) {
    this.allowSkewSeconds = config.get(OAuthConfig.ALLOW_SKEW_SECONDS);
    String configuredSignKey = config.get(OAuthConfig.DEFAULT_SIGN_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_TOKEN_PATH)),
        "The path for token of the default OAuth server can't be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.get(OAuthConfig.DEFAULT_SERVER_URI)),
        "The uri of the default OAuth server can't be blank");
    String algType = config.get(OAuthConfig.SIGNATURE_ALGORITHM_TYPE);
    this.defaultSigningKey = decodeSignKey(Base64.getDecoder().decode(configuredSignKey), algType);
    this.principalFields = config.get(OAuthConfig.PRINCIPAL_FIELDS);
    this.groupFields = config.get(OAuthConfig.GROUP_FIELDS);

    // Create principal mapper based on configuration
    String mapperType = config.get(OAuthConfig.PRINCIPAL_MAPPER);
    String regexPattern = config.get(OAuthConfig.PRINCIPAL_MAPPER_REGEX_PATTERN);
    this.principalMapper = PrincipalMapperFactory.create(mapperType, regexPattern);

    // Create group mapper based on configuration
    String groupMapperType = config.get(OAuthConfig.GROUP_MAPPER);
    String groupRegexPattern = config.get(OAuthConfig.GROUP_MAPPER_REGEX_PATTERN);
    this.groupMapper = GroupMapperFactory.create(groupMapperType, groupRegexPattern);
  }

  @Override
  public Principal validateToken(String token, String serviceAudience) {
    try {
      JwtParser parser =
          Jwts.parserBuilder()
              .setAllowedClockSkewSeconds(allowSkewSeconds)
              .setSigningKey(defaultSigningKey)
              .build();
      Jwt<?, Claims> jwt = parser.parseClaimsJws(token);
      Object audienceObject = jwt.getBody().get(Claims.AUDIENCE);
      if (audienceObject == null) {
        throw new UnauthorizedException("Found null Audience in token");
      }
      if (audienceObject instanceof String) {
        if (!serviceAudience.equals(audienceObject)) {
          throw new UnauthorizedException(
              "Audience in the token [%s] doesn't contain %s", audienceObject, serviceAudience);
        }
      } else if (audienceObject instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> audiences = (List<Object>) audienceObject;
        if (audiences.stream()
            .noneMatch(audienceInToken -> audienceInToken.equals(serviceAudience))) {
          throw new UnauthorizedException(
              "Audiences in the token %s don't contain %s", audienceObject, serviceAudience);
        }
      } else {
        throw new UnauthorizedException(
            "Audiences in token is not in expected format: %s", audienceObject);
      }

      // Extract principal from JWT claims using configured field(s)
      String principal = extractPrincipal(jwt.getBody());

      // Use principal mapper to extract username
      Principal userPrincipal = principalMapper.map(principal);
      List<String> groups = extractGroups(jwt.getBody());
      if (groups != null && !groups.isEmpty()) {
        List<String> mappedGroups = groupMapper.map(groups);
        return new UserPrincipal(userPrincipal.getName(), mappedGroups);
      }
      return userPrincipal;
    } catch (ExpiredJwtException
        | UnsupportedJwtException
        | MalformedJwtException
        | SignatureException
        | IllegalArgumentException e) {
      throw new UnauthorizedException(e, "JWT parse error");
    }
  }

  /** Extracts the principal from the JWT claims using configured field(s). */
  private String extractPrincipal(Claims claims) {
    // Try the principal field(s) one by one in order
    if (principalFields != null && !principalFields.isEmpty()) {
      for (String field : principalFields) {
        if (StringUtils.isNotBlank(field)) {
          Object claimValue = claims.get(field);
          if (claimValue != null) {
            return claimValue.toString();
          }
        }
      }
    }

    throw new UnauthorizedException(
        "No valid principal found in token. Checked fields: %s", principalFields);
  }

  /** Extracts the groups from the validated JWT claims using configured field(s). */
  private List<String> extractGroups(Claims claims) {
    if (groupFields != null && !groupFields.isEmpty()) {
      for (String field : groupFields) {
        if (StringUtils.isNotBlank(field)) {
          try {
            Object groups = claims.get(field);
            if (groups instanceof List) {
              return (List<String>) groups;
            }
          } catch (Exception e) {
            LOG.warn("Failed to parse groups from claim field: {}", field, e);
          }
        }
      }
    }

    return null;
  }

  private static Key decodeSignKey(byte[] key, String algType) {
    try {
      SignatureAlgorithmFamilyType algFamilyType =
          SignatureAlgorithmFamilyType.valueOf(SignatureAlgorithm.valueOf(algType).getFamilyName());

      return generateKeyByFamilyType(algFamilyType, key, algType);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to decode key", e);
    }
  }

  private static Key generateKeyByFamilyType(
      SignatureAlgorithmFamilyType algFamilyType, byte[] key, String algType) throws Exception {

    switch (algFamilyType) {
      case RSA:
        return KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(key));

      case ECDSA:
        return KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(key));

      case HMAC:
        return Keys.hmacShaKeyFor(key);

      default:
        throw new IllegalArgumentException("Unsupported signature algorithm type: " + algType);
    }
  }
}
