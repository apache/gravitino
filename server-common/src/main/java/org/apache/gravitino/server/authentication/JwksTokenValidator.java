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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.URL;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.GroupMapper;
import org.apache.gravitino.auth.GroupMapperFactory;
import org.apache.gravitino.auth.PrincipalMapper;
import org.apache.gravitino.auth.PrincipalMapperFactory;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic JWKS-based OAuth token validator that uses Nimbus JOSE + JWT library to validate JWT
 * tokens from any OAuth provider that exposes a JWKS endpoint.
 *
 * <p>This validator supports OAuth providers that publish their public keys via JWKS (JSON Web Key
 * Set) endpoints
 */
public class JwksTokenValidator implements OAuthTokenValidator {
  private static final Logger LOG = LoggerFactory.getLogger(JwksTokenValidator.class);

  private String jwksUri;
  private String expectedIssuer;
  private List<String> principalFields;
  private List<String> groupsFields;
  private long allowSkewSeconds;
  private PrincipalMapper principalMapper;
  private GroupMapper groupMapper;
  private JWKSource<SecurityContext> jwkSource;

  @Override
  public void initialize(Config config) {
    this.jwksUri = config.get(OAuthConfig.JWKS_URI);
    this.expectedIssuer = config.get(OAuthConfig.AUTHORITY);
    this.principalFields = config.get(OAuthConfig.PRINCIPAL_FIELDS);
    this.groupsFields = config.get(OAuthConfig.GROUPS_FIELDS);
    this.allowSkewSeconds = config.get(OAuthConfig.ALLOW_SKEW_SECONDS);

    // Create principal mapper based on configuration
    String mapperType = config.get(OAuthConfig.PRINCIPAL_MAPPER);
    String regexPattern = config.get(OAuthConfig.PRINCIPAL_MAPPER_REGEX_PATTERN);
    this.principalMapper = PrincipalMapperFactory.create(mapperType, regexPattern);

    // Create group mapper based on configuration
    String groupMapperType = config.get(OAuthConfig.GROUP_MAPPER);
    String groupRegexPattern = config.get(OAuthConfig.GROUP_MAPPER_REGEX_PATTERN);
    this.groupMapper = GroupMapperFactory.create(groupMapperType, groupRegexPattern);

    LOG.info("Initializing JWKS token validator");

    if (StringUtils.isBlank(jwksUri)) {
      throw new IllegalArgumentException(
          "JWKS URI must be configured when using JWKS-based OAuth providers");
    }

    // Create the JWK source once at initialization. JWKSourceBuilder.create(url).build() enables
    // rate-limiting (min 30 s between URL fetches) and caching with refresh-ahead by default:
    //   - Cache TTL: 5 minutes (DEFAULT_CACHE_TIME_TO_LIVE)
    //   - Refresh-ahead: 30 seconds before expiration on a background thread
    // The Nimbus library handles key rotation transparently within these defaults.
    try {
      this.jwkSource = JWKSourceBuilder.create(new URL(jwksUri)).build();
    } catch (Exception e) {
      LOG.error("Failed to create JWKS source from URI: {}", jwksUri, e);
      throw new IllegalArgumentException(
          "Invalid JWKS URI or failed to create JWKS source: " + jwksUri, e);
    }
  }

  @Override
  public Principal validateToken(String token, String serviceAudience) {
    if (token == null || token.trim().isEmpty()) {
      LOG.error("Token is null or empty");
      throw new UnauthorizedException("Token cannot be null or empty");
    }

    if (serviceAudience == null || serviceAudience.trim().isEmpty()) {
      LOG.error("Service audience is null or empty");
      throw new UnauthorizedException("Service audience cannot be null or empty");
    }

    SignedJWT signedJWT = null;
    try {
      signedJWT = SignedJWT.parse(token);

      // Set up JWKS source and processor
      JWSAlgorithm algorithm = JWSAlgorithm.parse(signedJWT.getHeader().getAlgorithm().getName());
      JWSKeySelector<SecurityContext> keySelector =
          new JWSVerificationKeySelector<>(algorithm, jwkSource);

      DefaultJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
      jwtProcessor.setJWSKeySelector(keySelector);

      // Audience validation per RFC 7519 (at-least-one match)
      Set<String> acceptedAudiences = null;
      if (StringUtils.isNotBlank(serviceAudience)) {
        acceptedAudiences = Collections.singleton(serviceAudience);
      }

      // Build exact match claims for issuer validation
      JWTClaimsSet.Builder exactMatchBuilder = new JWTClaimsSet.Builder();
      if (StringUtils.isNotBlank(expectedIssuer)) {
        exactMatchBuilder.issuer(expectedIssuer);
      }
      JWTClaimsSet exactMatchClaims = exactMatchBuilder.build();

      DefaultJWTClaimsVerifier<SecurityContext> claimsVerifier =
          new DefaultJWTClaimsVerifier<>(acceptedAudiences, exactMatchClaims, null, null);

      // Set clock skew tolerance
      claimsVerifier.setMaxClockSkew((int) allowSkewSeconds);
      jwtProcessor.setJWTClaimsSetVerifier(claimsVerifier);

      // Validate token signature and claims
      JWTClaimsSet validatedClaims = jwtProcessor.process(signedJWT, null);

      String principal = extractPrincipal(validatedClaims);
      if (principal == null) {
        LOG.error("No valid principal found in token");
        throw new UnauthorizedException("No valid principal found in token");
      }

      // Use principal mapper to extract username
      Principal userPrincipal = principalMapper.map(principal);
      List<Object> groups = extractGroups(validatedClaims);
      if (groups != null && !groups.isEmpty()) {
        List<UserGroup> mappedGroups = groupMapper.map(groups);
        return new UserPrincipal(userPrincipal.getName(), mappedGroups);
      }
      return userPrincipal;

    } catch (Exception e) {
      LOG.warn(
          "JWKS JWT validation failed for principal [{}]: {}",
          extractPrincipalForLogging(signedJWT),
          e.getMessage());
      throw new UnauthorizedException(e, "JWKS JWT validation error");
    }
  }

  /**
   * Extracts the principal from a parsed (but not yet verified) JWT for diagnostic logging. Safe to
   * call with a null or invalid JWT.
   */
  String extractPrincipalForLogging(SignedJWT signedJWT) {
    if (signedJWT == null) {
      return "unknown";
    }
    try {
      String principal = extractPrincipal(signedJWT.getJWTClaimsSet());
      return principal != null ? principal : "unknown";
    } catch (Exception ex) {
      return "unknown";
    }
  }

  /** Extracts the principal from the validated JWT claims using configured field(s). */
  private String extractPrincipal(JWTClaimsSet validatedClaims) {
    // Try the principal field(s) one by one in order
    if (principalFields != null && !principalFields.isEmpty()) {
      for (String field : principalFields) {
        if (StringUtils.isNotBlank(field)) {
          Object principal = validatedClaims.getClaim(field);
          if (principal != null) {
            return principal.toString();
          }
        }
      }
    }

    return null;
  }

  /** Extracts the groups from the validated JWT claims using configured field(s). */
  private List<Object> extractGroups(JWTClaimsSet validatedClaims) {
    if (groupsFields != null && !groupsFields.isEmpty()) {
      for (String field : groupsFields) {
        if (StringUtils.isNotBlank(field)) {
          try {
            Object groupsObj = validatedClaims.getClaim(field);
            if (groupsObj instanceof List) {
              return (List<Object>) groupsObj;
            }
          } catch (Exception e) {
            LOG.warn("Failed to parse groups from claim field: {}", field, e);
          }
        }
      }
    }

    return null;
  }
}
