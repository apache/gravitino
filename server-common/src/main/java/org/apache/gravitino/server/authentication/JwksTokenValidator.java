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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
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
  private long allowSkewSeconds;

  @Override
  public void initialize(Config config) {
    this.jwksUri = config.get(OAuthConfig.JWKS_URI);
    this.expectedIssuer = config.get(OAuthConfig.AUTHORITY);
    this.principalFields = config.get(OAuthConfig.PRINCIPAL_FIELDS);
    this.allowSkewSeconds = config.get(OAuthConfig.ALLOW_SKEW_SECONDS);

    LOG.info("Initializing JWKS token validator");

    if (StringUtils.isBlank(jwksUri)) {
      throw new IllegalArgumentException(
          "JWKS URI must be configured when using JWKS-based OAuth providers");
    }

    // Validate JWKS URI format
    try {
      new URL(jwksUri);
    } catch (Exception e) {
      LOG.error("Invalid JWKS URI format: {}", jwksUri);
      throw new IllegalArgumentException("Invalid JWKS URI format: " + jwksUri, e);
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

    try {
      SignedJWT signedJWT = SignedJWT.parse(token);

      // Set up JWKS source and processor
      JWKSource<SecurityContext> jwkSource = createJwkSource();
      JWSAlgorithm algorithm = JWSAlgorithm.parse(signedJWT.getHeader().getAlgorithm().getName());
      JWSKeySelector<SecurityContext> keySelector =
          new JWSVerificationKeySelector<>(algorithm, jwkSource);

      DefaultJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
      jwtProcessor.setJWSKeySelector(keySelector);

      // Configure claims verification
      JWTClaimsSet.Builder expectedClaimsBuilder = new JWTClaimsSet.Builder();

      // Set expected issuer if configured
      if (StringUtils.isNotBlank(expectedIssuer)) {
        expectedClaimsBuilder.issuer(expectedIssuer);
      }

      // Set expected audience if provided
      if (StringUtils.isNotBlank(serviceAudience)) {
        expectedClaimsBuilder.audience(serviceAudience);
      }

      DefaultJWTClaimsVerifier<SecurityContext> claimsVerifier =
          new DefaultJWTClaimsVerifier<SecurityContext>(expectedClaimsBuilder.build(), null);

      // Set clock skew tolerance
      claimsVerifier.setMaxClockSkew((int) allowSkewSeconds);
      jwtProcessor.setJWTClaimsSetVerifier(claimsVerifier);

      // Process and validate the token
      JWTClaimsSet validatedClaims = jwtProcessor.process(signedJWT, null);

      String principal = extractPrincipal(validatedClaims);
      if (principal == null) {
        LOG.error("No valid principal found in token");
        throw new UnauthorizedException("No valid principal found in token");
      }

      return new UserPrincipal(principal);

    } catch (Exception e) {
      LOG.error("JWKS JWT validation error: {}", e.getMessage());
      throw new UnauthorizedException(e, "JWKS JWT validation error");
    }
  }

  /** Creates a JWK source from the configured JWKS URI. */
  private JWKSource<SecurityContext> createJwkSource() throws Exception {
    try {
      return JWKSourceBuilder.create(new URL(jwksUri)).build();
    } catch (Exception e) {
      LOG.error("Failed to create JWKS source from URI: {}", jwksUri, e);
      throw new Exception("Failed to create JWKS source: " + e.getMessage(), e);
    }
  }

  /** Extracts the principal from the validated JWT claims using configured field(s). */
  private String extractPrincipal(JWTClaimsSet validatedClaims) {
    // Try the principal field(s) one by one in order
    if (principalFields != null && !principalFields.isEmpty()) {
      for (String field : principalFields) {
        if (StringUtils.isNotBlank(field)) {
          String principal = (String) validatedClaims.getClaim(field);
          if (principal != null) {
            return principal;
          }
        }
      }
    }

    return null;
  }
}
