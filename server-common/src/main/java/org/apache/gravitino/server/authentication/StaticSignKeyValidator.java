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
import org.apache.gravitino.auth.SignatureAlgorithmFamilyType;
import org.apache.gravitino.exceptions.UnauthorizedException;

/**
 * Static OAuth token validator that uses a pre-configured signing key for JWT validation.
 *
 * <p>This validator is designed for JWT validation scenarios where a single signing key is shared
 * between the token issuer and Gravitino. It uses the configured {@code defaultSignKey} and {@code
 * signAlgorithmType} for validation.
 */
public class StaticSignKeyValidator implements OAuthTokenValidator {
  private long allowSkewSeconds;
  private Key defaultSigningKey;

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
      return new UserPrincipal(jwt.getBody().getSubject());
    } catch (ExpiredJwtException
        | UnsupportedJwtException
        | MalformedJwtException
        | SignatureException
        | IllegalArgumentException e) {
      throw new UnauthorizedException(e, "JWT parse error");
    }
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
