/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.SignatureAlgorithmFamilyType;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
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
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.Principal;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * OAuth2TokenAuthenticator provides the OAuth 2.0 authentication mechanism.
 * OAuth2TokenAuthenticator only supports to validate the format of JWT's Bearer Token.
 */
class OAuth2TokenAuthenticator implements Authenticator {

  private long allowSkewSeconds;
  private Key defaultSigningKey;
  private String serviceAudience;

  @Override
  public boolean isDataFromToken() {
    return true;
  }

  @Override
  public Principal authenticateToken(byte[] tokenData) {
    if (tokenData == null) {
      throw new UnauthorizedException("Empty token authorization header");
    }
    String authData = new String(tokenData, StandardCharsets.UTF_8);
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(AuthConstants.AUTHORIZATION_BEARER_HEADER)) {
      throw new UnauthorizedException("Invalid token authorization header");
    }
    String token = authData.substring(AuthConstants.AUTHORIZATION_BEARER_HEADER.length());
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Blank token found");
    }
    // TODO: If we support multiple OAuth 2.0 servers, we should use multiple
    // signing keys.
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

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.serviceAudience = config.get(OAuthConfig.SERVICE_AUDIENCE);
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
