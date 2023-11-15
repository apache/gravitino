/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
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
import java.security.Key;
import java.security.KeyFactory;
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
  public boolean isDataFromHTTP() {
    return true;
  }

  @Override
  public String authenticateHTTPHeader(String authData) {
    if (StringUtils.isBlank(authData)
        || !authData.startsWith(Constants.HTTP_HEADER_AUTHORIZATION_BEARER)) {
      throw new UnauthorizedException("Invalid HTTP Authorization header");
    }
    String token = authData.substring(Constants.HTTP_HEADER_AUTHORIZATION_BEARER.length());
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Blank token found");
    }
    // TODO: If we support multiple OAuth 2.0 server, we should use multiple
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
              "Audience in the token [" + audienceObject + "] doesn't contain " + serviceAudience);
        }
      } else if (audienceObject instanceof List) {
        List<Object> audiences = (List<Object>) audienceObject;
        if (audiences.stream()
            .noneMatch(audienceInToken -> audienceInToken.equals(serviceAudience))) {
          throw new UnauthorizedException(
              "Audiences in the token " + audienceObject + " don't contain " + serviceAudience);
        }
      } else {
        throw new UnauthorizedException(
            "Audiences in token is not in expected format: " + audienceObject);
      }
      return jwt.getBody().getSubject();
    } catch (ExpiredJwtException
        | UnsupportedJwtException
        | MalformedJwtException
        | SignatureException
        | IllegalArgumentException e) {
      throw new UnauthorizedException("JWT parse error", e);
    }
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.serviceAudience = config.get(Configs.SERVICE_AUDIENCE);
    this.allowSkewSeconds = config.get(Configs.ALLOW_SKEW_SECONDS);
    String configuredSignKey = config.get(Configs.DEFAULT_SIGN_KEY);
    Preconditions.checkNotNull(configuredSignKey, "Default signing key can't be null");
    String algType = config.get(Configs.SIGNATURE_ALGORITHM_TYPE);
    this.defaultSigningKey = decodeSignKey(Base64.getDecoder().decode(configuredSignKey), algType);
  }

  private static Key decodeSignKey(byte[] key, String algType) {
    try {
      SignatureAlgorithmFamilyType algFamilyType =
          SignatureAlgorithmFamilyType.valueOf(SignatureAlgorithm.valueOf(algType).getFamilyName());

      if (SignatureAlgorithmFamilyType.HMAC.equals(algFamilyType)) {
        return Keys.hmacShaKeyFor(key);
      } else if (SignatureAlgorithmFamilyType.RSA.equals(algFamilyType)
          || SignatureAlgorithmFamilyType.ECDSA.equals(algFamilyType)) {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(key);
        KeyFactory kf = KeyFactory.getInstance(algFamilyType.name());
        return kf.generatePublic(spec);
      } else {
        throw new IllegalArgumentException("Wrong signature algorithm type: " + algType);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to decode key", e);
    }
  }
}
