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
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
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
 * <p>Supports N JWKS sources selected by matching the token's {@code iss} claim against configured
 * entries. Indexed config keys ({@code gravitino.authenticator.oauth.jwks.N.uri}, {@code .issuer},
 * {@code .audience}, {@code .principalFields}) allow multiple validators. Falls back to the single
 * {@code jwksUri} / {@code serviceAudience} / {@code principalFields} config for backward
 * compatibility.
 *
 * <p>Token routing is O(1): the {@code iss} claim is read from the JWT payload without signature
 * verification, then only the matching JWKS endpoint is consulted for signature validation.
 */
public class JwksTokenValidator implements OAuthTokenValidator {

  private static final Logger LOG = LoggerFactory.getLogger(JwksTokenValidator.class);
  private static final String JWKS_INDEX_PREFIX = "gravitino.authenticator.oauth.jwks.";

  // Ordered list (by config index) used for logging; keyed by issuer for O(1) lookup at runtime.
  private List<JwksEntry> orderedEntries;
  private Map<String, JwksEntry> entriesByIssuer;

  // When legacy single-entry config omits an authority/issuer, the entry acts as a catch-all.
  @Nullable private JwksEntry singleFallbackEntry;

  private long allowSkewSeconds;
  private PrincipalMapper principalMapper;
  private GroupMapper groupMapper;
  private List<String> groupsFields;

  @Override
  public void initialize(Config config) {
    this.allowSkewSeconds = config.get(OAuthConfig.ALLOW_SKEW_SECONDS);
    String mapperType = config.get(OAuthConfig.PRINCIPAL_MAPPER);
    String regexPattern = config.get(OAuthConfig.PRINCIPAL_MAPPER_REGEX_PATTERN);
    this.principalMapper = PrincipalMapperFactory.create(mapperType, regexPattern);

    String groupMapperType = config.get(OAuthConfig.GROUP_MAPPER);
    String groupRegexPattern = config.get(OAuthConfig.GROUP_MAPPER_REGEX_PATTERN);
    this.groupMapper = GroupMapperFactory.create(groupMapperType, groupRegexPattern);
    this.groupsFields = config.get(OAuthConfig.GROUPS_FIELDS);

    this.orderedEntries = new ArrayList<>();
    this.entriesByIssuer = new HashMap<>();

    boolean loaded = loadIndexedEntries(config);
    if (!loaded) {
      loadLegacyEntry(config);
    }

    Preconditions.checkArgument(
        !orderedEntries.isEmpty(),
        "No JWKS entries configured. Use gravitino.authenticator.oauth.jwks.0.uri "
            + "or gravitino.authenticator.oauth.jwksUri");

    LOG.info("JwksTokenValidator initialized with {} JWKS entry/entries:", orderedEntries.size());
    for (JwksEntry e : orderedEntries) {
      LOG.info("  issuer={} jwksUri={} audience={}", e.issuer, e.jwksUri, e.audience);
    }
  }

  @Override
  public Principal validateToken(String token, String serviceAudience) {
    if (StringUtils.isBlank(token)) {
      throw new UnauthorizedException("Token cannot be null or empty");
    }

    // Step 1: parse without signature verification to extract iss for O(1) entry lookup.
    SignedJWT signedJWT;
    String iss;
    try {
      signedJWT = SignedJWT.parse(token);
      iss = signedJWT.getJWTClaimsSet().getIssuer();
    } catch (Exception e) {
      throw new UnauthorizedException(e, "Failed to parse JWT token");
    }

    // Step 2: route to the matching JWKS entry by issuer.
    JwksEntry entry = StringUtils.isNotBlank(iss) ? entriesByIssuer.get(iss) : null;
    if (entry == null) {
      entry = singleFallbackEntry;
    }
    if (entry == null) {
      throw new UnauthorizedException(
          "No JWKS entry configured for issuer: %s. Known issuers: %s",
          iss, entriesByIssuer.keySet());
    }

    // Step 3: full signature + claims validation against only this entry's JWKS.
    try {
      ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
      JWSKeySelector<SecurityContext> keySelector =
          new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, entry.jwkSource);
      processor.setJWSKeySelector(keySelector);

      // For legacy single-entry config, audience is taken from the validateToken parameter;
      // for indexed multi-entry config, it is set explicitly in the entry.
      String effectiveAudience =
          StringUtils.isNotBlank(entry.audience) ? entry.audience : serviceAudience;

      // Use Set-based audience for RFC 7519 containment matching (at-least-one).
      Set<String> acceptedAudiences =
          StringUtils.isNotBlank(effectiveAudience)
              ? Collections.singleton(effectiveAudience)
              : null;

      // Put only issuer in the exact-match claims; audience is handled separately above.
      JWTClaimsSet.Builder exactMatchBuilder = new JWTClaimsSet.Builder();
      if (StringUtils.isNotBlank(entry.issuer)) {
        exactMatchBuilder.issuer(entry.issuer);
      }
      DefaultJWTClaimsVerifier<SecurityContext> claimsVerifier =
          new DefaultJWTClaimsVerifier<>(acceptedAudiences, exactMatchBuilder.build(), null, null);
      claimsVerifier.setMaxClockSkew((int) allowSkewSeconds);
      processor.setJWTClaimsSetVerifier(claimsVerifier);

      JWTClaimsSet claims = processor.process(token, null);
      String principal = extractPrincipal(claims, entry.principalFields);
      if (principal == null) {
        throw new UnauthorizedException(
            "No valid principal found in token claims for fields: %s", entry.principalFields);
      }
      Principal userPrincipal = principalMapper.map(principal);
      List<Object> groups = extractGroups(claims);
      if (groups != null && !groups.isEmpty()) {
        List<UserGroup> mappedGroups = groupMapper.map(groups);
        return new UserPrincipal(userPrincipal.getName(), mappedGroups);
      }
      return userPrincipal;
    } catch (UnauthorizedException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("JWKS JWT validation failed for issuer={}: {}", iss, e.getMessage());
      throw new UnauthorizedException(e, "JWKS JWT validation failed for issuer: %s", iss);
    }
  }

  /**
   * Scans indexed config keys ({@code gravitino.authenticator.oauth.jwks.N.*}) sequentially and
   * builds one {@link JwksEntry} per index until no {@code .uri} key is found.
   *
   * @return {@code true} if at least one indexed entry was loaded.
   */
  private boolean loadIndexedEntries(Config config) {
    for (int i = 0; ; i++) {
      String uri = config.getRawString(JWKS_INDEX_PREFIX + i + ".uri");
      if (uri == null) {
        break;
      }
      String issuer = config.getRawString(JWKS_INDEX_PREFIX + i + ".issuer");
      String audience = config.getRawString(JWKS_INDEX_PREFIX + i + ".audience");
      String principalFieldsRaw = config.getRawString(JWKS_INDEX_PREFIX + i + ".principalFields");

      Preconditions.checkArgument(
          StringUtils.isNotBlank(issuer),
          "Missing required config: %s%s.issuer",
          JWKS_INDEX_PREFIX,
          i);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(audience),
          "Missing required config: %s%s.audience",
          JWKS_INDEX_PREFIX,
          i);

      List<String> principalFields =
          principalFieldsRaw != null
              ? Arrays.asList(principalFieldsRaw.split(","))
              : Arrays.asList("sub");

      JwksEntry entry = buildEntry(uri, issuer, audience, principalFields, i);
      orderedEntries.add(entry);
      entriesByIssuer.put(issuer, entry);
    }
    return !orderedEntries.isEmpty();
  }

  /**
   * Falls back to the legacy single-entry config ({@code jwksUri} / {@code serviceAudience} /
   * {@code principalFields}). If {@code authority} is set it is used as the issuer for matching;
   * otherwise the entry acts as a catch-all for any issuer.
   */
  private void loadLegacyEntry(Config config) {
    String uri = config.get(OAuthConfig.JWKS_URI);
    if (StringUtils.isBlank(uri)) {
      return;
    }
    // Audience is intentionally null here: the legacy path uses the serviceAudience
    // parameter passed to validateToken(), preserving backward-compatible behaviour.
    List<String> principalFields = config.get(OAuthConfig.PRINCIPAL_FIELDS);
    String issuer = config.get(OAuthConfig.AUTHORITY);

    JwksEntry entry = buildEntry(uri, issuer, null, principalFields, 0);
    orderedEntries.add(entry);
    if (StringUtils.isNotBlank(issuer)) {
      entriesByIssuer.put(issuer, entry);
    } else {
      singleFallbackEntry = entry;
    }
  }

  private JwksEntry buildEntry(
      String uri, String issuer, String audience, List<String> principalFields, int index) {
    try {
      JWKSource<SecurityContext> jwkSource = JWKSourceBuilder.create(new URL(uri)).build();
      return new JwksEntry(uri, issuer, audience, principalFields, jwkSource);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JWKS URI at index " + index + ": " + uri, e);
    }
  }

  private String extractPrincipal(JWTClaimsSet claims, List<String> fields) {
    for (String field : fields) {
      if (StringUtils.isNotBlank(field)) {
        Object val = claims.getClaim(field.trim());
        if (val instanceof String && StringUtils.isNotBlank((String) val)) {
          return (String) val;
        }
      }
    }
    return null;
  }

  /** Extracts the groups from validated JWT claims using configured group field(s). */
  private List<Object> extractGroups(JWTClaimsSet validatedClaims) {
    if (groupsFields != null && !groupsFields.isEmpty()) {
      for (String field : groupsFields) {
        if (StringUtils.isNotBlank(field)) {
          try {
            Object groupsObj = validatedClaims.getClaim(field);
            if (groupsObj instanceof List) {
              @SuppressWarnings("unchecked")
              List<Object> groups = (List<Object>) groupsObj;
              return groups;
            }
          } catch (Exception e) {
            LOG.warn("Failed to parse groups from claim field: {}", field, e);
          }
        }
      }
    }
    return null;
  }

  /** Immutable descriptor for one JWKS entry. */
  private static final class JwksEntry {

    final String jwksUri;
    @Nullable final String issuer;
    final String audience;
    final List<String> principalFields;
    final JWKSource<SecurityContext> jwkSource;

    JwksEntry(
        String jwksUri,
        @Nullable String issuer,
        String audience,
        List<String> principalFields,
        JWKSource<SecurityContext> jwkSource) {
      this.jwksUri = jwksUri;
      this.issuer = issuer;
      this.audience = audience;
      this.principalFields = principalFields;
      this.jwkSource = jwkSource;
    }
  }
}
