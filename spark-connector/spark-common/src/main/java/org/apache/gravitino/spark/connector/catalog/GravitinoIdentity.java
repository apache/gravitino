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
package org.apache.gravitino.spark.connector.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The identity a Gravitino request is made on behalf of. It exists only to partition the client and
 * catalog caches held by {@link GravitinoCatalogManager}; it is never presented to the Gravitino
 * server and never used to make a trust decision.
 *
 * <p>The safety property this class must uphold is that the cache key is at least as fine-grained
 * as the principal the Gravitino server derives from the same credential, and never exactly equal
 * to it in the sense of being coarser. A key finer than the server's principal costs a redundant
 * cache entry and nothing more. A key coarser than the server's principal would let one user be
 * served another user's cached metadata, which is precisely the bug this class exists to prevent.
 * When in doubt, be finer.
 *
 * <p>For that reason the claim read out of a JWT is taken without verifying the signature.
 * Validating the token is the server's job, and a value used only to split a cache must not be
 * mistaken for a verified assertion about who is calling.
 */
public final class GravitinoIdentity {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoIdentity.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Prefix of every token-derived key, so it can never collide with an auth-type key. */
  private static final String TOKEN_KEY_PREFIX = "token:";

  private static final String HASHED_KEY_PREFIX = TOKEN_KEY_PREFIX + "sha256:";

  private static final int JWT_PART_COUNT = 3;

  private final String key;

  private GravitinoIdentity(String key) {
    this.key = key;
  }

  /**
   * Returns the single identity shared by every auth type that resolves one credential for the
   * whole Spark application, namely {@code simple}, {@code basic}, {@code oauth2} and {@code
   * kerberos}. Those deployments keep exactly one identity per application.
   *
   * @param authType the configured auth type, used verbatim as the key
   * @return the application-wide identity for that auth type
   */
  public static GravitinoIdentity application(String authType) {
    return new GravitinoIdentity(String.valueOf(authType));
  }

  /**
   * Derives an identity from a bearer token.
   *
   * <p>When the token is a three-part JWT whose payload parses, the first of {@code
   * principalFields} that is present and non-blank supplies the key. Otherwise, and for opaque
   * access tokens generally, the key falls back to the hex SHA-256 of the token. The signature is
   * never verified.
   *
   * @param token the bearer token, never logged and never stored on the returned identity
   * @param principalFields the ordered claim names to try, mirroring the server's {@code
   *     gravitino.authenticator.oauth.principalFields}
   * @return an identity suitable for use as a cache key
   */
  public static GravitinoIdentity fromToken(String token, List<String> principalFields) {
    String[] parts = token.split("\\.");
    if (parts.length == JWT_PART_COUNT) {
      try {
        JsonNode payload = OBJECT_MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1].trim()));
        for (String field : principalFields) {
          JsonNode value = payload.get(field);
          if (value != null && !value.isNull() && StringUtils.isNotBlank(value.asText())) {
            return new GravitinoIdentity(TOKEN_KEY_PREFIX + field + ":" + value.asText());
          }
        }
        LOG.debug("No principal field of {} found in the token payload.", principalFields);
      } catch (Exception e) {
        LOG.debug("Cannot parse the token payload as a JWT, falling back to a token hash.", e);
      }
    }
    return new GravitinoIdentity(
        HASHED_KEY_PREFIX + Hashing.sha256().hashString(token, StandardCharsets.UTF_8));
  }

  /**
   * Returns the cache key of this identity. It never is, and never contains, a raw token.
   *
   * @return the cache key
   */
  public String key() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GravitinoIdentity)) {
      return false;
    }
    return Objects.equals(key, ((GravitinoIdentity) o).key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  @Override
  public String toString() {
    return "GravitinoIdentity{key=" + key + "}";
  }
}
