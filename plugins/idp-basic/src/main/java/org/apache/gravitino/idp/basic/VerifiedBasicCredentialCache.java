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
package org.apache.gravitino.idp.basic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;

/**
 * In-memory cache of successfully verified Basic credentials.
 *
 * <p>Failed authentications are never stored. Cache keys are digests of the username and password
 * so plaintext secrets are not retained. Entries also carry the stored password-hash fingerprint so
 * a password change is detected on the next request even before TTL expiry.
 */
public final class VerifiedBasicCredentialCache implements Closeable {

  private static final HexFormat HEX = HexFormat.of();

  @Nullable private final Cache<String, CachedEntry> credentialToEntry;
  private final ConcurrentHashMap<String, String> usernameToCredentialKey =
      new ConcurrentHashMap<>();

  /**
   * Creates a cache from server configuration.
   *
   * @param config server configuration
   */
  public VerifiedBasicCredentialCache(Config config) {
    this(
        config.get(IdpBasicConfigs.CREDENTIAL_CACHE_ENABLED),
        config.get(IdpBasicConfigs.CREDENTIAL_CACHE_EXPIRATION_SECS),
        config.get(IdpBasicConfigs.CREDENTIAL_CACHE_MAX_SIZE));
  }

  @VisibleForTesting
  VerifiedBasicCredentialCache(boolean enabled, long expirationSecs, long maxSize) {
    Preconditions.checkArgument(expirationSecs > 0, "expirationSecs must be > 0");
    Preconditions.checkArgument(maxSize > 0, "maxSize must be > 0");
    if (!enabled) {
      this.credentialToEntry = null;
      return;
    }
    this.credentialToEntry =
        Caffeine.newBuilder()
            .expireAfterWrite(expirationSecs, TimeUnit.SECONDS)
            .maximumSize(maxSize)
            .removalListener(
                (String key, CachedEntry value, RemovalCause cause) -> {
                  if (key != null && value != null) {
                    usernameToCredentialKey.remove(value.username(), key);
                  }
                })
            .build();
  }

  /**
   * Returns whether a successful verification for this credential may skip password derivation.
   *
   * @param username username
   * @param password plaintext password
   * @param passwordHash current stored password hash from the metadata store
   * @return {@code true} when the credential was verified recently against the same hash
   */
  public boolean isVerified(String username, String password, String passwordHash) {
    if (credentialToEntry == null || StringUtils.isAnyBlank(username, password, passwordHash)) {
      return false;
    }
    String credentialKey = credentialKey(username, password);
    CachedEntry cached = credentialToEntry.getIfPresent(credentialKey);
    return cached != null && Objects.equals(cached.passwordHash(), passwordHash);
  }

  /**
   * Records a successful password verification.
   *
   * @param username username
   * @param password plaintext password
   * @param passwordHash stored password hash that was just verified
   */
  public void rememberSuccess(String username, String password, String passwordHash) {
    if (credentialToEntry == null || StringUtils.isAnyBlank(username, password, passwordHash)) {
      return;
    }
    String credentialKey = credentialKey(username, password);
    String previousKey = usernameToCredentialKey.put(username, credentialKey);
    if (previousKey != null && !previousKey.equals(credentialKey)) {
      credentialToEntry.invalidate(previousKey);
    }
    credentialToEntry.put(credentialKey, new CachedEntry(username, passwordHash));
  }

  /**
   * Drops any cached successful credentials for the username.
   *
   * @param username username whose cached credentials should be removed
   */
  public void invalidateUser(String username) {
    if (credentialToEntry == null || StringUtils.isBlank(username)) {
      return;
    }
    String credentialKey = usernameToCredentialKey.remove(username);
    if (credentialKey != null) {
      credentialToEntry.invalidate(credentialKey);
    }
  }

  /** Returns whether caching is enabled. */
  public boolean isEnabled() {
    return credentialToEntry != null;
  }

  @VisibleForTesting
  long estimatedSize() {
    return credentialToEntry == null ? 0L : credentialToEntry.estimatedSize();
  }

  @Override
  public void close() {
    if (credentialToEntry != null) {
      credentialToEntry.invalidateAll();
      credentialToEntry.cleanUp();
    }
    usernameToCredentialKey.clear();
  }

  static String credentialKey(String username, String password) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(username.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);
      digest.update(password.getBytes(StandardCharsets.UTF_8));
      return HEX.formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required for Basic credential caching", e);
    }
  }

  private static final class CachedEntry {
    private final String username;
    private final String passwordHash;

    private CachedEntry(String username, String passwordHash) {
      this.username = username;
      this.passwordHash = passwordHash;
    }

    private String username() {
      return username;
    }

    private String passwordHash() {
      return passwordHash;
    }
  }
}
