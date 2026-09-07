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
package org.apache.gravitino.iceberg.common.io;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Process-wide cache of GCS OAuth2 access tokens minted from service-account JSON files.
 *
 * <p>Token lifetime is independent of the IRC catalog-wrapper cache: entries expire shortly before
 * the underlying Google access token expires, and the next lookup remints a fresh token from the
 * service-account file.
 */
public final class GcsAccessTokenCache {

  private static final String GCS_CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  /** Evict this long before the Google access token expires so reminting happens early. */
  @VisibleForTesting static final long TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private static final Cache<String, AccessToken> CACHE =
      Caffeine.newBuilder().expireAfter(new AccessTokenExpiry()).build();

  private GcsAccessTokenCache() {}

  /**
   * Returns a cached access token for {@code serviceAccountFile}, minting one on cache miss or
   * after expiry.
   *
   * @param serviceAccountFile path to the GCS service-account JSON file
   * @return a non-null access token
   */
  public static AccessToken get(String serviceAccountFile) {
    return CACHE.get(serviceAccountFile, GcsAccessTokenCache::loadAccessToken);
  }

  @VisibleForTesting
  static void invalidate(String serviceAccountFile) {
    CACHE.invalidate(serviceAccountFile);
  }

  @VisibleForTesting
  static void invalidateAll() {
    CACHE.invalidateAll();
  }

  @VisibleForTesting
  static long durationUntilRefreshNanos(AccessToken accessToken, long nowEpochMillis) {
    if (accessToken.getExpirationTime() == null) {
      // No expiry from Google — keep the entry briefly so a bad token cannot live forever.
      return TimeUnit.MINUTES.toNanos(30);
    }
    long remainingMs =
        accessToken.getExpirationTime().toInstant().toEpochMilli()
            - TOKEN_REFRESH_BUFFER_MS
            - nowEpochMillis;
    return remainingMs <= 0 ? 0L : TimeUnit.MILLISECONDS.toNanos(remainingMs);
  }

  private static AccessToken loadAccessToken(String serviceAccountFile) {
    Path credentialsFilePath = Paths.get(serviceAccountFile);
    try (InputStream inputStream = Files.newInputStream(credentialsFilePath)) {
      GoogleCredentials credentials =
          GoogleCredentials.fromStream(inputStream).createScoped(GCS_CLOUD_PLATFORM_SCOPE);
      credentials.refreshIfExpired();
      AccessToken accessToken = credentials.getAccessToken();
      if (accessToken == null || accessToken.getTokenValue() == null) {
        throw new IllegalStateException(
            "Failed to obtain GCS access token from service account file: " + serviceAccountFile);
      }
      return accessToken;
    } catch (NoSuchFileException e) {
      throw new UncheckedIOException(
          "GCS service account file does not exist: " + serviceAccountFile, e);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to load GCS service account file: " + serviceAccountFile, e);
    }
  }

  private static final class AccessTokenExpiry implements Expiry<String, AccessToken> {

    @Override
    public long expireAfterCreate(String key, AccessToken value, long currentTime) {
      return durationUntilRefreshNanos(value, System.currentTimeMillis());
    }

    @Override
    public long expireAfterUpdate(
        String key, AccessToken value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }

    @Override
    public long expireAfterRead(
        String key, AccessToken value, long currentTime, long currentDuration) {
      // Recalculate from wall clock so access does not stretch past token expiry.
      return expireAfterCreate(key, value, currentTime);
    }
  }
}
