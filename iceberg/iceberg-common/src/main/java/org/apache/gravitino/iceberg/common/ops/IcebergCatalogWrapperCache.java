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
package org.apache.gravitino.iceberg.common.ops;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get-or-create cache for IRC {@link IcebergCatalogWrapper} instances.
 *
 * <p>Used by IRC {@code IcebergCatalogWrapperManager}. Metalake lakehouse keeps a separate owned
 * wrapper and refreshes GCS tokens via {@code CatalogManager}'s catalog cache — objects are not
 * shared with this cache. The cache key is {@link Key}({@code catalogName}, {@code
 * current_version}): a catalog alter that bumps {@code catalog_meta.current_version} stores a new
 * entry and leaves the previous one until expire-after-access removes it. Eviction is also capped
 * so minted GCS OAuth2 tokens are refreshed before {@code gcs.oauth2.token-expires-at}.
 */
public final class IcebergCatalogWrapperCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperCache.class);

  /**
   * Evict a cached catalog this long before its minted GCS OAuth2 token expires, so the next
   * request recreates the catalog and refreshes the token.
   */
  @VisibleForTesting static final long GCS_TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private final Cache<Key, IcebergCatalogWrapper> cache;

  /**
   * Creates a cache using {@link IcebergConfig#ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL}.
   *
   * @param config Iceberg configuration (IRC server props or lakehouse catalog props)
   * @return new cache
   */
  public static IcebergCatalogWrapperCache create(IcebergConfig config) {
    return new IcebergCatalogWrapperCache(
        config.get(IcebergConfig.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL));
  }

  /**
   * Creates a cache with the given access-based eviction interval.
   *
   * @param accessEvictionMs expire-after-access interval in milliseconds
   */
  @VisibleForTesting
  IcebergCatalogWrapperCache(long accessEvictionMs) {
    long accessEvictionNanos = TimeUnit.MILLISECONDS.toNanos(accessEvictionMs);
    this.cache =
        Caffeine.newBuilder()
            .expireAfter(new CatalogWrapperExpiry(accessEvictionNanos))
            .removalListener(
                (key, catalogWrapper, cause) -> {
                  LOG.debug(
                      "Removing IcebergCatalogWrapper from cache: key={}, cause={}", key, cause);
                  closeQuietly(catalogWrapper);
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("iceberg-catalog-wrapper-cleaner-%d")
                            .build())))
            .build();
  }

  /**
   * Returns a cached wrapper, creating it with {@code loader} when absent. Uses an unversioned key.
   *
   * @param catalogName catalog name
   * @param loader creates the wrapper on cache miss
   * @param <T> wrapper type
   * @return cached or newly created wrapper
   */
  public <T extends IcebergCatalogWrapper> T get(String catalogName, Function<String, T> loader) {
    return get(catalogName, null, loader);
  }

  /**
   * Returns the wrapper cached for {@code catalogName} at {@code catalogVersion}, creating it with
   * {@code loader} when absent. A different version is a different cache entry; the previous entry
   * stays until it expires.
   *
   * @param catalogName catalog name
   * @param catalogVersion {@code catalog_meta.current_version}; {@code null} for an unversioned
   *     entry
   * @param loader creates the wrapper on cache miss
   * @param <T> wrapper type
   * @return cached or newly created wrapper
   */
  @SuppressWarnings("unchecked")
  public <T extends IcebergCatalogWrapper> T get(
      String catalogName, @Nullable Long catalogVersion, Function<String, T> loader) {
    Key key = new Key(catalogName, catalogVersion);
    T wrapper = (T) cache.get(key, k -> loader.apply(k.catalogName()));
    wrapper.reloadHadoopConf();
    return wrapper;
  }

  /**
   * Removes every cached wrapper for {@code catalogName}, including all versions.
   *
   * <p>Closes on this thread via {@code asMap().remove}. Caffeine may also invoke the removal
   * listener later; {@link IcebergCatalogWrapper#close()} is idempotent ({@code beginClose()}).
   *
   * @param catalogName catalog name
   */
  public void invalidate(String catalogName) {
    List<Key> keys = new ArrayList<>();
    for (Key key : cache.asMap().keySet()) {
      if (key.catalogName().equals(catalogName)) {
        keys.add(key);
      }
    }
    for (Key key : keys) {
      IcebergCatalogWrapper removed = cache.asMap().remove(key);
      closeQuietly(removed);
    }
  }

  @Override
  public void close() {
    List<IcebergCatalogWrapper> wrappers = new ArrayList<>(cache.asMap().values());
    for (Key key : new ArrayList<>(cache.asMap().keySet())) {
      cache.asMap().remove(key);
    }
    for (IcebergCatalogWrapper wrapper : wrappers) {
      closeQuietly(wrapper);
    }
  }

  /**
   * Computes how long a catalog wrapper may stay in the cache.
   *
   * <p>Uses the configured access-based eviction interval, capped by the time until a minted GCS
   * OAuth2 token should be refreshed ({@code gcs.oauth2.token-expires-at} minus {@link
   * #GCS_TOKEN_REFRESH_BUFFER_MS}). When no token expiry is present, returns {@code
   * accessEvictionNanos}.
   *
   * @param config catalog config that may contain {@code gcs.oauth2.token-expires-at}
   * @param accessEvictionNanos default expire-after-access duration in nanoseconds
   * @param nowEpochMillis current wall-clock time
   * @return cache duration in nanoseconds; {@code 0} means expire immediately
   */
  @VisibleForTesting
  public static long computeCacheDurationNanos(
      IcebergConfig config, long accessEvictionNanos, long nowEpochMillis) {
    String expiresAt =
        config.getAllConfig().get(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT);
    if (StringUtils.isBlank(expiresAt)) {
      return accessEvictionNanos;
    }

    long expiresAtMs;
    try {
      expiresAtMs = Long.parseLong(expiresAt);
    } catch (NumberFormatException e) {
      LOG.warn("Invalid {}: {}", IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT, expiresAt);
      return accessEvictionNanos;
    }

    long remainingMs = expiresAtMs - GCS_TOKEN_REFRESH_BUFFER_MS - nowEpochMillis;
    if (remainingMs <= 0) {
      return 0L;
    }
    return Math.min(accessEvictionNanos, TimeUnit.MILLISECONDS.toNanos(remainingMs));
  }

  private static void closeQuietly(IcebergCatalogWrapper catalogWrapper) {
    if (catalogWrapper == null) {
      return;
    }
    try {
      catalogWrapper.close();
    } catch (Exception ex) {
      LOG.warn("Close Iceberg catalog wrapper fail: {}, {}", catalogWrapper, ex);
    }
  }

  /**
   * Cache key for one catalog at one {@code catalog_meta.current_version}.
   *
   * <p>{@code catalogVersion} may be {@code null} for callers that do not version the entry
   * (standalone / non-aux).
   */
  public static final class Key {
    private final String catalogName;
    @Nullable private final Long catalogVersion;

    /**
     * @param catalogName catalog name
     * @param catalogVersion {@code catalog_meta.current_version}, or {@code null}
     */
    public Key(String catalogName, @Nullable Long catalogVersion) {
      this.catalogName = catalogName;
      this.catalogVersion = catalogVersion;
    }

    /**
     * @return catalog name
     */
    public String catalogName() {
      return catalogName;
    }

    /**
     * @return catalog version, or {@code null}
     */
    @Nullable
    public Long catalogVersion() {
      return catalogVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key key = (Key) o;
      return Objects.equal(catalogName, key.catalogName)
          && Objects.equal(catalogVersion, key.catalogVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(catalogName, catalogVersion);
    }

    @Override
    public String toString() {
      return "Key{catalogName='" + catalogName + "', catalogVersion=" + catalogVersion + '}';
    }
  }

  private static final class CatalogWrapperExpiry implements Expiry<Key, IcebergCatalogWrapper> {

    private final long accessEvictionNanos;

    CatalogWrapperExpiry(long accessEvictionNanos) {
      this.accessEvictionNanos = accessEvictionNanos;
    }

    @Override
    public long expireAfterCreate(Key key, IcebergCatalogWrapper value, long currentTime) {
      return computeCacheDurationNanos(
          value.getIcebergConfig(), accessEvictionNanos, System.currentTimeMillis());
    }

    @Override
    public long expireAfterUpdate(
        Key key, IcebergCatalogWrapper value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }

    @Override
    public long expireAfterRead(
        Key key, IcebergCatalogWrapper value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }
  }
}
