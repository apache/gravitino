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
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared get-or-create cache for {@link IcebergCatalogWrapper} instances.
 *
 * <p>Used by IRC {@code IcebergCatalogWrapperManager} and by {@link
 * StandaloneCatalogWrapperManager} when IRC runs standalone (not embedded). A caller that knows the
 * catalog's {@code current_version} passes it in: a cached wrapper is reused only while its version
 * is still current. Replacing a stale wrapper does not close it while a catalog lease still holds
 * it. Eviction follows expire-after-access, capped so minted GCS OAuth2 tokens are refreshed before
 * {@code gcs.oauth2.token-expires-at}.
 */
public final class IcebergCatalogWrapperCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperCache.class);

  /**
   * Evict a cached catalog this long before its minted GCS OAuth2 token expires, so the next
   * request recreates the catalog and refreshes the token.
   */
  @VisibleForTesting static final long GCS_TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private final Cache<String, CachedCatalog> cache;
  private final ConcurrentHashMap<String, Object> keyLocks = new ConcurrentHashMap<>();
  private final Map<IcebergCatalogWrapper, CachedCatalog> byWrapper =
      Collections.synchronizedMap(new IdentityHashMap<>());

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
                (catalogName, cachedCatalog, cause) -> {
                  LOG.debug(
                      "Removing IcebergCatalogWrapper from cache: catalog={}, cause={}",
                      catalogName,
                      cause);
                  if (cachedCatalog == null) {
                    return;
                  }
                  if (cause == RemovalCause.REPLACED) {
                    // The catalog lease that loaded this wrapper still uses it. Drop only the
                    // cache's reference; the lease closes the wrapper when it finishes.
                    cachedCatalog.release();
                  } else {
                    cachedCatalog.forceClose();
                  }
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
   * Returns a cached wrapper, creating it with {@code loader} when absent. The cached value is
   * reused without comparing a catalog version.
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
   * Returns the cached wrapper when it was built for {@code catalogVersion} or a newer one.
   * Otherwise {@code loader} builds a replacement. The previous wrapper stays open until every
   * lease that {@link #acquire acquired} it has {@link #release released} it.
   *
   * @param catalogName catalog name
   * @param catalogVersion {@code catalog_meta.current_version}; {@code null} skips the version
   *     check and reuses only an unversioned entry
   * @param loader creates the wrapper on cache miss or when the cached version is older
   * @param <T> wrapper type
   * @return cached or newly created wrapper
   */
  @SuppressWarnings("unchecked")
  public <T extends IcebergCatalogWrapper> T get(
      String catalogName, @Nullable Long catalogVersion, Function<String, T> loader) {
    synchronized (lock(catalogName)) {
      CachedCatalog cached = cache.getIfPresent(catalogName);
      if (cached != null && versionMatches(cached, catalogVersion)) {
        cached.wrapper.reloadHadoopConf();
        return (T) cached.wrapper;
      }
      T created = loader.apply(catalogName);
      CachedCatalog entry = new CachedCatalog(catalogVersion, created);
      byWrapper.put(created, entry);
      cache.put(catalogName, entry);
      created.reloadHadoopConf();
      return created;
    }
  }

  /**
   * Keeps {@code wrapper} open across a catalog-version replacement. The cache already holds one
   * reference; each lease adds another.
   *
   * @param wrapper wrapper returned by {@link #get(String, Long, Function)}
   */
  public void acquire(IcebergCatalogWrapper wrapper) {
    CachedCatalog cached = byWrapper.get(wrapper);
    if (cached != null) {
      cached.acquire();
    }
  }

  /**
   * Drops one lease reference. The wrapper is closed once the cache and every lease have released
   * it.
   *
   * @param wrapper wrapper previously passed to {@link #acquire}
   */
  public void release(IcebergCatalogWrapper wrapper) {
    CachedCatalog cached = byWrapper.get(wrapper);
    if (cached != null) {
      cached.release();
    }
  }

  /**
   * Invalidates a cached wrapper. An explicit removal closes it even if a lease still holds it.
   * Closing is done on this thread; Caffeine's removal listener can run later and is idempotent.
   *
   * @param catalogName catalog name
   */
  public void invalidate(String catalogName) {
    CachedCatalog removed = cache.asMap().remove(catalogName);
    if (removed != null) {
      removed.forceClose();
    }
  }

  @Override
  public void close() {
    for (CachedCatalog cached : cache.asMap().values()) {
      cached.forceClose();
    }
    cache.invalidateAll();
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

  private Object lock(String catalogName) {
    return keyLocks.computeIfAbsent(catalogName, key -> new Object());
  }

  private static boolean versionMatches(CachedCatalog cached, @Nullable Long catalogVersion) {
    if (catalogVersion == null) {
      return cached.version == null;
    }
    return cached.version != null && cached.version >= catalogVersion;
  }

  private void forget(CachedCatalog cached) {
    byWrapper.remove(cached.wrapper);
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

  private final class CachedCatalog {

    private final Long version;
    private final IcebergCatalogWrapper wrapper;
    private final AtomicInteger refs = new AtomicInteger(1);

    CachedCatalog(@Nullable Long version, IcebergCatalogWrapper wrapper) {
      this.version = version;
      this.wrapper = wrapper;
    }

    void acquire() {
      refs.incrementAndGet();
    }

    void release() {
      if (refs.decrementAndGet() == 0) {
        forget(this);
        closeQuietly(wrapper);
      }
    }

    void forceClose() {
      forget(this);
      refs.set(0);
      closeQuietly(wrapper);
    }
  }

  private static final class CatalogWrapperExpiry implements Expiry<String, CachedCatalog> {

    private final long accessEvictionNanos;

    CatalogWrapperExpiry(long accessEvictionNanos) {
      this.accessEvictionNanos = accessEvictionNanos;
    }

    @Override
    public long expireAfterCreate(String key, CachedCatalog value, long currentTime) {
      return computeCacheDurationNanos(
          value.wrapper.getIcebergConfig(), accessEvictionNanos, System.currentTimeMillis());
    }

    @Override
    public long expireAfterUpdate(
        String key, CachedCatalog value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }

    @Override
    public long expireAfterRead(
        String key, CachedCatalog value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }
  }
}
