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
package org.apache.gravitino.catalog.hadoop.fs;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache for Hadoop FileSystem instances. This cache is designed to be shared between the fileset
 * server module and the GVFS client module.
 *
 * <p>The cache uses Caffeine as the underlying cache implementation and provides:
 *
 * <ul>
 *   <li>Automatic expiration of entries after a configurable time
 *   <li>Maximum capacity limiting
 *   <li>Automatic cleanup of expired entries via a scheduler
 *   <li>Proper resource cleanup when entries are removed
 *   <li>Optional metrics support via StatsCounter
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FileSystemCache cache = FileSystemCache.newBuilder()
 *     .expireAfterAccess(1, TimeUnit.HOURS)
 *     .maximumSize(100)
 *     .withCleanerScheduler("my-cache-cleaner")
 *     .build();
 *
 * FileSystem fs = cache.get(cacheKey, key -> createFileSystem(key));
 * }</pre>
 */
public class FileSystemCache implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemCache.class);

  private final Cache<FileSystemCacheKey, FileSystem> cache;
  private final ScheduledExecutorService scheduler;

  private FileSystemCache(
      Cache<FileSystemCacheKey, FileSystem> cache, ScheduledExecutorService scheduler) {
    this.cache = cache;
    this.scheduler = scheduler;
  }

  /**
   * Get or create a FileSystem for the given key.
   *
   * @param key the cache key
   * @param loader the function to create a FileSystem if not present in cache
   * @return the cached or newly created FileSystem
   */
  public FileSystem get(FileSystemCacheKey key, Function<FileSystemCacheKey, FileSystem> loader) {
    return cache.get(key, loader);
  }

  /**
   * Get a FileSystem for the given key if present.
   *
   * @param key the cache key
   * @return the cached FileSystem, or null if not present
   */
  public FileSystem getIfPresent(FileSystemCacheKey key) {
    return cache.getIfPresent(key);
  }

  /**
   * Invalidate (remove) a specific entry from the cache.
   *
   * @param key the cache key to invalidate
   */
  public void invalidate(FileSystemCacheKey key) {
    cache.invalidate(key);
  }

  /** Invalidate all entries in the cache. */
  public void invalidateAll() {
    cache.invalidateAll();
  }

  /** Performs any pending maintenance operations needed by the cache. */
  public void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Returns the underlying cache map view.
   *
   * @return a map view of the cache
   */
  public Map<FileSystemCacheKey, FileSystem> asMap() {
    return cache.asMap();
  }

  /**
   * Returns the approximate number of entries in this cache.
   *
   * @return the estimated size of the cache
   */
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  @Override
  public void close() throws IOException {
    // Close all cached file systems
    cache
        .asMap()
        .forEach(
            (k, v) -> {
              try {
                v.close();
              } catch (IOException e) {
                LOG.warn("Failed to close FileSystem instance in cache for key: {}", k, e);
              }
            });
    cache.cleanUp();

    // Shutdown the scheduler if it was created
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  /**
   * Creates a new builder for FileSystemCache.
   *
   * @return a new Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Builder class for FileSystemCache. */
  public static class Builder {
    private long expireAfterAccessDuration = 1;
    private TimeUnit expireAfterAccessUnit = TimeUnit.HOURS;
    private long maximumSize = -1;
    private String schedulerThreadName = "filesystem-cache-cleaner";
    private Supplier<StatsCounter> statsCounterSupplier;

    private Builder() {}

    /**
     * Sets the time after which an entry should be automatically removed after its last access.
     *
     * @param duration the duration
     * @param unit the time unit
     * @return this builder
     */
    public Builder expireAfterAccess(long duration, TimeUnit unit) {
      this.expireAfterAccessDuration = duration;
      this.expireAfterAccessUnit = unit;
      return this;
    }

    /**
     * Sets the maximum number of entries the cache may contain.
     *
     * @param maximumSize the maximum size
     * @return this builder
     */
    public Builder maximumSize(long maximumSize) {
      this.maximumSize = maximumSize;
      return this;
    }

    /**
     * Sets the name for the scheduler thread used for cache cleanup.
     *
     * @param threadName the thread name pattern
     * @return this builder
     */
    public Builder withCleanerScheduler(String threadName) {
      this.schedulerThreadName = threadName;
      return this;
    }

    /**
     * Sets a supplier for StatsCounter to record cache statistics.
     *
     * @param statsCounterSupplier the stats counter supplier
     * @return this builder
     */
    public Builder recordStats(Supplier<StatsCounter> statsCounterSupplier) {
      this.statsCounterSupplier = statsCounterSupplier;
      return this;
    }

    /**
     * Builds the FileSystemCache.
     *
     * @return a new FileSystemCache instance
     */
    public FileSystemCache build() {
      ScheduledThreadPoolExecutor scheduledExecutor =
          new ScheduledThreadPoolExecutor(
              1,
              new ThreadFactoryBuilder().setDaemon(true).setNameFormat(schedulerThreadName).build());

      RemovalListener<FileSystemCacheKey, FileSystem> removalListener =
          (key, value, cause) -> {
            if (value != null) {
              try {
                value.close();
              } catch (IOException e) {
                LOG.warn("Failed to close FileSystem instance in cache for key: {}", key, e);
              }
            }
          };

      Caffeine<Object, Object> cacheBuilder =
          Caffeine.newBuilder()
              .expireAfterAccess(expireAfterAccessDuration, expireAfterAccessUnit)
              .removalListener(removalListener)
              .scheduler(Scheduler.forScheduledExecutorService(scheduledExecutor));

      if (maximumSize > 0) {
        cacheBuilder.maximumSize(maximumSize);
      }

      if (statsCounterSupplier != null) {
        cacheBuilder.recordStats(statsCounterSupplier);
      }

      return new FileSystemCache(cacheBuilder.build(), scheduledExecutor);
    }
  }
}
