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

package org.apache.gravitino.iceberg.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local in-memory implementation of {@link ScanPlanCache} using Caffeine cache.
 *
 * <p>This cache is thread-safe and uses a LRU eviction policy.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * ScanPlanCache cache = new LocalScanPlanCache(100, 60);
 * Optional<PlanTableScanResponse> response = cache.get(key);
 * response.ifPresent(r -> processResponse(r));
 * }</pre>
 */
public class LocalScanPlanCache implements ScanPlanCache {

  private static final Logger LOG = LoggerFactory.getLogger(LocalScanPlanCache.class);

  private final Cache<ScanPlanCacheKey, PlanTableScanResponse> scanPlanCache;

  /**
   * Creates a new ScanPlanCache with the specified capacity and expiration time.
   *
   * @param capacity Maximum number of scan plan results to cache. When the cache reaches this size,
   *     least-recently-used entries will be evicted.
   * @param expireMinutes Time in minutes after which cached scan plans expire if not accessed.
   *     Cached entries are automatically removed after this period of inactivity.
   *     <p><b>Memory considerations:</b> Memory usage is approximately {@code capacity} multiplied
   *     by the average scan plan size. For example, with a capacity of 200 and an average scan plan
   *     size of 1MB, expect approximately 200MB of memory usage for the cache. A typical scan plan
   *     might be several KB to MB depending on table size.
   *     <p><b>Memory considerations:</b> Memory usage is approximately {@code capacity} multiplied
   *     by the average scan plan size. For example, with a capacity of 200 and an average scan plan
   *     size of 1MB, expect approximately 200MB of memory usage for the cache. A typical scan plan
   *     might be several KB to MB depending on table size.
   */
  public LocalScanPlanCache(int capacity, int expireMinutes) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("Cache capacity must be positive, got: " + capacity);
    }
    if (expireMinutes <= 0) {
      throw new IllegalArgumentException(
          "Cache expiration time must be positive, got: " + expireMinutes);
    }
    LOG.info(
        "Initializing LocalScanPlanCache with capacity: {}, expireAfterAccess: {} minutes",
        capacity,
        expireMinutes);

    this.scanPlanCache =
        Caffeine.newBuilder()
            .maximumSize(capacity)
            .expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
            .executor(Runnable::run)
            .build();

    LOG.info("LocalScanPlanCache initialized successfully");
  }

  /**
   * Retrieves a cached scan plan for the given key.
   *
   * @param key the cache key containing table identifier and snapshot information
   * @return an Optional containing the cached PlanTableScanResponse if present, or empty otherwise
   */
  @Override
  public Optional<PlanTableScanResponse> get(ScanPlanCacheKey key) {
    if (key == null) {
      throw new IllegalArgumentException("Cache key cannot be null");
    }
    PlanTableScanResponse cachedResponse = scanPlanCache.getIfPresent(key);
    if (cachedResponse != null) {
      LOG.debug(
          "Cache HIT for table: {}, snapshot: {}", key.getTableIdentifier(), key.getSnapshotId());
    } else {
      LOG.debug(
          "Cache MISS for table: {}, snapshot: {}", key.getTableIdentifier(), key.getSnapshotId());
    }

    return Optional.ofNullable(cachedResponse);
  }

  /**
   * Stores a scan plan in the cache with the given key.
   *
   * @param key the cache key containing table identifier and snapshot information
   * @param scanResponse the scan plan response to cache
   */
  @Override
  public void put(ScanPlanCacheKey key, PlanTableScanResponse scanResponse) {
    if (key == null) {
      throw new IllegalArgumentException("Cache key cannot be null");
    }
    if (scanResponse == null) {
      throw new IllegalArgumentException("Scan response cannot be null");
    }
    scanPlanCache.put(key, scanResponse);
    LOG.debug(
        "Cached scan plan for table: {}, snapshot: {}",
        key.getTableIdentifier(),
        key.getSnapshotId());
  }

  /**
   * Closes the cache and releases all cached resources. This method invalidates all cached entries
   * and performs cleanup operations.
   */
  @Override
  public void close() {
    LOG.info("Closing LocalScanPlanCache");

    if (scanPlanCache != null) {
      scanPlanCache.invalidateAll();
      scanPlanCache.cleanUp();
    }
    LOG.info("LocalScanPlanCache closed successfully");
  }
}
