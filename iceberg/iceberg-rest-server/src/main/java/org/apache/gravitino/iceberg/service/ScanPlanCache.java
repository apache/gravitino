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

package org.apache.gravitino.iceberg.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scan plan cache. */
public class ScanPlanCache implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ScanPlanCache.class);

  private final Cache<ScanPlanCacheKey, PlanTableScanResponse> scanPlanCache;
  private final ScheduledExecutorService cleanupExecutor;

  public ScanPlanCache(int capacity, int expireMinutes) {
    LOG.info(
        "Initializing ScanPlanCache with capacity: {}, expireAfterAccess: {} minutes",
        capacity,
        expireMinutes);

    // Create a scheduled executor for periodic cleanup
    this.cleanupExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scan-plan-cache-cleanup-%d")
                .build());

    this.scanPlanCache =
        Caffeine.newBuilder()
            .maximumSize(capacity)
            .expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
            .scheduler(Scheduler.forScheduledExecutorService(cleanupExecutor))
            .removalListener(
                (key, value, cause) -> {
                  LOG.debug("Evicted scan plan from cache: {}, cause: {}", key, cause);
                })
            .build();

    LOG.info("ScanPlanCache initialized with automatic cleanup");
  }

  public PlanTableScanResponse get(Table table, PlanTableScanRequest scanRequest) {
    ScanPlanCacheKey key = ScanPlanCacheKey.create(table, scanRequest);
    PlanTableScanResponse cachedResponse = scanPlanCache.getIfPresent(key);

    if (cachedResponse != null) {
      LOG.info("Cache HIT for table: {}, snapshot: {}", table.name(), key.snapshotId);
    } else {
      LOG.debug("Cache MISS for table: {}, snapshot: {}", table.name(), key.snapshotId);
    }

    return cachedResponse;
  }

  public void put(
      Table table, PlanTableScanRequest scanRequest, PlanTableScanResponse scanResponse) {
    ScanPlanCacheKey key = ScanPlanCacheKey.create(table, scanRequest);
    scanPlanCache.put(key, scanResponse);
    LOG.debug("Cached scan plan for table: {}, snapshot: {}", table.name(), key.snapshotId);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing ScanPlanCache");

    if (scanPlanCache != null) {
      scanPlanCache.invalidateAll();
      scanPlanCache.cleanUp();
    }

    if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
      cleanupExecutor.shutdownNow();
    }
    LOG.info("ScanPlanCache closed successfully");
  }

  private static class ScanPlanCacheKey {
    private final TableIdentifier tableIdentifier;
    private final Long snapshotId;
    private final String filterStr;
    private final String selectStr;
    private final String statsFieldsStr;
    private final boolean caseSensitive;

    private ScanPlanCacheKey(
        TableIdentifier tableIdentifier,
        Long snapshotId,
        String filter,
        String select,
        String statsFields,
        boolean caseSensitive) {
      this.tableIdentifier = tableIdentifier;
      this.snapshotId = snapshotId;
      this.filterStr = filter;
      this.selectStr = select;
      this.statsFieldsStr = statsFields;
      this.caseSensitive = caseSensitive;
    }

    static ScanPlanCacheKey create(Table table, PlanTableScanRequest scanRequest) {
      TableIdentifier identifier = TableIdentifier.of(table.name());

      // Use current snapshot if not specified
      Long snapshotId = scanRequest.snapshotId();
      if (snapshotId == null && table.currentSnapshot() != null) {
        snapshotId = table.currentSnapshot().snapshotId();
      }

      // Convert complex objects to strings for equality comparison
      String filterStr = scanRequest.filter() != null ? scanRequest.filter().toString() : "";
      String selectStr = scanRequest.select() != null ? String.join(",", scanRequest.select()) : "";
      String statsFieldsStr =
          scanRequest.statsFields() != null ? String.join(",", scanRequest.statsFields()) : "";

      return new ScanPlanCacheKey(
          identifier,
          snapshotId,
          filterStr,
          selectStr,
          statsFieldsStr,
          scanRequest.caseSensitive());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ScanPlanCacheKey)) {
        return false;
      }
      ScanPlanCacheKey that = (ScanPlanCacheKey) o;
      return caseSensitive == that.caseSensitive
          && Objects.equal(tableIdentifier, that.tableIdentifier)
          && Objects.equal(snapshotId, that.snapshotId)
          && Objects.equal(filterStr, that.filterStr)
          && Objects.equal(selectStr, that.selectStr)
          && Objects.equal(statsFieldsStr, that.statsFieldsStr);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(
          tableIdentifier, snapshotId, filterStr, selectStr, statsFieldsStr, caseSensitive);
    }
  }
}
