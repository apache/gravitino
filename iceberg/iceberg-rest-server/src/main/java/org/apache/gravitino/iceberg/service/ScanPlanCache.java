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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  public PlanTableScanResponse get(
      TableIdentifier tableIdentifier, Table table, PlanTableScanRequest scanRequest) {
    ScanPlanCacheKey key = ScanPlanCacheKey.create(tableIdentifier, table, scanRequest);
    PlanTableScanResponse cachedResponse = scanPlanCache.getIfPresent(key);

    if (cachedResponse != null) {
      LOG.debug("Cache HIT for table: {}, snapshot: {}", tableIdentifier, key.snapshotId);
    } else {
      LOG.debug("Cache MISS for table: {}, snapshot: {}", tableIdentifier, key.snapshotId);
    }

    return cachedResponse;
  }

  public void put(
      TableIdentifier tableIdentifier,
      Table table,
      PlanTableScanRequest scanRequest,
      PlanTableScanResponse scanResponse) {
    ScanPlanCacheKey key = ScanPlanCacheKey.create(tableIdentifier, table, scanRequest);
    scanPlanCache.put(key, scanResponse);
    LOG.debug("Cached scan plan for table: {}, snapshot: {}", tableIdentifier, key.snapshotId);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing ScanPlanCache");

    if (scanPlanCache != null) {
      scanPlanCache.invalidateAll();
      scanPlanCache.cleanUp();
    }

    if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
      cleanupExecutor.shutdown();
      try {
        if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          cleanupExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        cleanupExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    LOG.info("ScanPlanCache closed successfully");
  }

  private static class ScanPlanCacheKey {
    private final TableIdentifier tableIdentifier;
    private final Long snapshotId;
    private final Long startSnapshotId;
    private final Long endSnapshotId;
    private final String filterStr;
    private final String selectStr;
    private final String statsFieldsStr;
    private final boolean caseSensitive;

    private ScanPlanCacheKey(
        TableIdentifier tableIdentifier,
        Long snapshotId,
        Long startSnapshotId,
        Long endSnapshotId,
        String filter,
        String select,
        String statsFields,
        boolean caseSensitive) {
      this.tableIdentifier = tableIdentifier;
      this.snapshotId = snapshotId;
      this.startSnapshotId = startSnapshotId;
      this.endSnapshotId = endSnapshotId;
      this.filterStr = filter;
      this.selectStr = select;
      this.statsFieldsStr = statsFields;
      this.caseSensitive = caseSensitive;
    }

    static ScanPlanCacheKey create(
        TableIdentifier tableIdentifier, Table table, PlanTableScanRequest scanRequest) {

      // Use current snapshot if not specified
      Long snapshotId = scanRequest.snapshotId();
      if ((snapshotId == null || snapshotId == 0L) && table.currentSnapshot() != null) {
        snapshotId = table.currentSnapshot().snapshotId();
      }

      // Include startSnapshotId and endSnapshotId in the key
      Long startSnapshotId = scanRequest.startSnapshotId();
      Long endSnapshotId = scanRequest.endSnapshotId();

      String filterStr = scanRequest.filter() != null ? scanRequest.filter().toString() : "";

      // Sort select and statsFields to make key order-independent
      String selectStr = "";
      if (scanRequest.select() != null && !scanRequest.select().isEmpty()) {
        List<String> sortedSelect = new ArrayList<>(scanRequest.select());
        Collections.sort(sortedSelect);
        selectStr = String.join(",", sortedSelect);
      }

      String statsFieldsStr = "";
      if (scanRequest.statsFields() != null && !scanRequest.statsFields().isEmpty()) {
        List<String> sortedStatsFields = new ArrayList<>(scanRequest.statsFields());
        Collections.sort(sortedStatsFields);
        statsFieldsStr = String.join(",", sortedStatsFields);
      }

      return new ScanPlanCacheKey(
          tableIdentifier,
          snapshotId,
          startSnapshotId,
          endSnapshotId,
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
          && Objects.equal(startSnapshotId, that.startSnapshotId)
          && Objects.equal(endSnapshotId, that.endSnapshotId)
          && Objects.equal(filterStr, that.filterStr)
          && Objects.equal(selectStr, that.selectStr)
          && Objects.equal(statsFieldsStr, that.statsFieldsStr);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(
          tableIdentifier,
          snapshotId,
          startSnapshotId,
          endSnapshotId,
          filterStr,
          selectStr,
          statsFieldsStr,
          caseSensitive);
    }

    @Override
    public String toString() {
      return String.format(
          "ScanPlanCacheKey{table=%s, snapshotId=%s, startSnapshotId=%s, endSnapshotId=%s, "
              + "filter=%s, select=%s, statsFields=%s, caseSensitive=%s}",
          tableIdentifier,
          snapshotId,
          startSnapshotId,
          endSnapshotId,
          filterStr.isEmpty() ? "null" : filterStr,
          selectStr.isEmpty() ? "null" : selectStr,
          statsFieldsStr.isEmpty() ? "null" : statsFieldsStr,
          caseSensitive);
    }
  }
}
