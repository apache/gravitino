/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically removes expired rows from {@code entity_change_log} on a dedicated thread.
 *
 * <p>Cleanup is intentionally independent from polling. The database calculates expiration using
 * its own clock, which is also the clock used when change records are inserted.
 *
 * <p>Retention is validated against the poll interval. A retention shorter than a few poll cycles
 * lets this cleaner delete records before every node had a chance to consume them, which loses
 * invalidations silently, so such a configuration is rejected at startup instead.
 */
public class EntityChangeLogCleaner implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogCleaner.class);

  /** Upper bound for the delay before the first cleanup run. */
  private static final long MAX_INITIAL_DELAY_MS = TimeUnit.MINUTES.toMillis(10);

  /**
   * How many poll cycles a change record must survive at minimum. A record has to outlive more than
   * one cycle, because a node can miss cycles while it is restarting, stalled in a long GC pause,
   * or paused retrying a failed listener.
   */
  private static final long MIN_RETENTION_POLL_CYCLES = 10;

  private final long retentionMs;
  private final long cleanupIntervalMs;

  private ScheduledExecutorService scheduler;

  /**
   * Creates an entity change log cleaner.
   *
   * @param retentionMs retention time in milliseconds, or 0 to disable cleanup
   * @param cleanupIntervalMs interval between cleanup runs in milliseconds
   * @param pollIntervalMs interval between poller cycles in milliseconds, used to reject a
   *     retention that would drop records before the pollers can consume them
   */
  public EntityChangeLogCleaner(long retentionMs, long cleanupIntervalMs, long pollIntervalMs) {
    Preconditions.checkArgument(retentionMs >= 0, "retentionMs must be non-negative");
    Preconditions.checkArgument(cleanupIntervalMs > 0, "cleanupIntervalMs must be positive");
    Preconditions.checkArgument(pollIntervalMs > 0, "pollIntervalMs must be positive");
    // Divide rather than multiply: pollIntervalMs comes from configuration and a multiplication
    // could overflow.
    Preconditions.checkArgument(
        retentionMs == 0 || retentionMs / MIN_RETENTION_POLL_CYCLES >= pollIntervalMs,
        "retentionMs (%s) must be 0 to disable cleanup, or at least %s times the poll interval "
            + "(%s ms); a shorter retention can prune change records before every node consumed "
            + "them and silently lose cache invalidations",
        retentionMs,
        MIN_RETENTION_POLL_CYCLES,
        pollIntervalMs);
    this.retentionMs = retentionMs;
    this.cleanupIntervalMs = cleanupIntervalMs;
  }

  /** Starts the dedicated cleanup thread when automatic cleanup is enabled. */
  public void start() {
    if (retentionMs == 0) {
      LOG.info("Automatic entity change log cleanup is disabled");
      return;
    }

    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("Gravitino-EntityChangeLogCleaner");
              thread.setDaemon(true);
              return thread;
            });
    long initialDelayMs = initialDelayMs();
    scheduler.scheduleWithFixedDelay(
        this::cleanExpiredChanges, initialDelayMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
    LOG.info(
        "Starting entity change log cleaner with retention {} ms, cleanup interval {} ms and "
            + "initial delay {} ms",
        retentionMs,
        cleanupIntervalMs,
        initialDelayMs);
  }

  /**
   * Returns a randomized short delay before the first run. It is deliberately much smaller than the
   * cleanup interval, otherwise a server restarted more often than that interval would never clean
   * up. The randomization spreads the first run of the HA nodes, which all delete from the same
   * rows.
   */
  @VisibleForTesting
  long initialDelayMs() {
    long bound = Math.min(cleanupIntervalMs, MAX_INITIAL_DELAY_MS);
    return ThreadLocalRandom.current().nextLong(bound) + 1;
  }

  @VisibleForTesting
  void cleanExpiredChanges() {
    if (retentionMs == 0) {
      return;
    }

    long totalPrunedRows = 0;
    try {
      int prunedRows;
      do {
        prunedRows =
            SessionUtils.doWithCommitAndFetchResult(
                EntityChangeLogMapper.class, mapper -> mapper.pruneOldEntityChanges(retentionMs));
        totalPrunedRows += prunedRows;
      } while (prunedRows == EntityChangeLogMapper.ENTITY_CHANGE_LOG_PRUNE_BATCH_SIZE
          && !Thread.currentThread().isInterrupted());

      if (totalPrunedRows > 0) {
        LOG.info(
            "Pruned {} entity change log record(s) older than {} ms, measured with database time",
            totalPrunedRows,
            retentionMs);
      } else {
        // Without this line a cleaner that never runs looks exactly like one that finds nothing.
        LOG.debug("No entity change log record older than {} ms to prune", retentionMs);
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to clean expired entity change logs after pruning {} record(s)",
          totalPrunedRows,
          e);
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
