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
package org.apache.gravitino.storage.relational;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global poller for {@code entity_change_log}.
 *
 * <p>The poller owns the single high-water mark for a Gravitino server process and dispatches each
 * consumed batch to registered listeners. Listeners should only perform idempotent local cache
 * invalidation. The cursor always advances after dispatch regardless of individual listener
 * failures, so a faulty listener cannot block other listeners or prevent pruning.
 */
public class EntityChangeLogPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);

  /** Max entity-change rows to fetch per poller cycle. */
  private static final int ENTITY_CHANGE_POLLER_MAX_ROWS = 500;

  private final List<EntityChangeLogListener> listeners = new CopyOnWriteArrayList<>();
  private final long pollIntervalSecs;
  private final long retentionMs;
  private final long cleanupIntervalMs;
  private final LongSupplier clockMs;

  private ScheduledExecutorService scheduler;
  private volatile long entityPollHighWaterId = 0;
  private volatile long lastCleanupMs = Long.MIN_VALUE;

  /**
   * Creates an {@link EntityChangeLogPoller}.
   *
   * @param pollIntervalSecs interval between successive polling cycles
   */
  public EntityChangeLogPoller(long pollIntervalSecs) {
    this(
        pollIntervalSecs,
        TimeUnit.DAYS.toMillis(1),
        TimeUnit.HOURS.toMillis(1),
        System::currentTimeMillis);
  }

  /**
   * Creates an {@link EntityChangeLogPoller}.
   *
   * @param pollIntervalSecs interval between successive polling cycles
   * @param retentionMs entity change retention in milliseconds, or 0 to disable cleanup
   * @param cleanupIntervalMs interval between successive cleanup attempts in milliseconds
   */
  public EntityChangeLogPoller(long pollIntervalSecs, long retentionMs, long cleanupIntervalMs) {
    this(pollIntervalSecs, retentionMs, cleanupIntervalMs, System::currentTimeMillis);
  }

  @VisibleForTesting
  EntityChangeLogPoller(
      long pollIntervalSecs, long retentionMs, long cleanupIntervalMs, LongSupplier clockMs) {
    Preconditions.checkArgument(pollIntervalSecs > 0, "pollIntervalSecs must be positive");
    Preconditions.checkArgument(retentionMs >= 0, "retentionMs must be non-negative");
    Preconditions.checkArgument(cleanupIntervalMs > 0, "cleanupIntervalMs must be positive");
    this.pollIntervalSecs = pollIntervalSecs;
    this.retentionMs = retentionMs;
    this.cleanupIntervalMs = cleanupIntervalMs;
    this.clockMs = clockMs;
  }

  /**
   * Registers a listener to receive future entity change batches.
   *
   * @param listener the listener to register
   */
  public void registerListener(EntityChangeLogListener listener) {
    Preconditions.checkArgument(listener != null, "listener cannot be null");
    listeners.add(listener);
  }

  /**
   * Unregisters a previously registered listener.
   *
   * @param listener the listener to unregister
   */
  public void unregisterListener(EntityChangeLogListener listener) {
    Preconditions.checkArgument(listener != null, "listener cannot be null");
    listeners.remove(listener);
  }

  /**
   * Initializes the high-water cursor to the current DB tail and schedules periodic polling.
   *
   * <p>On every start (including restarts), the cursor is set to the current maximum change ID in
   * the DB, so historical change records written before this server process started are NOT
   * replayed. This is intentional: on startup the local cache is cold, so there is no stale state
   * to invalidate. Only changes written after this server started need to be applied to the warming
   * cache.
   */
  public void start() {
    entityPollHighWaterId =
        getOrDefault(
            SessionUtils.getWithoutCommit(
                EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId));

    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setName("Gravitino-EntityChangeLogPoller");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleWithFixedDelay(
        this::pollChanges, pollIntervalSecs, pollIntervalSecs, TimeUnit.SECONDS);
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

  @VisibleForTesting
  void pollChanges() {
    try {
      doPollChanges();
    } catch (Exception e) {
      if (handleInterruptIfAny(e, "Entity change poll")) {
        return;
      }
      LOG.warn("Entity change poll failed", e);
    }
  }

  private synchronized void doPollChanges() {
    List<EntityChangeRecord> changes = fetchEntityChanges();
    if (changes.isEmpty()) {
      pruneExpiredChangesIfNeeded();
      return;
    }

    long maxSeenId = entityPollHighWaterId;
    for (EntityChangeRecord change : changes) {
      if (change.getId() > maxSeenId) {
        maxSeenId = change.getId();
      }
    }

    List<EntityChangeRecord> dispatchedChanges = Collections.unmodifiableList(changes);
    for (EntityChangeLogListener listener : listeners) {
      try {
        listener.onEntityChange(dispatchedChanges);
      } catch (Exception e) {
        LOG.warn("Entity change listener {} failed", listener.getClass().getName(), e);
      }
    }

    entityPollHighWaterId = maxSeenId;
    pruneExpiredChangesIfNeeded();
  }

  private List<EntityChangeRecord> fetchEntityChanges() {
    return SessionUtils.getWithoutCommit(
        EntityChangeLogMapper.class,
        m -> m.selectEntityChanges(entityPollHighWaterId, ENTITY_CHANGE_POLLER_MAX_ROWS));
  }

  private static boolean handleInterruptIfAny(Throwable e, String context) {
    Throwable t = e;
    while (t != null) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        LOG.debug("{} interrupted, stopping poll cycle", context);
        return true;
      }
      t = t.getCause();
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.debug("{} ran while thread was interrupted, stopping poll cycle", context);
      return true;
    }
    return false;
  }

  private void pruneExpiredChangesIfNeeded() {
    if (retentionMs <= 0) {
      return;
    }

    long now = clockMs.getAsLong();
    if (lastCleanupMs != Long.MIN_VALUE && now - lastCleanupMs < cleanupIntervalMs) {
      return;
    }

    long before = now - retentionMs;
    try {
      SessionUtils.doWithoutCommit(
          EntityChangeLogMapper.class, mapper -> mapper.pruneOldEntityChanges(before));
    } catch (Exception e) {
      LOG.warn("Failed to prune expired entity change logs before {}", before, e);
    } finally {
      // Always advance the cursor regardless of success or failure. A transient DB error
      // should not cause repeated prune attempts on every poll cycle (every few seconds)
      // until one eventually succeeds — the next cleanup will happen after cleanupIntervalMs.
      lastCleanupMs = now;
    }
  }

  private static long getOrDefault(Long value) {
    return value == null ? 0L : value;
  }
}
