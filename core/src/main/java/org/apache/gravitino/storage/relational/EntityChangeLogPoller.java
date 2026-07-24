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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global poller for {@code entity_change_log}.
 *
 * <p>The poller owns the single high-water mark for a Gravitino server process and dispatches each
 * consumed batch to registered listeners. The cursor is advanced only after every listener applies
 * the batch; if any listener fails, forward progress is paused and the same batch is retried only
 * for listeners that have not applied it yet, so a transient listener failure cannot silently drop
 * a batch's invalidations or re-deliver the batch to listeners that already succeeded. Because the
 * process owns one shared cursor, a persistently failing listener blocks progress for all
 * listeners; if the process restarts after the stuck rows age past the retention window, their
 * invalidations can be lost permanently (logged at ERROR).
 *
 * <p><b>Known limitation — the commit-ordering gap (currently accepted, not worked around).</b> The
 * cursor advances with {@code WHERE id > lastConsumedId ORDER BY id}. Auto-increment ids are
 * assigned at INSERT but a row only becomes visible at COMMIT, so when two change-log rows are
 * written concurrently the one with the smaller id can commit <i>after</i> the larger id has
 * already been consumed and the cursor advanced past it. {@code id > cursor} then never returns the
 * smaller-id row again, so its invalidation is permanently dropped on this node and the
 * corresponding cache entry can stay stale until it is otherwise reloaded or (for the catalog
 * cache) evicted by its {@code expireAfterAccess} TTL.
 *
 * <p><b>When / how likely.</b> It requires all of: (a) two or more transactions writing {@code
 * entity_change_log} with overlapping lifetimes, (b) the lower-id transaction committing later than
 * a higher-id one, and (c) a poll cycle landing between the two commits. So it only happens under
 * concurrent mutations and is rare in practice; for the catalog cache it is further bounded by TTL
 * eviction, which eventually refreshes a stale entry even if its invalidation was missed.
 *
 * <p><b>Why it is not fixed here.</b> No cheap, robust fix is currently known. Preventing the miss
 * by re-reading an overlapping {@code created_at} window re-delivers rows and is only safe if every
 * listener is perfectly idempotent — it is not: re-delivery once made the poller invalidate and
 * tear down an in-use catalog, closing its classloader and connection pool (a permanently cached
 * {@code NoClassDefFoundError}). Reliably <i>detecting</i> the miss is also impractical: the only
 * contiguity-independent signals need per-row bookkeeping that is lost on restart and is confused
 * by legitimate id gaps (e.g. {@code auto_increment_increment > 1}, sequence caching, rollbacks).
 * The gap is therefore documented and accepted; revisit with a dedicated design if it is shown to
 * matter in production.
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

  @Nullable private PausedBatch pausedBatch;

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
    if (pausedBatch != null) {
      retryPausedBatch();
      pruneExpiredChangesIfNeeded();
      return;
    }

    List<EntityChangeRecord> changes = fetchEntityChanges();

    if (!changes.isEmpty()) {
      long maxSeenId = entityPollHighWaterId;
      for (EntityChangeRecord change : changes) {
        if (change.getId() > maxSeenId) {
          maxSeenId = change.getId();
        }
      }

      List<EntityChangeRecord> dispatchedChanges = List.copyOf(changes);
      Set<EntityChangeLogListener> failedListeners = dispatchChanges(dispatchedChanges, null);

      // Only advance the cursor when every listener applied the batch. A listener failure must not
      // drop the batch's invalidations: keeping the cursor in place retries failed listeners with
      // this same batch on the next cycle until all listeners have applied it.
      if (failedListeners.isEmpty()) {
        entityPollHighWaterId = maxSeenId;
      } else {
        pausedBatch = new PausedBatch(dispatchedChanges, maxSeenId, failedListeners);
        // Forward progress is paused until every listener applies the batch; the same batch is
        // retried for failed listeners every cycle. If this persists and the process restarts after
        // retention cleanup prunes the stuck rows, their invalidations are lost permanently.
        // Surface at ERROR so operators can act.
        logPausedCursor();
      }
    }

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

  private void retryPausedBatch() {
    PausedBatch currentPausedBatch = pausedBatch;
    if (currentPausedBatch == null) {
      return;
    }

    Set<EntityChangeLogListener> failedListeners =
        dispatchChanges(currentPausedBatch.changes, currentPausedBatch.pendingListeners);
    if (failedListeners.isEmpty()) {
      entityPollHighWaterId = currentPausedBatch.maxSeenId;
      pausedBatch = null;
    } else {
      currentPausedBatch.pendingListeners = failedListeners;
      logPausedCursor();
    }
  }

  private Set<EntityChangeLogListener> dispatchChanges(
      List<EntityChangeRecord> dispatchedChanges,
      @Nullable Set<EntityChangeLogListener> pendingListenersOnly) {
    Set<EntityChangeLogListener> failedListeners = new HashSet<>();
    boolean retryOnlyPendingListeners = pendingListenersOnly != null;
    for (EntityChangeLogListener listener : listeners) {
      if (retryOnlyPendingListeners && !pendingListenersOnly.contains(listener)) {
        continue;
      }

      try {
        listener.onEntityChange(dispatchedChanges);
      } catch (Exception e) {
        failedListeners.add(listener);
        LOG.warn("Entity change listener {} failed", listener.getClass().getName(), e);
      }
    }
    return failedListeners;
  }

  private void logPausedCursor() {
    LOG.error(
        "Entity change cursor is paused at id {} because at least one listener failed to apply "
            + "the current batch; invalidations may be lost on restart if this is not resolved "
            + "before the stuck rows age past the retention window",
        entityPollHighWaterId);
  }

  private static class PausedBatch {
    private final List<EntityChangeRecord> changes;
    private final long maxSeenId;
    private Set<EntityChangeLogListener> pendingListeners;

    private PausedBatch(
        List<EntityChangeRecord> changes,
        long maxSeenId,
        Set<EntityChangeLogListener> pendingListeners) {
      this.changes = changes;
      this.maxSeenId = maxSeenId;
      this.pendingListeners = pendingListeners;
    }
  }
}
