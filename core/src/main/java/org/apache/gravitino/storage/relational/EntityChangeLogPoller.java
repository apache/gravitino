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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
 * invalidation. The cursor is advanced only after every listener applies the batch; if any listener
 * fails, forward progress is paused and the same batch is re-dispatched on subsequent cycles until
 * all listeners succeed, so a transient listener failure cannot silently drop a batch's
 * invalidations. Because the process owns one shared cursor, a persistently failing listener blocks
 * progress for all listeners until the stuck rows age past the retention window (logged at ERROR).
 */
public class EntityChangeLogPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);

  /** Max entity-change rows to fetch per poller cycle. */
  private static final int ENTITY_CHANGE_POLLER_MAX_ROWS = 500;

  /**
   * Upper bound on the number of candidate "missed id" gaps tracked at once, so the detection state
   * can never grow without bound regardless of write/rollback patterns.
   */
  private static final int MAX_TRACKED_GAP_IDS = 10_000;

  /**
   * Gaps wider than this are not tracked as missed-row candidates. A real commit-ordering gap (a
   * few concurrent in-flight transactions whose ids are interleaved with their commit order) is
   * narrow; a wide gap is almost always rolled-back/abandoned auto-increment ids that will never
   * commit, and tracking them would only add noise.
   */
  private static final long MAX_GAP_WIDTH = 256;

  /**
   * Candidate gap ids further than this below the cursor are dropped as stale (never committed).
   */
  private static final long GAP_STALE_LOOKBACK = 1_000_000;

  private final List<EntityChangeLogListener> listeners = new CopyOnWriteArrayList<>();
  private final long pollIntervalSecs;
  private final long retentionMs;
  private final long cleanupIntervalMs;
  private final LongSupplier clockMs;

  /**
   * Auto-increment ids below the cursor that were absent when the cursor advanced past them. If
   * such an id later becomes visible it was committed after the {@code id > cursor} query had
   * already skipped it — i.e. a permanently missed change-log row (see {@link
   * #detectFilledGaps()}). This is observability-only state; it never affects dispatch. Guarded by
   * {@code doPollChanges}' monitor.
   */
  private final TreeSet<Long> pendingGapIds = new TreeSet<>();

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

    if (!changes.isEmpty()) {
      long previousCursor = entityPollHighWaterId;
      Set<Long> receivedIds = new HashSet<>();
      long maxSeenId = entityPollHighWaterId;
      for (EntityChangeRecord change : changes) {
        receivedIds.add(change.getId());
        if (change.getId() > maxSeenId) {
          maxSeenId = change.getId();
        }
      }

      List<EntityChangeRecord> dispatchedChanges = Collections.unmodifiableList(changes);
      boolean allListenersSucceeded = true;
      for (EntityChangeLogListener listener : listeners) {
        try {
          listener.onEntityChange(dispatchedChanges);
        } catch (Exception e) {
          allListenersSucceeded = false;
          LOG.warn("Entity change listener {} failed", listener.getClass().getName(), e);
        }
      }

      // Only advance the cursor when every listener applied the batch. A listener failure must not
      // drop the batch's invalidations: keeping the cursor in place re-dispatches the same batch on
      // the next cycle until all listeners succeed. Listeners are idempotent, so re-dispatching to
      // an already-applied listener is harmless.
      if (allListenersSucceeded) {
        entityPollHighWaterId = maxSeenId;
        recordNewGaps(previousCursor, maxSeenId, receivedIds);
      } else {
        // Forward progress is paused until every listener applies the batch; the same batch is
        // re-dispatched every cycle. If this persists, the stuck rows will eventually be pruned by
        // retention cleanup and their invalidations lost permanently, leaving caches to serve stale
        // data. Surface at ERROR so operators can act.
        LOG.error(
            "Entity change cursor is paused at id {} because at least one listener failed to apply "
                + "the current batch; invalidations will be lost if this is not resolved before the "
                + "stuck rows age past the retention window",
            entityPollHighWaterId);
      }
    }

    // A missed row's id is below the cursor, so the id>cursor fetch above never returns it; the
    // fill check must therefore run on every cycle, including cycles where the fetch was empty.
    detectFilledGaps();
    pruneExpiredChangesIfNeeded();
  }

  /**
   * Records ids in {@code (previousCursor, maxSeenId]} that were absent from this batch as
   * candidate missed rows. Narrow gaps only (see {@link #MAX_GAP_WIDTH}) and bounded in total.
   * Observability only; does not affect dispatch or the cursor.
   */
  private void recordNewGaps(long previousCursor, long maxSeenId, Set<Long> receivedIds) {
    for (long id = previousCursor + 1; id <= maxSeenId; id++) {
      if (id > previousCursor + MAX_GAP_WIDTH && pendingGapIds.isEmpty()) {
        // The batch's id span is far wider than a plausible concurrent-commit gap and we have no
        // gaps to confirm; treat the bulk as rolled-back ids rather than scanning the whole span.
        break;
      }
      if (!receivedIds.contains(id)) {
        pendingGapIds.add(id);
      }
    }
    while (pendingGapIds.size() > MAX_TRACKED_GAP_IDS) {
      pendingGapIds.pollFirst();
    }
  }

  /**
   * Logs any previously recorded gap id that has since become visible in the DB: such a row was
   * committed after the {@code id > cursor} cursor had already advanced past it, so it was
   * permanently skipped — a missed cache invalidation. Purely diagnostic; it re-reads from below
   * the lowest pending gap and never dispatches the rows. Stale gaps (far below the cursor, never
   * committed) are pruned so the lookback stays cheap.
   */
  private void detectFilledGaps() {
    if (pendingGapIds.isEmpty()) {
      return;
    }

    long staleFloor = entityPollHighWaterId - GAP_STALE_LOOKBACK;
    pendingGapIds.headSet(staleFloor).clear();
    if (pendingGapIds.isEmpty()) {
      return;
    }

    long lookbackFrom = pendingGapIds.first() - 1;
    List<EntityChangeRecord> filled =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class,
            m -> m.selectEntityChanges(lookbackFrom, ENTITY_CHANGE_POLLER_MAX_ROWS));
    for (EntityChangeRecord record : filled) {
      if (pendingGapIds.remove(record.getId())) {
        LOG.warn(
            "entity_change_log MISSED a change row (commit-ordering gap): id={} fullName={} "
                + "entityType={} operateType={} became visible below the poll cursor (current "
                + "cursor id={}) and was permanently skipped by the id>cursor query, so its cache "
                + "invalidation was dropped on this node",
            record.getId(),
            record.getFullName(),
            record.getEntityType(),
            record.getOperateType(),
            entityPollHighWaterId);
      }
    }
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

  @VisibleForTesting
  Set<Long> pendingGapIds() {
    return pendingGapIds;
  }
}
