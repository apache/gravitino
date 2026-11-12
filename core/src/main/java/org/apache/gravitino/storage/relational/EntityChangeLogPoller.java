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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.Configs;
import org.apache.gravitino.metrics.source.EntityChangeLogMetricsSource;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global poller for {@code entity_change_log}.
 *
 * <p>There is one poller per Gravitino server process, and it keeps one read position (the id of
 * the last incremental row it has read). It reads a batch of rows, hands it to every listener, and
 * then always moves the read position forward, even if a listener failed. The read position is
 * shared by all listeners, so holding it back to retry one listener would stop every other listener
 * from seeing new changes, and all the caches in this process would fall behind.
 *
 * <p>Listener failures are not retried, so every listener must be able to fix itself when it fails.
 * The usual way to do that is to clear its whole cache: clearing everything also covers whatever
 * the listener failed to remove, so nothing stale is left behind. The three listeners registered
 * today do exactly that:
 *
 * <ul>
 *   <li>{@code EntityCacheChangeLogListener} clears its whole entity cache.
 *   <li>{@code JcasbinChangeListener} clears its whole {@code metadataIdCache}. A stale
 *       name&#8594;id entry there would be used by authorization checks.
 *   <li>{@code CatalogChangeLogListener} clears its whole catalog cache. Clearing it retires every
 *       cached catalog while operation leases defer {@code IsolatedClassLoader} cleanup for
 *       catalogs this process is still serving, so no operation is torn down mid-flight. Connector
 *       metadata is detached before its operation lease closes, so response processing does not
 *       retain a dependency on the retired catalog. The clear only happens when a normal removal
 *       failed.
 * </ul>
 *
 * <p>Do not register a listener here if it cannot recover on its own.
 *
 * <p>Every listener failure is logged at {@code ERROR}, so a listener that keeps failing stays
 * visible in the logs even though the poller keeps going.
 *
 * <p>Each poll cycle reads at most one incremental batch and one recent-history replay page. Replay
 * catches out-of-order commits within a configured record-age window, deduplicates recently
 * delivered IDs, and never moves the incremental cursor backwards. It does not guarantee delivery
 * beyond the window or after log pruning. Replay is delivered only to listeners opting into {@link
 * EntityChangeLogListener#supportsReplay()}. Their callbacks must be idempotent because bounded
 * deduplication may evict IDs and startup replay can include historical rows.
 *
 * <p>Each replay sweep rereads every row in the window, so a new sweep starts at most once per
 * {@code max(pollIntervalSecs, replayWindowSecs / 4)}. This bounds rereads to about four per row
 * and delays recovery of a late row by up to one sweep interval; a row is caught only if it becomes
 * visible before its recorded age exceeds the window minus one sweep interval.
 *
 * <p>A full incremental batch or replay page means more rows may be waiting, so the next cycle
 * starts right away instead of after the poll interval. When both reads are empty or partial, or
 * the cycle fails, the poller waits the full interval. A backlog is therefore drained as fast as
 * batches can be read and delivered, while an idle or failing poller still polls only once per
 * interval.
 */
public class EntityChangeLogPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);

  /** Max records rendered in a batch summary log line. */
  private static final int MAX_SUMMARIZED_RECORDS = 20;

  private final List<EntityChangeLogListener> listeners = new CopyOnWriteArrayList<>();
  private final long pollIntervalSecs;
  private final int batchSize;
  private final EntityChangeLogMetricsSource metrics;
  private final long replayWindowMs;
  private final int replayMaxEntries;
  // Minimum gap between the starts of two replay sweeps. Each sweep rereads the whole window, so
  // starting one every poll would reread every recent row about window/interval times.
  private final long replaySweepIntervalNanos;
  // Guarded by doPollChanges' monitor. Insertion order makes expiry and capacity eviction bounded.
  private final Map<Long, Long> rememberedIds = new LinkedHashMap<>();
  private long replayAfterCreatedAt = -1;
  private long replayAfterId;
  private long replayUpperId;
  private long startupHighWaterId;
  private boolean replayFailed;
  private long replayFailureNanos;
  private boolean capacityWarningReported;
  private long capacityWarningNanos;
  private long pendingDedupEvictions;
  private boolean replaySweepStarted;
  // The dedup set is empty until the first sweep completes, so every row it finds is expected
  // startup history rather than evidence of a late commit.
  private boolean replaySweepCompleted;
  private long lastReplaySweepStartNanos;

  private ScheduledExecutorService scheduler;
  private volatile boolean closed = false;
  private volatile long entityPollHighWaterId = 0;
  private boolean tailSampleAttempted;
  private long lastTailSampleNanos;

  /**
   * Creates an {@link EntityChangeLogPoller} with an unregistered metrics source for callers that
   * do not use the server metrics system.
   *
   * @param pollIntervalSecs interval between polling cycles once the poller has caught up
   */
  public EntityChangeLogPoller(long pollIntervalSecs) {
    this(pollIntervalSecs, new EntityChangeLogMetricsSource());
  }

  /**
   * Creates a poller using the metrics source registered by the entity store.
   *
   * @param pollIntervalSecs interval between polling cycles once the poller has caught up
   * @param metrics process-local change log metrics
   */
  public EntityChangeLogPoller(long pollIntervalSecs, EntityChangeLogMetricsSource metrics) {
    this(pollIntervalSecs, Configs.DEFAULT_ENTITY_CHANGE_LOG_POLL_BATCH_SIZE, metrics);
  }

  /**
   * Creates a poller with a custom batch size, using the metrics source registered by the entity
   * store.
   *
   * @param pollIntervalSecs interval between polling cycles once the poller has caught up
   * @param batchSize maximum number of change log rows read per polling cycle
   * @param metrics process-local change log metrics
   */
  public EntityChangeLogPoller(
      long pollIntervalSecs, int batchSize, EntityChangeLogMetricsSource metrics) {
    this(
        pollIntervalSecs,
        batchSize,
        Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_WINDOW_SECS,
        Configs.DEFAULT_ENTITY_CHANGE_LOG_REPLAY_MAX_ENTRIES,
        metrics);
  }

  /**
   * Creates a poller with bounded recent-history replay.
   *
   * @param pollIntervalSecs interval between caught-up polling cycles
   * @param batchSize maximum records per query
   * @param replayWindowSecs maximum stored timestamp age replayed, or zero to disable replay
   * @param replayMaxEntries maximum remembered IDs; eviction permits duplicate delivery
   * @param metrics process-local change log metrics
   */
  public EntityChangeLogPoller(
      long pollIntervalSecs,
      int batchSize,
      long replayWindowSecs,
      int replayMaxEntries,
      EntityChangeLogMetricsSource metrics) {
    Preconditions.checkArgument(
        replayWindowSecs >= 0 && replayWindowSecs <= 86400,
        "replayWindowSecs must be between 0 and 86400");
    Preconditions.checkArgument(replayMaxEntries > 0, "replayMaxEntries must be positive");
    this.replayWindowMs = TimeUnit.SECONDS.toMillis(replayWindowSecs);
    this.replayMaxEntries = replayMaxEntries;
    Preconditions.checkArgument(pollIntervalSecs > 0, "pollIntervalSecs must be positive");
    this.replaySweepIntervalNanos =
        TimeUnit.SECONDS.toNanos(Math.max(pollIntervalSecs, replayWindowSecs / 4));
    Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics cannot be null");
    this.pollIntervalSecs = pollIntervalSecs;
    this.batchSize = batchSize;
  }

  /**
   * Registers a listener to receive future entity change batches.
   *
   * <p>A listener only receives batches fetched after it was registered.
   *
   * @param listener the listener to register
   */
  public void registerListener(EntityChangeLogListener listener) {
    Preconditions.checkArgument(listener != null, "listener cannot be null");
    listeners.add(listener);
    LOG.info(
        "Registered entity change log listener {}, {} listener(s) active",
        listener.getClass().getName(),
        listeners.size());
  }

  /**
   * Unregisters a previously registered listener.
   *
   * @param listener the listener to unregister
   */
  public void unregisterListener(EntityChangeLogListener listener) {
    Preconditions.checkArgument(listener != null, "listener cannot be null");
    if (listeners.remove(listener)) {
      LOG.info(
          "Unregistered entity change log listener {}, {} listener(s) active",
          listener.getClass().getName(),
          listeners.size());
    }
  }

  /**
   * Initializes the high-water cursor to the current DB tail and schedules the first poll one
   * interval later. A poller cannot be started again after {@link #close()}.
   *
   * <p>On every start (including restarts), the cursor is set to the current maximum change ID in
   * the DB for the incremental stream. Recent-history replay also examines records at or below that
   * initial cursor: a transaction may have allocated a lower ID before startup but commit after the
   * local cache warms. Already committed recent rows can therefore be replayed too.
   */
  public void start() {
    Preconditions.checkState(!closed, "A closed entity change log poller cannot be restarted");
    entityPollHighWaterId =
        getOrDefault(
            SessionUtils.getWithoutCommit(
                EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId));
    startupHighWaterId = entityPollHighWaterId;
    LOG.info(
        "Entity change log replay windowMs={} maxRememberedIds={}; recorded created_at age is not commit age; "
            + "transactions older than the window or pruned history may still be missed",
        replayWindowMs,
        replayMaxEntries);
    metrics.setDbTailId(entityPollHighWaterId);
    tailSampleAttempted = true;
    lastTailSampleNanos = nanoTime();
    metrics.setCursorId(entityPollHighWaterId);
    LOG.info(
        "Starting entity change log poller at high-water id {} with a {} second interval and a "
            + "batch size of {}, {} listener(s) registered",
        entityPollHighWaterId,
        pollIntervalSecs,
        batchSize,
        listeners.size());

    scheduler = createScheduler();
    scheduleNextPoll(TimeUnit.SECONDS.toMillis(pollIntervalSecs));
  }

  @Override
  public void close() {
    closed = true;
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

    // The final cursor tells where this node stopped consuming, which is the starting point when
    // comparing nodes after an incident.
    LOG.info("Stopped entity change log poller at high-water id {}", entityPollHighWaterId);
  }

  @VisibleForTesting
  ScheduledExecutorService createScheduler() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread t = new Thread(r);
              t.setName("Gravitino-EntityChangeLogPoller");
              t.setDaemon(true);
              return t;
            });
    // Drop polls whose delay has not elapsed so close() does not wait for an idle poll. Already-due
    // tasks, including zero-delay drain hops, can still run after shutdown(); runPollCycle's closed
    // guard prevents those tasks from accessing the closing store.
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return executor;
  }

  @VisibleForTesting
  long nanoTime() {
    return System.nanoTime();
  }

  /**
   * Runs one poll cycle and schedules the next one: immediately when the cycle read a full batch,
   * otherwise after the poll interval. A failed cycle also waits the full interval, so a database
   * outage is not retried in a tight loop.
   */
  private void runPollCycle() {
    if (closed) {
      return;
    }
    long nextDelayMillis = TimeUnit.SECONDS.toMillis(pollIntervalSecs);
    try {
      if (pollChanges()) {
        // Full batches advance the incremental cursor or the replay page cursor. Each query is
        // bounded by batchSize. Tail sampling is rate-limited during full incremental batches;
        // replay-only drains still sample the tail when their incremental query is partial.
        nextDelayMillis = 0;
      }
    } finally {
      // pollChanges() catches Throwable, but scheduling in finally keeps the poller alive even if
      // that ever changes: a cycle that is not rescheduled stops cache invalidation for good.
      scheduleNextPoll(nextDelayMillis);
    }
  }

  private void scheduleNextPoll(long delayMillis) {
    if (closed) {
      return;
    }
    try {
      scheduler.schedule(this::runPollCycle, delayMillis, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Only a shut-down scheduler rejects work, and close() sets closed before shutting down.
      if (!closed) {
        LOG.error("Could not schedule the next entity change log poll", e);
      }
    }
  }

  /**
   * Polls an incremental batch and a bounded recent-history page, delivering unseen records.
   *
   * @return true if the incremental batch or the replay page was full, so more rows may be waiting
   *     and the next poll should not wait for the interval; false if both caught up or the poll
   *     failed
   */
  @VisibleForTesting
  boolean pollChanges() {
    try (Timer.Context ignored = metrics.timePoll()) {
      return doPollChanges();
    } catch (Throwable e) {
      // Catch Throwable, not Exception: a listener or its recovery path can throw an Error as well
      // as an Exception. Losing the poller would stop cache invalidation for every listener in
      // this process, so we log and let the next cycle run after the normal interval.
      if (handleInterruptIfAny(e, "Entity change poll")) {
        return false;
      }
      metrics.pollFailed();
      LOG.warn("Entity change poll failed at high-water id {}", entityPollHighWaterId, e);
      return false;
    }
  }

  private synchronized boolean doPollChanges() {
    BatchDelivery delivery = fetchNextDelivery();
    if (replayWindowMs > 0) {
      expireRememberedIds();
    }
    boolean incrementalBacklog = delivery != null && delivery.changes.size() >= batchSize;
    if (delivery != null) {
      deliver(delivery);
      remember(delivery.changes);
    }
    // Replay failure must not discard or delay delivery of a successfully fetched new batch.
    boolean replayBacklog = false;
    if (replayWindowMs > 0
        && entityPollHighWaterId > 0
        && (replayUpperId != 0 || replaySweepDue())
        && (!replayFailed
            || nanoTime() - replayFailureNanos >= TimeUnit.SECONDS.toNanos(pollIntervalSecs))) {
      try {
        replayBacklog = replayRecentChanges();
        replayFailed = false;
      } catch (RuntimeException e) {
        if (handleInterruptIfAny(e, "Entity change log replay")) {
          return false;
        }
        replayFailed = true;
        metrics.replayFailed();
        replayFailureNanos = nanoTime();
        LOG.warn(
            "Entity change log replay failed cursor={} replayAfterCreatedAt={} replayAfterId={} "
                + "replayUpperId={} windowMs={}; incremental delivery continues; replay retries after the poll interval",
            entityPollHighWaterId,
            replayAfterCreatedAt,
            replayAfterId,
            replayUpperId,
            replayWindowMs,
            e);
      }
    }
    return incrementalBacklog || replayBacklog;
  }

  private boolean replayRecentChanges() {
    if (replayUpperId == 0) {
      replaySweepStarted = true;
      lastReplaySweepStartNanos = nanoTime();
      replayUpperId = entityPollHighWaterId;
    }
    List<EntityChangeRecord> page =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class,
            m ->
                m.selectRecentEntityChanges(
                    replayAfterCreatedAt, replayAfterId, replayUpperId, replayWindowMs, batchSize));
    List<EntityChangeRecord> unseen = new ArrayList<>();
    for (EntityChangeRecord change : page) {
      if (!rememberedIds.containsKey(change.getId())) {
        unseen.add(change);
        // Absence from this bounded set is evidence of a possible late commit, not proof:
        // startup history and capacity eviction can also produce previously unseen IDs.
        LOG.debug(
            "Replaying previously unseen entity change behind cursor: changeId={} cursor={} "
                + "startupCursor={} entityType={} fullName={} operation={} createdAt={} approximateAgeMs={} "
                + "windowMs={}; possible late commit, startup history, or dedup eviction; "
                + "age uses JVM time and is not transaction duration",
            change.getId(),
            entityPollHighWaterId,
            startupHighWaterId,
            change.getEntityType(),
            change.getFullName(),
            change.getOperateType(),
            change.getCreatedAt(),
            System.currentTimeMillis() - change.getCreatedAt(),
            replayWindowMs);
      }
    }
    if (!unseen.isEmpty()) {
      metrics.replayRecordsFound(unseen.size());
      unseen.sort(Comparator.comparingLong(EntityChangeRecord::getId));
      if (!replaySweepCompleted) {
        LOG.info(
            "Entity change log startup replay delivered {} recent record(s) behind cursor={} "
                + "startupCursor={} windowMs={} records={}; expected after a restart because "
                + "deduplication starts empty",
            unseen.size(),
            entityPollHighWaterId,
            startupHighWaterId,
            replayWindowMs,
            summarize(unseen));
      } else {
        LOG.warn(
            "Entity change log replay found {} previously unseen record(s) behind cursor={} "
                + "startupCursor={} windowMs={} approximateOldestAgeMs={} records={}; "
                + "possible late commit or dedup eviction; record details at DEBUG; "
                + "age uses JVM time, not transaction duration",
            unseen.size(),
            entityPollHighWaterId,
            startupHighWaterId,
            replayWindowMs,
            System.currentTimeMillis()
                - unseen.stream().mapToLong(EntityChangeRecord::getCreatedAt).min().orElse(0),
            summarize(unseen));
      }
      List<EntityChangeRecord> immutable = List.copyOf(unseen);
      notifyListeners(
          new BatchDelivery(
              immutable, immutable.get(immutable.size() - 1).getId(), replayListeners()));
      remember(immutable);
    }
    if (page.size() < batchSize) {
      LOG.debug(
          "Entity change log replay sweep complete cursor={} upperId={} fetched={} replayed={} "
              + "deduplicated={} windowMs={}",
          entityPollHighWaterId,
          replayUpperId,
          page.size(),
          unseen.size(),
          page.size() - unseen.size(),
          replayWindowMs);
      replaySweepCompleted = true;
      replayAfterCreatedAt = -1;
      replayAfterId = 0;
      replayUpperId = 0;
      return false;
    }
    EntityChangeRecord last = page.get(page.size() - 1);
    replayAfterCreatedAt = last.getCreatedAt();
    replayAfterId = last.getId();
    LOG.debug(
        "Entity change log replay page cursor={} upperId={} afterCreatedAt={} afterId={} "
            + "fetched={} replayed={} deduplicated={}",
        entityPollHighWaterId,
        replayUpperId,
        replayAfterCreatedAt,
        replayAfterId,
        page.size(),
        unseen.size(),
        page.size() - unseen.size());
    return true;
  }

  /**
   * A new sweep starts at most once per sweep interval; pages of a sweep in progress are not
   * throttled. The first sweep after startup runs at once to cover history below the start cursor.
   * A row is caught when it becomes visible before its recorded age exceeds the window minus one
   * sweep interval.
   */
  private boolean replaySweepDue() {
    return !replaySweepStarted
        || nanoTime() - lastReplaySweepStartNanos >= replaySweepIntervalNanos;
  }

  private List<EntityChangeLogListener> replayListeners() {
    List<EntityChangeLogListener> eligible = new ArrayList<>();
    for (EntityChangeLogListener listener : listeners) {
      try {
        if (listener.supportsReplay()) {
          eligible.add(listener);
        }
      } catch (Throwable e) {
        LOG.warn(
            "Entity change log replay capability check failed listener={}; skipping its replay",
            listener.getClass().getName(),
            e);
      }
    }
    return List.copyOf(eligible);
  }

  private void expireRememberedIds() {
    long now = nanoTime();
    Iterator<Map.Entry<Long, Long>> entries = rememberedIds.entrySet().iterator();
    while (entries.hasNext()) {
      if (now - entries.next().getValue() < TimeUnit.MILLISECONDS.toNanos(replayWindowMs)) {
        break;
      }
      entries.remove();
    }
  }

  private void remember(List<EntityChangeRecord> changes) {
    if (replayWindowMs == 0) {
      return;
    }
    long now = nanoTime();
    int evicted = 0;
    for (EntityChangeRecord change : changes) {
      // Keep first-delivery time and insertion order aligned with prefix expiry. Seeing an ID
      // again in a replay query does not deliver it again or extend its deduplication lifetime.
      rememberedIds.putIfAbsent(change.getId(), now);
      if (rememberedIds.size() > replayMaxEntries) {
        Iterator<Long> ids = rememberedIds.keySet().iterator();
        ids.next();
        ids.remove();
        evicted++;
      }
    }
    if (evicted > 0) {
      metrics.dedupEvicted(evicted);
    }
    pendingDedupEvictions += evicted;
    if (pendingDedupEvictions > 0
        && (!capacityWarningReported
            || now - capacityWarningNanos >= TimeUnit.SECONDS.toNanos(pollIntervalSecs))) {
      capacityWarningReported = true;
      capacityWarningNanos = now;
      LOG.warn(
          "Entity change log replay dedup capacity reached cursor={} evictedIds={} maxEntries={} "
              + "windowMs={}; duplicate invalidations may occur; consider increasing replayMaxEntries",
          entityPollHighWaterId,
          pendingDedupEvictions,
          replayMaxEntries,
          replayWindowMs);
      pendingDedupEvictions = 0;
    }
  }

  @Nullable
  private BatchDelivery fetchNextDelivery() {
    long fetchStartNanos = System.nanoTime();
    List<EntityChangeRecord> changes = fetchEntityChanges();
    // The tail is for observability only. A failed sample must not suppress delivery of rows
    // already fetched successfully or hold the cursor back.
    @Nullable Long dbTailId = null;
    long nowNanos = nanoTime();
    if (changes.size() < batchSize
        || !tailSampleAttempted
        || nowNanos - lastTailSampleNanos >= TimeUnit.SECONDS.toNanos(pollIntervalSecs)) {
      // A full batch already signals backlog. Sample at most once per interval during a drain,
      // including failed attempts, while partial/empty polls refresh the caught-up state.
      tailSampleAttempted = true;
      lastTailSampleNanos = nowNanos;
      try {
        dbTailId =
            getOrDefault(
                SessionUtils.getWithoutCommit(
                    EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId));
        metrics.setDbTailId(dbTailId);
      } catch (RuntimeException e) {
        if (handleInterruptIfAny(e, "Entity change log tail sample")) {
          throw e;
        }
        metrics.tailSampleFailed();
        LOG.warn("Could not sample entity change log tail; retaining the previous gauge value", e);
      }
    }
    metrics.pollSucceeded(changes.size());
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fetchStartNanos);
    if (changes.isEmpty()) {
      LOG.debug(
          "entityChangeLog poll cursor={} fetched=0 tailId={} durationMs={}",
          entityPollHighWaterId,
          dbTailId,
          durationMs);
      return null;
    }

    List<EntityChangeRecord> immutableChanges = List.copyOf(changes);
    long lastChangeId = immutableChanges.get(immutableChanges.size() - 1).getId();
    BatchDelivery delivery =
        new BatchDelivery(immutableChanges, lastChangeId, List.copyOf(listeners));
    LOG.debug(
        "entityChangeLog poll cursor={} fetched={} firstId={} lastId={} tailId={} durationMs={} records={}",
        entityPollHighWaterId,
        immutableChanges.size(),
        delivery.firstChangeId(),
        delivery.lastChangeId,
        dbTailId,
        durationMs,
        summarize(immutableChanges));
    return delivery;
  }

  /**
   * Renders a batch as {@code id:ENTITY_TYPE:OPERATE_TYPE:fullName} entries, so a DEBUG log answers
   * "which invalidations did this node actually see" without querying the database. Long batches
   * are truncated, because the whole batch shares one id range that is already logged.
   */
  private static String summarize(List<EntityChangeRecord> changes) {
    StringBuilder builder = new StringBuilder();
    int limit = Math.min(changes.size(), MAX_SUMMARIZED_RECORDS);
    for (int i = 0; i < limit; i++) {
      EntityChangeRecord change = changes.get(i);
      if (i > 0) {
        builder.append(", ");
      }
      builder
          .append(change.getId())
          .append(':')
          .append(change.getEntityType())
          .append(':')
          .append(change.getOperateType())
          .append(':')
          .append(change.getFullName());
    }
    if (changes.size() > limit) {
      builder.append(", ... ").append(changes.size() - limit).append(" more");
    }
    return builder.toString();
  }

  private List<EntityChangeRecord> fetchEntityChanges() {
    return SessionUtils.getWithoutCommit(
        EntityChangeLogMapper.class, m -> m.selectEntityChanges(entityPollHighWaterId, batchSize));
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

  private static long getOrDefault(Long value) {
    return value == null ? 0L : value;
  }

  private void deliver(BatchDelivery delivery) {
    notifyListeners(delivery);
    advanceCursor(delivery);
  }

  private void advanceCursor(BatchDelivery delivery) {
    long previousHighWaterId = entityPollHighWaterId;
    entityPollHighWaterId = delivery.lastChangeId;
    metrics.setCursorId(entityPollHighWaterId);
    LOG.info(
        "Consumed {} entity change log record(s), id range [{}, {}]; cursor advanced from {} to {}; "
            + "newest record is ~{} ms old",
        delivery.changes.size(),
        delivery.firstChangeId(),
        delivery.lastChangeId,
        previousHighWaterId,
        entityPollHighWaterId,
        delivery.approximateLagMs());
  }

  /**
   * Hands the batch to every listener that is still registered. If a listener throws, the error is
   * only logged: each listener is expected to clean up after itself, and the read position moves
   * forward either way.
   */
  private void notifyListeners(BatchDelivery delivery) {
    for (EntityChangeLogListener listener : delivery.targetListeners) {
      if (!listeners.contains(listener)) {
        LOG.debug(
            "Skipping unregistered entity change log listener {} for batch id range [{}, {}]",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId);
        continue;
      }

      try {
        LOG.debug(
            "entityChangeLog delivery listener={} firstId={} lastId={} count={} attempt=1",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId,
            delivery.changes.size());
        listener.onEntityChange(delivery.changes);
        metrics.recordsDelivered(listenerMetricName(listener), delivery.changes.size());
        LOG.debug(
            "Entity change log listener {} consumed batch id range [{}, {}]",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId);
      } catch (Throwable e) {
        metrics.listenerFailed(listenerMetricName(listener));
        // Throwable, not Exception: one faulty listener must not take down the whole poller, even
        // if it fails with an Error rather than an Exception.
        LOG.error(
            "Entity change log listener {} failed to consume batch id range [{}, {}]; the batch is "
                + "not retried, so the listener is responsible for local recovery",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId,
            e);
      }
    }
  }

  /** Uses a bounded, stable metric bucket for lambda and anonymous listener implementations. */
  private static String listenerMetricName(EntityChangeLogListener listener) {
    Class<?> listenerClass = listener.getClass();
    return listenerClass.isSynthetic() || listenerClass.isAnonymousClass()
        ? "anonymous"
        : listenerClass.getName();
  }

  private static class BatchDelivery {
    private final List<EntityChangeRecord> changes;
    private final long lastChangeId;
    private final List<EntityChangeLogListener> targetListeners;

    private BatchDelivery(
        List<EntityChangeRecord> changes,
        long lastChangeId,
        List<EntityChangeLogListener> targetListeners) {
      this.changes = changes;
      this.lastChangeId = lastChangeId;
      this.targetListeners = targetListeners;
    }

    private long firstChangeId() {
      return changes.get(0).getId();
    }

    /**
     * How far this node is behind the newest record of the batch, in milliseconds. It compares the
     * DB-generated {@code created_at} with the local JVM clock, so it is only an estimate and can
     * even be negative when the two clocks disagree. It is still the quickest way to spot a node
     * that fell behind: a steadily growing value means this node is not keeping up.
     */
    private long approximateLagMs() {
      return System.currentTimeMillis() - changes.get(changes.size() - 1).getCreatedAt();
    }
  }
}
