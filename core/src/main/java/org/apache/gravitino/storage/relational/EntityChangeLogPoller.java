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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global poller for {@code entity_change_log}.
 *
 * <p>There is one poller per Gravitino server process, and it keeps one read position (the id of
 * the last row it has read). It reads a batch of rows, hands that batch to every listener once, and
 * then always moves the read position forward, even if a listener failed. The read position is
 * shared by all listeners, so holding it back to retry one listener would stop every other listener
 * from seeing new changes, and all the caches in this process would fall behind.
 *
 * <p>Because a batch is handed out only once and is never sent again, every listener must be able
 * to fix itself when it fails. The usual way to do that is to clear its whole cache: clearing
 * everything also covers whatever the listener failed to remove, so nothing stale is left behind.
 * The three listeners registered today do exactly that:
 *
 * <ul>
 *   <li>{@code EntityCacheChangeLogListener} clears its whole entity cache.
 *   <li>{@code JcasbinChangeListener} clears its whole {@code metadataIdCache}. A stale
 *       name&#8594;id entry there would be used by authorization checks.
 *   <li>{@code CatalogChangeLogListener} clears its whole catalog cache. Clearing it retires every
 *       cached catalog while operation leases defer {@code IsolatedClassLoader} cleanup for
 *       catalogs this process is still serving. This prevents stale reads without disrupting
 *       in-flight operations, and the clear only happens when a normal removal failed.
 * </ul>
 *
 * <p>Do not register a listener here if it cannot recover on its own.
 *
 * <p>Every listener failure is logged at {@code ERROR}, so a listener that keeps failing stays
 * visible in the logs even though the poller keeps going.
 */
public class EntityChangeLogPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);

  /** Max entity-change rows to fetch per batch. */
  private static final int ENTITY_CHANGE_POLLER_MAX_ROWS = 2000;

  /** Max records rendered in a batch summary log line. */
  private static final int MAX_SUMMARIZED_RECORDS = 20;

  private final List<EntityChangeLogListener> listeners = new CopyOnWriteArrayList<>();
  private final long pollIntervalSecs;

  private ScheduledExecutorService scheduler;
  private volatile long entityPollHighWaterId = 0;

  /**
   * Creates an {@link EntityChangeLogPoller}.
   *
   * @param pollIntervalSecs interval between successive polling cycles
   */
  public EntityChangeLogPoller(long pollIntervalSecs) {
    Preconditions.checkArgument(pollIntervalSecs > 0, "pollIntervalSecs must be positive");
    this.pollIntervalSecs = pollIntervalSecs;
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
    LOG.info(
        "Starting entity change log poller at high-water id {} with a {} second interval, "
            + "{} listener(s) registered",
        entityPollHighWaterId,
        pollIntervalSecs,
        listeners.size());

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

    // The final cursor tells where this node stopped consuming, which is the starting point when
    // comparing nodes after an incident.
    LOG.info("Stopped entity change log poller at high-water id {}", entityPollHighWaterId);
  }

  @VisibleForTesting
  void pollChanges() {
    try {
      doPollChanges();
    } catch (Throwable e) {
      // Catch Throwable, not Exception: this method is the task handed to
      // scheduleWithFixedDelay(), and anything that escapes it cancels all future runs for good,
      // silently. A listener or its recovery path can throw an Error as well as an Exception.
      // Losing the poller would stop cache invalidation for every listener in this process, so we
      // log and let the next cycle run.
      if (handleInterruptIfAny(e, "Entity change poll")) {
        return;
      }
      LOG.warn("Entity change poll failed at high-water id {}", entityPollHighWaterId, e);
    }
  }

  private synchronized void doPollChanges() {
    BatchDelivery delivery = fetchNextDelivery();
    if (delivery != null) {
      deliver(delivery);
    }
  }

  @Nullable
  private BatchDelivery fetchNextDelivery() {
    List<EntityChangeRecord> changes = fetchEntityChanges();
    if (changes.isEmpty()) {
      return null;
    }

    List<EntityChangeRecord> immutableChanges = List.copyOf(changes);
    long lastChangeId = immutableChanges.get(immutableChanges.size() - 1).getId();
    BatchDelivery delivery =
        new BatchDelivery(immutableChanges, lastChangeId, List.copyOf(listeners));
    LOG.debug(
        "Fetched {} entity change log record(s) after cursor {}, id range [{}, {}]: {}",
        immutableChanges.size(),
        entityPollHighWaterId,
        delivery.firstChangeId(),
        delivery.lastChangeId,
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
        listener.onEntityChange(delivery.changes);
        LOG.debug(
            "Entity change log listener {} consumed batch id range [{}, {}]",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId);
      } catch (Throwable e) {
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
