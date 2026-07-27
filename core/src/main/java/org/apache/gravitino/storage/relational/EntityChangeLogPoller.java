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
import java.util.ArrayList;
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
 * <p>The poller owns the single high-water mark for a Gravitino server process and dispatches each
 * consumed batch to registered listeners. The cursor advances only after every listener applies the
 * batch. If a listener throws, the immutable batch stays in memory and the next poll retries only
 * the listeners that have not succeeded yet.
 *
 * <p>A listener that throws after partially applying a batch receives the whole batch again, so
 * listeners must make each callback atomic or tolerate retrying changes they already applied. A
 * listener that cannot satisfy that contract must swallow its own failures, as {@code
 * CatalogChangeLogListener} does.
 *
 * <p>Retries are bounded by {@code maxListenerRetries}. While a batch is paused no new batch is
 * fetched, so a permanently failing listener would otherwise freeze cache invalidation for the
 * whole process. When the bound is reached, {@link ListenerFailureAction} decides what happens:
 * {@code EXIT} stops this server (the local caches are known to be stale, so serving from them
 * would trade correctness for availability), {@code SKIP} drops the failed listeners from the batch
 * and advances the cursor.
 */
public class EntityChangeLogPoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EntityChangeLogPoller.class);

  /**
   * Max entity-change rows to fetch per batch. A paused batch is retained in memory until every
   * listener applies it, so this also bounds the poller's retained heap.
   */
  private static final int ENTITY_CHANGE_POLLER_MAX_ROWS = 2000;

  /** What the poller does when a listener keeps failing after {@code maxListenerRetries}. */
  public enum ListenerFailureAction {
    /** Stop this server process, because its local caches are known to be stale. */
    EXIT,
    /** Drop the failing listener from the batch, advance the cursor and keep serving. */
    SKIP
  }

  private final List<EntityChangeLogListener> listeners = new CopyOnWriteArrayList<>();
  private final long pollIntervalSecs;
  private final int maxListenerRetries;
  private final ListenerFailureAction listenerFailureAction;
  private final Runnable exitHandler;

  @Nullable private BatchDelivery pendingDelivery;

  private ScheduledExecutorService scheduler;
  private volatile long entityPollHighWaterId = 0;

  /**
   * Creates an {@link EntityChangeLogPoller}.
   *
   * @param pollIntervalSecs interval between successive polling cycles
   * @param maxListenerRetries how many times a failing listener is retried for the same batch
   *     before {@code listenerFailureAction} is applied
   * @param listenerFailureAction what to do once a listener exhausted its retries
   */
  public EntityChangeLogPoller(
      long pollIntervalSecs, int maxListenerRetries, ListenerFailureAction listenerFailureAction) {
    // System.exit() runs the JVM shutdown hooks, which is where GravitinoServer performs its
    // graceful stop, so in-flight requests still get a chance to finish.
    this(pollIntervalSecs, maxListenerRetries, listenerFailureAction, () -> System.exit(1));
  }

  @VisibleForTesting
  EntityChangeLogPoller(
      long pollIntervalSecs,
      int maxListenerRetries,
      ListenerFailureAction listenerFailureAction,
      Runnable exitHandler) {
    Preconditions.checkArgument(pollIntervalSecs > 0, "pollIntervalSecs must be positive");
    Preconditions.checkArgument(maxListenerRetries >= 0, "maxListenerRetries must be non-negative");
    Preconditions.checkArgument(
        listenerFailureAction != null, "listenerFailureAction cannot be null");
    this.pollIntervalSecs = pollIntervalSecs;
    this.maxListenerRetries = maxListenerRetries;
    this.listenerFailureAction = listenerFailureAction;
    this.exitHandler = exitHandler;
  }

  /**
   * Registers a listener to receive future entity change batches.
   *
   * <p>A listener only receives batches fetched after it was registered. In particular, if a batch
   * is currently paused by a failing listener, the newly registered listener does not receive that
   * batch and the cursor moves past it once the batch completes.
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
    LOG.info(
        "Starting entity change log poller at high-water id {} with a {} second interval",
        entityPollHighWaterId,
        pollIntervalSecs);

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
    BatchDelivery delivery = pendingDelivery;
    if (delivery != null) {
      LOG.debug(
          "Retrying entity change log batch with {} record(s), id range [{}, {}], for {} pending "
              + "listener(s)",
          delivery.changes.size(),
          delivery.firstChangeId(),
          delivery.lastChangeId,
          delivery.pendingListeners.size());
      deliver(delivery);
      return;
    }

    delivery = fetchNextDelivery();
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
        new BatchDelivery(immutableChanges, lastChangeId, List.copyOf(listeners), 1);
    LOG.debug(
        "Fetched {} entity change log record(s) after cursor {}, id range [{}, {}]",
        immutableChanges.size(),
        entityPollHighWaterId,
        delivery.firstChangeId(),
        delivery.lastChangeId);
    return delivery;
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
    List<EntityChangeLogListener> failedListeners = notifyListeners(delivery);
    if (failedListeners.isEmpty()) {
      advanceCursor(delivery);
      return;
    }

    if (delivery.attempts > maxListenerRetries) {
      handleExhaustedRetries(delivery, failedListeners);
      return;
    }

    pendingDelivery = delivery.retryOnly(failedListeners);
    LOG.error(
        "Entity change log cursor is paused at id {} because {} listener(s) failed to apply batch "
            + "id range [{}, {}] (attempt {} of {})",
        entityPollHighWaterId,
        failedListeners.size(),
        delivery.firstChangeId(),
        delivery.lastChangeId,
        delivery.attempts,
        maxListenerRetries + 1);
  }

  private void advanceCursor(BatchDelivery delivery) {
    long previousHighWaterId = entityPollHighWaterId;
    entityPollHighWaterId = delivery.lastChangeId;
    pendingDelivery = null;
    LOG.info(
        "Consumed {} entity change log record(s), id range [{}, {}]; cursor advanced from {} to {}",
        delivery.changes.size(),
        delivery.firstChangeId(),
        delivery.lastChangeId,
        previousHighWaterId,
        entityPollHighWaterId);
  }

  private void handleExhaustedRetries(
      BatchDelivery delivery, List<EntityChangeLogListener> failedListeners) {
    List<String> failedListenerNames = new ArrayList<>();
    for (EntityChangeLogListener listener : failedListeners) {
      failedListenerNames.add(listener.getClass().getName());
    }

    if (listenerFailureAction == ListenerFailureAction.EXIT) {
      LOG.error(
          "Stopping this server: listener(s) {} failed to apply entity change log batch id range "
              + "[{}, {}] after {} attempt(s), so local caches are stale and cannot be trusted",
          failedListenerNames,
          delivery.firstChangeId(),
          delivery.lastChangeId,
          delivery.attempts);
      exitHandler.run();
      return;
    }

    LOG.error(
        "Dropping entity change log batch id range [{}, {}] for listener(s) {} after {} attempt(s);"
            + " their local caches may be stale until the affected entries expire",
        delivery.firstChangeId(),
        delivery.lastChangeId,
        failedListenerNames,
        delivery.attempts);
    advanceCursor(delivery);
  }

  private List<EntityChangeLogListener> notifyListeners(BatchDelivery delivery) {
    List<EntityChangeLogListener> failedListeners = new ArrayList<>();
    for (EntityChangeLogListener listener : delivery.pendingListeners) {
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
      } catch (Exception e) {
        failedListeners.add(listener);
        LOG.warn(
            "Entity change log listener {} failed to consume batch id range [{}, {}]",
            listener.getClass().getName(),
            delivery.firstChangeId(),
            delivery.lastChangeId,
            e);
      }
    }
    return failedListeners;
  }

  private static class BatchDelivery {
    private final List<EntityChangeRecord> changes;
    private final long lastChangeId;
    private final List<EntityChangeLogListener> pendingListeners;

    /** How many times this batch has been dispatched, starting at 1 for the initial dispatch. */
    private final int attempts;

    private BatchDelivery(
        List<EntityChangeRecord> changes,
        long lastChangeId,
        List<EntityChangeLogListener> pendingListeners,
        int attempts) {
      this.changes = changes;
      this.lastChangeId = lastChangeId;
      this.pendingListeners = pendingListeners;
      this.attempts = attempts;
    }

    private BatchDelivery retryOnly(List<EntityChangeLogListener> failedListeners) {
      return new BatchDelivery(changes, lastChangeId, List.copyOf(failedListeners), attempts + 1);
    }

    private long firstChangeId() {
      return changes.get(0).getId();
    }
  }
}
