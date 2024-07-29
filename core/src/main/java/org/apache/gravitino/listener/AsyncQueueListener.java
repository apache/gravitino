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

package org.apache.gravitino.listener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AsyncQueueListener acts as event listener, and internally buffer event to a queue, start a
 * dispatcher thread to dispatch event to the real listeners. For default AsyncQueueListener it may
 * contain multi listeners share with one queue and dispatcher thread. For other
 * AsyncQueueDispatchers, contain only one listener.
 */
public class AsyncQueueListener implements EventListenerPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncQueueListener.class);
  private static final String NAME_PREFIX = "async-queue-listener-";

  private final List<EventListenerPlugin> eventListeners;
  private final BlockingQueue<Event> queue;
  private final Thread asyncProcessor;
  private final int dispatcherJoinSeconds;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicLong dropEventCounters = new AtomicLong(0);
  private final AtomicLong lastDropEventCounters = new AtomicLong(0);
  private Instant lastRecordDropEventTime;
  private final String asyncQueueListenerName;

  public AsyncQueueListener(
      List<EventListenerPlugin> listeners,
      String name,
      int queueCapacity,
      int dispatcherJoinSeconds) {
    this.asyncQueueListenerName = NAME_PREFIX + name;
    this.eventListeners = listeners;
    this.queue = new LinkedBlockingQueue<>(queueCapacity);
    this.asyncProcessor = new Thread(() -> processEvents());
    this.dispatcherJoinSeconds = dispatcherJoinSeconds;
    asyncProcessor.setDaemon(true);
    asyncProcessor.setName(asyncQueueListenerName);
  }

  @Override
  public void onPostEvent(Event event) {
    if (stopped.get()) {
      LOG.warn(
          "{} drop event: {}, since AsyncQueueListener is stopped",
          asyncQueueListenerName,
          event.getClass().getSimpleName());
      return;
    }

    if (queue.offer(event)) {
      return;
    }

    logDropEventsIfNecessary();
  }

  @Override
  public void init(Map<String, String> properties) {
    throw new RuntimeException(
        "Should not reach here, the event listener has already been initialized.");
  }

  @Override
  public void start() {
    eventListeners.forEach(listenerPlugin -> listenerPlugin.start());
    asyncProcessor.start();
  }

  @Override
  public void stop() {
    Preconditions.checkState(!stopped.get(), asyncQueueListenerName + " had already stopped");
    stopped.compareAndSet(false, true);
    asyncProcessor.interrupt();
    try {
      asyncProcessor.join(dispatcherJoinSeconds * 1000L);
    } catch (InterruptedException e) {
      LOG.warn("{} interrupt async processor failed.", asyncQueueListenerName, e);
    }
    eventListeners.forEach(listenerPlugin -> listenerPlugin.stop());
  }

  @VisibleForTesting
  List<EventListenerPlugin> getEventListeners() {
    return this.eventListeners;
  }

  private void processEvents() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Event event = queue.take();
        this.eventListeners.forEach(listener -> listener.onPostEvent(event));
      } catch (InterruptedException e) {
        LOG.warn("{} event dispatcher thread is interrupted.", asyncQueueListenerName);
        break;
      } catch (Exception e) {
        LOG.warn("{} throw a exception while processing event", asyncQueueListenerName, e);
      }
    }

    if (!queue.isEmpty()) {
      LOG.warn(
          "{} drop {} events since dispatch thread is interrupted",
          asyncQueueListenerName,
          queue.size());
    }
  }

  private void logDropEventsIfNecessary() {
    long currentDropEvents = dropEventCounters.incrementAndGet();
    long lastDropEvents = lastDropEventCounters.get();
    // dropEvents may less than zero in such conditions:
    // 1. Thread A increment dropEventCounters
    // 2. Thread B increment dropEventCounters and update lastDropEventCounters
    // 3. Thread A get lastDropEventCounters
    long dropEvents = currentDropEvents - lastDropEvents;
    if (dropEvents > 0 && Instant.now().isAfter(lastRecordDropEventTime.plusSeconds(60))) {
      if (lastDropEventCounters.compareAndSet(lastDropEvents, currentDropEvents)) {
        LOG.warn(
            "{} drop {} events since {}",
            asyncQueueListenerName,
            dropEvents,
            lastRecordDropEventTime);
        lastRecordDropEventTime = Instant.now();
      }
    }
  }
}
