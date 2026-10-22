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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestAsyncQueueListener {
  private BlockingAppender blockingAppender;
  private LoggerContext loggerContext;

  @BeforeEach
  void setUp() {
    loggerContext =
        (LoggerContext) LogManager.getContext(AsyncQueueListener.class.getClassLoader(), false);
    Configuration configuration = loggerContext.getConfiguration();

    blockingAppender = new BlockingAppender("asyncQueueListenerCapture");
    blockingAppender.start();
    configuration.addAppender(blockingAppender);

    LoggerConfig loggerConfig =
        new LoggerConfig(AsyncQueueListener.class.getName(), Level.WARN, false);
    loggerConfig.addAppender(blockingAppender, Level.WARN, null);
    configuration.addLogger(AsyncQueueListener.class.getName(), loggerConfig);
    loggerContext.updateLoggers();
  }

  @AfterEach
  void tearDown() {
    blockingAppender.releaseFirstLog.countDown();
    AbstractConfiguration configuration = (AbstractConfiguration) loggerContext.getConfiguration();
    configuration.removeLogger(AsyncQueueListener.class.getName());
    blockingAppender.stop();
    configuration.removeAppender(blockingAppender.getName());
    loggerContext.updateLoggers();
  }

  @Test
  void testDropEventLogThrottlingIsAtomic() throws Exception {
    AsyncQueueListener listener = new AsyncQueueListener(List.of(), "test", 1, 1);
    Event event = mock(Event.class);
    listener.onPostEvent(event);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> firstDrop = executor.submit(() -> listener.onPostEvent(event));
      assertTrue(blockingAppender.firstLogEntered.await(30, TimeUnit.SECONDS));

      Future<?> secondDrop = executor.submit(() -> listener.onPostEvent(event));
      secondDrop.get(30, TimeUnit.SECONDS);

      blockingAppender.releaseFirstLog.countDown();
      firstDrop.get(30, TimeUnit.SECONDS);

      assertEquals(1, blockingAppender.logCount.get());
    } finally {
      blockingAppender.releaseFirstLog.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void testStopIsIdempotent() {
    // Do not block the WARN log the dispatcher thread writes when it is interrupted.
    blockingAppender.releaseFirstLog.countDown();
    EventListenerPlugin innerListener = mock(EventListenerPlugin.class);
    AsyncQueueListener listener = new AsyncQueueListener(List.of(innerListener), "test", 1, 1);
    listener.start();

    listener.stop();
    listener.stop();

    verify(innerListener, times(1)).stop();
  }

  @Test
  void testStopWhenCallerIsInterrupted() {
    blockingAppender.releaseFirstLog.countDown();
    EventListenerPlugin innerListener = mock(EventListenerPlugin.class);
    AsyncQueueListener listener =
        new AsyncQueueListener(List.of(innerListener), "interrupted-caller", 10, 1);
    listener.start();

    Thread.currentThread().interrupt();
    listener.stop();

    // The caller keeps its interrupt status, and the dispatcher thread is still stopped.
    assertTrue(Thread.interrupted());
    assertFalse(
        Thread.getAllStackTraces().keySet().stream()
            .anyMatch(
                thread ->
                    thread.getName().equals("async-queue-listener-interrupted-caller")
                        && thread.isAlive()));
    verify(innerListener, times(1)).stop();
  }

  @Test
  void testStopDrainsQueuedEvents() {
    blockingAppender.releaseFirstLog.countDown();
    AtomicInteger processedEvents = new AtomicInteger();
    EventListenerPlugin slowListener =
        new EventListenerPlugin() {
          @Override
          public void init(Map<String, String> properties) {}

          @Override
          public void start() {}

          @Override
          public void stop() {}

          @Override
          public void onPostEvent(Event event) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            processedEvents.incrementAndGet();
          }
        };
    AsyncQueueListener listener = new AsyncQueueListener(List.of(slowListener), "test", 10, 10);
    listener.start();

    Event event = mock(Event.class);
    for (int i = 0; i < 5; i++) {
      listener.onPostEvent(event);
    }
    listener.stop();

    assertEquals(5, processedEvents.get());
  }

  @Test
  void testStopInterruptsDispatcherThatDoesNotFinishInTime() throws Exception {
    blockingAppender.releaseFirstLog.countDown();
    CountDownLatch neverReleased = new CountDownLatch(1);
    CountDownLatch entered = new CountDownLatch(1);
    EventListenerPlugin blockedListener =
        new EventListenerPlugin() {
          @Override
          public void init(Map<String, String> properties) {}

          @Override
          public void start() {}

          @Override
          public void stop() {}

          @Override
          public void onPostEvent(Event event) {
            entered.countDown();
            try {
              neverReleased.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        };
    AsyncQueueListener listener = new AsyncQueueListener(List.of(blockedListener), "test", 10, 1);
    listener.start();
    listener.onPostEvent(mock(Event.class));
    assertTrue(entered.await(30, TimeUnit.SECONDS));

    long startNanos = System.nanoTime();
    listener.stop();

    // Stop waits at most twice dispatcherJoinSeconds: once to drain, once after the interrupt.
    // The dispatcher exits right after the interrupt, so stop() takes a little over 1 second.
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    assertTrue(elapsedMillis >= 900, "stop() returned before the drain timeout: " + elapsedMillis);
    assertTrue(elapsedMillis < 5000, "stop() took too long: " + elapsedMillis);
  }

  private static class BlockingAppender extends AbstractAppender {
    private final AtomicInteger logCount = new AtomicInteger();
    private final CountDownLatch firstLogEntered = new CountDownLatch(1);
    private final CountDownLatch releaseFirstLog = new CountDownLatch(1);

    BlockingAppender(String name) {
      super(name, null, PatternLayout.createDefaultLayout(), true, null);
    }

    @Override
    public void append(LogEvent event) {
      if (logCount.incrementAndGet() != 1) {
        return;
      }

      firstLogEntered.countDown();
      try {
        assertTrue(releaseFirstLog.await(30, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while waiting to release the first log", e);
      }
    }
  }
}
