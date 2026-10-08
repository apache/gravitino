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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.metrics.source.EntityChangeLogMetricsSource;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

public class TestEntityChangeLogPoller {

  private static final int MAX_ROWS = 2000;

  @Test
  void testRejectsInvalidConfiguration() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new EntityChangeLogPoller(0));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new EntityChangeLogPoller(-1));
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new EntityChangeLogPoller(1, 0, metrics));
  }

  @Test
  void testPollChangesReportsBacklogOnlyForFullBatch() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, 2)).thenReturn(changes(1L, 2L));
    when(mapper.selectEntityChanges(2L, 2)).thenReturn(changes(3L, 3L));
    when(mapper.selectEntityChanges(3L, 2)).thenReturn(List.of());
    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, 2, metrics);
      poller.registerListener(received::addAll);

      Assertions.assertTrue(poller.pollChanges(), "a full batch may leave a backlog");
      Assertions.assertFalse(poller.pollChanges(), "a partial batch means the poller caught up");
      Assertions.assertFalse(poller.pollChanges(), "an empty poll means the poller caught up");
      Assertions.assertEquals(
          3L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    }

    Assertions.assertEquals(List.of(1L, 2L, 3L), ids(received));
  }

  @Test
  void testFullBatchSchedulesNextPollWithoutWaitingForTheInterval() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, 2)).thenReturn(changes(1L, 2L));
    when(mapper.selectEntityChanges(2L, 2)).thenReturn(changes(3L, 3L));
    when(mapper.selectEntityChanges(3L, 2)).thenThrow(new RuntimeException("database down"));
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ArgumentCaptor<Runnable> cycle = ArgumentCaptor.forClass(Runnable.class);
    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller =
          spy(new EntityChangeLogPoller(5, 2, new EntityChangeLogMetricsSource()));
      doReturn(scheduler).when(poller).createScheduler();
      poller.registerListener(received::addAll);

      poller.start();
      InOrder schedules = inOrder(scheduler);
      schedules.verify(scheduler).schedule(cycle.capture(), eq(5000L), eq(TimeUnit.MILLISECONDS));

      // A full batch: more rows may be waiting, so the next cycle starts right away.
      cycle.getValue().run();
      schedules.verify(scheduler).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));

      // A partial batch: caught up, so wait the normal interval.
      cycle.getValue().run();
      schedules
          .verify(scheduler)
          .schedule(any(Runnable.class), eq(5000L), eq(TimeUnit.MILLISECONDS));

      // A failed poll keeps the normal interval instead of retrying in a tight loop.
      cycle.getValue().run();
      schedules
          .verify(scheduler)
          .schedule(any(Runnable.class), eq(5000L), eq(TimeUnit.MILLISECONDS));

      // Once closed, a cycle that is still running does not schedule another one.
      poller.close();
      cycle.getValue().run();
      verify(scheduler, times(4)).schedule(any(Runnable.class), anyLong(), any());
    }

    Assertions.assertEquals(List.of(1L, 2L, 3L), ids(received));
  }

  @Test
  void testDrainSamplesTailOncePerIntervalAndRefreshesAfterCatchingUp() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, 2)).thenReturn(changes(1L, 2L));
    when(mapper.selectEntityChanges(2L, 2)).thenReturn(changes(3L, 4L));
    when(mapper.selectEntityChanges(4L, 2)).thenReturn(changes(5L, 6L));
    when(mapper.selectEntityChanges(6L, 2)).thenReturn(changes(7L, 7L));
    when(mapper.selectEntityChanges(7L, 2)).thenReturn(List.of());
    when(mapper.selectMaxChangeId()).thenReturn(10L, 12L, 13L, 14L);
    AtomicLong clock = new AtomicLong();
    List<EntityChangeRecord> received = new ArrayList<>();
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(5, 2, metrics));
    doAnswer(invocation -> clock.get()).when(poller).nanoTime();
    poller.registerListener(received::addAll);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      Assertions.assertTrue(poller.pollChanges());
      clock.set(TimeUnit.SECONDS.toNanos(1));
      Assertions.assertTrue(poller.pollChanges());
      verify(mapper, times(1)).selectMaxChangeId();
      Assertions.assertEquals(
          10L, metrics.getMetricRegistry().getGauges().get("db-tail-id").getValue());

      clock.set(TimeUnit.SECONDS.toNanos(5));
      Assertions.assertTrue(poller.pollChanges());
      verify(mapper, times(2)).selectMaxChangeId();
      Assertions.assertFalse(poller.pollChanges());
      Assertions.assertFalse(poller.pollChanges());
      verify(mapper, times(4)).selectMaxChangeId();
    }
    Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L), ids(received));
    Assertions.assertEquals(
        7L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    Assertions.assertEquals(
        14L, metrics.getMetricRegistry().getGauges().get("db-tail-id").getValue());
  }

  @Test
  void testFailedTailSamplesAreAlsoRateLimitedDuringDrain() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, 2)).thenReturn(changes(1L, 2L));
    when(mapper.selectEntityChanges(2L, 2)).thenReturn(changes(3L, 4L));
    when(mapper.selectEntityChanges(4L, 2)).thenReturn(changes(5L, 6L));
    when(mapper.selectMaxChangeId()).thenThrow(new RuntimeException("tail unavailable"));
    AtomicLong clock = new AtomicLong();
    List<EntityChangeRecord> received = new ArrayList<>();
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(5, 2, metrics));
    doAnswer(invocation -> clock.get()).when(poller).nanoTime();
    poller.registerListener(received::addAll);
    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      Assertions.assertTrue(poller.pollChanges());
      clock.set(TimeUnit.SECONDS.toNanos(1));
      Assertions.assertTrue(poller.pollChanges());
      verify(mapper, times(1)).selectMaxChangeId();
      clock.set(TimeUnit.SECONDS.toNanos(5));
      Assertions.assertTrue(poller.pollChanges());
      verify(mapper, times(2)).selectMaxChangeId();
    }
    Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), ids(received));
    Assertions.assertEquals(
        6L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    Assertions.assertEquals(
        2, metrics.getMetricRegistry().counter("tail-sample-failures-total").getCount());
    Assertions.assertEquals(
        0, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
  }

  @Test
  void testSchedulingRejectionDoesNotEscapeStart() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    doThrow(new RejectedExecutionException("scheduler stopped"))
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any());
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(1));
    doReturn(scheduler).when(poller).createScheduler();
    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      Assertions.assertDoesNotThrow(poller::start);
      verify(scheduler).schedule(any(Runnable.class), anyLong(), any());
      poller.close();
    }
  }

  @Test
  void testSchedulingRejectionWhileClosingDoesNotEscapeStart() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(1));
    doReturn(scheduler).when(poller).createScheduler();
    doAnswer(
            invocation -> {
              poller.close();
              throw new RejectedExecutionException("closed during scheduling");
            })
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any());
    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      Assertions.assertDoesNotThrow(poller::start);
      verify(scheduler).shutdown();
      verify(scheduler).schedule(any(Runnable.class), anyLong(), any());
    }
  }

  @Test
  void testPollChangesDispatchesSameBatchToAllListenersAndAdvancesCursor() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "SCHEMA", "ml1.cat1.sch1");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of(first, second));
    when(mapper.selectEntityChanges(2L, MAX_ROWS)).thenReturn(List.of());

    List<EntityChangeRecord> firstListenerRecords = new ArrayList<>();
    List<EntityChangeRecord> secondListenerRecords = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, metrics);
      poller.registerListener(firstListenerRecords::addAll);
      poller.registerListener(secondListenerRecords::addAll);

      poller.pollChanges();
      poller.pollChanges();
      Assertions.assertEquals(
          4, metrics.getMetricRegistry().counter("records-delivered-total").getCount());
      Assertions.assertEquals(
          4, metrics.getMetricRegistry().counter("records-delivered.anonymous-total").getCount());
    }

    Assertions.assertEquals(List.of(first, second), firstListenerRecords);
    Assertions.assertEquals(List.of(first, second), secondListenerRecords);
    verify(mapper).selectEntityChanges(2L, MAX_ROWS);
  }

  @Test
  void testThrowingListenerNeitherPausesCursorNorBlocksOtherListeners() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "CATALOG", "ml1.cat2");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of(first));
    when(mapper.selectEntityChanges(1L, MAX_ROWS)).thenReturn(List.of(second));
    when(mapper.selectEntityChanges(2L, MAX_ROWS)).thenReturn(List.of());

    AtomicInteger throwingListenerCalls = new AtomicInteger();
    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(
          changes -> {
            throwingListenerCalls.incrementAndGet();
            throw new RuntimeException("listener failed");
          });
      poller.registerListener(received::addAll);

      poller.pollChanges();
      poller.pollChanges();
      poller.pollChanges();
    }

    // Each batch is handed out exactly once: the failing listener never gets a batch a second
    // time, the healthy listener still gets every batch, and the read position moves past both.
    Assertions.assertEquals(2, throwingListenerCalls.get());
    Assertions.assertEquals(List.of(first, second), received);
    verify(mapper).selectEntityChanges(1L, MAX_ROWS);
    verify(mapper).selectEntityChanges(2L, MAX_ROWS);
  }

  @Test
  void testListenerThrowingErrorDoesNotKillThePoller() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "CATALOG", "ml1.cat2");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of(first));
    when(mapper.selectEntityChanges(1L, MAX_ROWS)).thenReturn(List.of(second));
    when(mapper.selectEntityChanges(2L, MAX_ROWS)).thenReturn(List.of());

    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      // A listener that clears its catalog cache can close an IsolatedClassLoader that is still in
      // use, and a request holding a class from it then fails with NoClassDefFoundError, an Error
      // rather than an Exception. pollChanges() runs inside each scheduled cycle, so
      // anything escaping it would cancel every future poll and freeze invalidation process-wide.
      poller.registerListener(
          changes -> {
            throw new NoClassDefFoundError("closed isolated classloader");
          });
      poller.registerListener(received::addAll);

      Assertions.assertDoesNotThrow(poller::pollChanges);
      Assertions.assertDoesNotThrow(poller::pollChanges);
      Assertions.assertDoesNotThrow(poller::pollChanges);
    }

    Assertions.assertEquals(List.of(first, second), received);
    verify(mapper).selectEntityChanges(1L, MAX_ROWS);
    verify(mapper).selectEntityChanges(2L, MAX_ROWS);
  }

  @Test
  void testUnregisteredListenerIsSkipped() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "CATALOG", "ml1.cat2");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of(first));
    when(mapper.selectEntityChanges(1L, MAX_ROWS)).thenReturn(List.of(second));

    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      EntityChangeLogListener listener = received::addAll;
      poller.registerListener(listener);

      poller.pollChanges();
      poller.unregisterListener(listener);
      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(first), received);
    verify(mapper).selectEntityChanges(1L, MAX_ROWS);
  }

  @Test
  void testConsumesBacklogLargerThanOneBatch() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    List<EntityChangeRecord> firstBatch = changes(1L, MAX_ROWS);
    EntityChangeRecord remainingChange = change(2001L, "TABLE", "ml1.cat1.schema1.table2001");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(firstBatch);
    when(mapper.selectEntityChanges(2000L, MAX_ROWS)).thenReturn(List.of(remainingChange));

    List<EntityChangeRecord> received = new ArrayList<>();
    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(received::addAll);

      poller.pollChanges();
      poller.pollChanges();
    }

    Assertions.assertEquals(2001, received.size());
    Assertions.assertEquals(remainingChange, received.get(2000));
  }

  @Test
  void testPollChangesCatchesFetchFailures() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenThrow(new RuntimeException("db failed"));

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, metrics);

      Assertions.assertDoesNotThrow(poller::pollChanges);
      Assertions.assertEquals(
          1, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
      Assertions.assertEquals(
          0, metrics.getMetricRegistry().counter("records-fetched-total").getCount());
    }
  }

  @Test
  void testTailSampleFailureDoesNotSuppressFetchedBatch() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord change = change(1L, "CATALOG", "ml1.cat1");
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of(change));
    when(mapper.selectMaxChangeId()).thenThrow(new RuntimeException("tail query failed"));
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, metrics);
      poller.registerListener(received::addAll);
      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(change), received);
    Assertions.assertEquals(
        1L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    // The tail was never sampled: the gauge stays at its initial value and the cursor moves past
    // it, which clamps record-lag to zero. The freshness gauge is what reveals that state.
    Assertions.assertEquals(
        0L, metrics.getMetricRegistry().getGauges().get("db-tail-id").getValue());
    Assertions.assertEquals(
        0L, metrics.getMetricRegistry().getGauges().get("record-lag").getValue());
    Assertions.assertEquals(
        -1L,
        metrics
            .getMetricRegistry()
            .getGauges()
            .get("seconds-since-last-successful-tail-sample")
            .getValue());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("records-fetched-total").getCount());
    Assertions.assertEquals(
        0, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("tail-sample-failures-total").getCount());
  }

  @Test
  void testTailSampleFailureRetainsPreviousTail() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS))
        .thenReturn(List.of(change(1L, "CATALOG", "ml1.cat1")));
    when(mapper.selectMaxChangeId()).thenThrow(new RuntimeException("tail query failed"));
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    metrics.setDbTailId(9L);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      new EntityChangeLogPoller(1, metrics).pollChanges();
    }

    Assertions.assertEquals(
        9L, metrics.getMetricRegistry().getGauges().get("db-tail-id").getValue());
    Assertions.assertEquals(
        1L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    Assertions.assertEquals(
        8L, metrics.getMetricRegistry().getGauges().get("record-lag").getValue());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("tail-sample-failures-total").getCount());
  }

  @Test
  void testInterruptedPollIsNotCountedAsFailure() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS))
        .thenThrow(new RuntimeException(new InterruptedException("shutdown")));
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      new EntityChangeLogPoller(1, metrics).pollChanges();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertEquals(
          0, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
      Assertions.assertEquals(
          0, metrics.getMetricRegistry().counter("tail-sample-failures-total").getCount());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testInterruptedTailSampleStopsDeliveryWithoutCountingFailure() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS))
        .thenReturn(List.of(change(1L, "CATALOG", "ml1.cat1")));
    when(mapper.selectMaxChangeId())
        .thenThrow(new RuntimeException(new InterruptedException("shutdown")));
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, metrics);
      poller.registerListener(received::addAll);
      poller.pollChanges();
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Assertions.assertTrue(received.isEmpty());
      Assertions.assertEquals(
          0L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
      Assertions.assertEquals(
          0, metrics.getMetricRegistry().counter("poll-failures-total").getCount());
      Assertions.assertEquals(
          0, metrics.getMetricRegistry().counter("tail-sample-failures-total").getCount());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testSuccessfulEmptyPollSamplesTailAndUpdatesMetrics() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS)).thenReturn(List.of());
    when(mapper.selectMaxChangeId()).thenReturn(4L);
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      new EntityChangeLogPoller(1, metrics).pollChanges();
    }

    Assertions.assertEquals(
        4L, metrics.getMetricRegistry().getGauges().get("db-tail-id").getValue());
    Assertions.assertEquals(
        4L, metrics.getMetricRegistry().getGauges().get("record-lag").getValue());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().histogram("batch-size-records").getCount());
    Assertions.assertEquals(
        0, metrics.getMetricRegistry().counter("records-fetched-total").getCount());
    Assertions.assertTrue(
        ((Number)
                    metrics
                        .getMetricRegistry()
                        .getGauges()
                        .get("seconds-since-last-successful-poll")
                        .getValue())
                .longValue()
            >= 0);
  }

  @Test
  void testListenerFailureIsAttributedAndCursorStillAdvances() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    when(mapper.selectEntityChanges(0L, MAX_ROWS))
        .thenReturn(List.of(change(1L, "TABLE", "ml1.cat1.schema1.table1")));
    when(mapper.selectMaxChangeId()).thenReturn(1L);
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogPoller poller = new EntityChangeLogPoller(1, metrics);
      poller.registerListener(
          changes -> {
            throw new IllegalStateException("failure");
          });
      poller.pollChanges();
    }

    Assertions.assertEquals(
        1L, metrics.getMetricRegistry().getGauges().get("cursor-id").getValue());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("listener-failures-total").getCount());
    Assertions.assertEquals(
        1, metrics.getMetricRegistry().counter("listener-failures.anonymous-total").getCount());
    Assertions.assertEquals(
        0, metrics.getMetricRegistry().counter("records-applied-total").getCount());
  }

  @Test
  void testCloseDropsThePendingPollInsteadOfWaitingForIt() {
    // Uses the real scheduler, with the pending poll two seconds away. If that delayed task
    // survived
    // shutdown, close() would wait for it. No poll may run after close() either. A poll would run
    // on the scheduler thread, where this thread's static SessionUtils mock is not active, so it is
    // observed through the poll timer, which counts every attempt, failed or not.
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    long closeMillis;

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);
      EntityChangeLogPoller poller = new EntityChangeLogPoller(2, metrics);
      poller.start();
      long startNanos = System.nanoTime();
      poller.close();
      closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      Assertions.assertThrows(IllegalStateException.class, poller::start);
    }

    Assertions.assertTrue(closeMillis < 1500, "close() took " + closeMillis + " ms");
    Assertions.assertEquals(0, metrics.getMetricRegistry().timer("poll-duration").getCount());
  }

  @Test
  void testAlreadyDueDrainHopDoesNotPollAfterClose() throws Exception {
    EntityChangeLogMetricsSource metrics = new EntityChangeLogMetricsSource();
    EntityChangeLogPoller poller = spy(new EntityChangeLogPoller(1, metrics));
    ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) poller.createScheduler();
    ScheduledExecutorService scheduler =
        mock(ScheduledExecutorService.class, delegatesTo(executor));
    doReturn(scheduler).when(poller).createScheduler();
    CountDownLatch workerBusy = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    CountDownLatch shutdownStarted = new CountDownLatch(1);
    AtomicReference<ScheduledFuture<?>> queuedHop = new AtomicReference<>();
    doAnswer(
            invocation -> {
              // Queue the real poll-cycle runnable as an already-due drain hop behind a busy
              // worker.
              ScheduledFuture<?> hop =
                  executor.schedule(invocation.<Runnable>getArgument(0), 0, TimeUnit.MILLISECONDS);
              queuedHop.set(hop);
              return hop;
            })
        .when(scheduler)
        .schedule(any(Runnable.class), anyLong(), any());
    doAnswer(
            invocation -> {
              executor.shutdown();
              shutdownStarted.countDown();
              return null;
            })
        .when(scheduler)
        .shutdown();
    ExecutorService closer = Executors.newSingleThreadExecutor();
    Future<?> blockingTask =
        executor.submit(
            () -> {
              workerBusy.countDown();
              try {
                Assertions.assertTrue(releaseWorker.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            });
    try {
      Assertions.assertTrue(workerBusy.await(5, TimeUnit.SECONDS));
      EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
      try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
        mockSessionUtils(sessionUtils, mapper);
        poller.start();
      }
      Future<?> closed = closer.submit(poller::close);
      Assertions.assertTrue(shutdownStarted.await(5, TimeUnit.SECONDS));
      releaseWorker.countDown();
      blockingTask.get(5, TimeUnit.SECONDS);
      // The shutdown policy retains already-due tasks: get() proves this hop ran, not cancelled.
      queuedHop.get().get(5, TimeUnit.SECONDS);
      closed.get(5, TimeUnit.SECONDS);
      Assertions.assertEquals(0, metrics.getMetricRegistry().timer("poll-duration").getCount());
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
      closer.shutdownNow();
      poller.close();
    }
  }

  @Test
  void testDispatchesImmutableBatchToListeners() {
    EntityChangeLogMapper mapper = mock(EntityChangeLogMapper.class);
    EntityChangeRecord first = change(1L, "CATALOG", "ml1.cat1");
    EntityChangeRecord second = change(2L, "SCHEMA", "ml1.cat1.sch1");
    when(mapper.selectEntityChanges(0L, MAX_ROWS))
        .thenReturn(new ArrayList<>(List.of(first, second)));

    List<EntityChangeRecord> received = new ArrayList<>();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      mockSessionUtils(sessionUtils, mapper);

      EntityChangeLogPoller poller = new EntityChangeLogPoller(1);
      poller.registerListener(
          changes -> Assertions.assertThrows(UnsupportedOperationException.class, changes::clear));
      poller.registerListener(received::addAll);

      poller.pollChanges();
    }

    Assertions.assertEquals(List.of(first, second), received);
  }

  private static EntityChangeRecord change(long id, String type, String fullName) {
    return new EntityChangeRecord(id, "ml1", type, fullName, OperateType.ALTER, 0L);
  }

  private static List<EntityChangeRecord> changes(long firstId, long lastId) {
    List<EntityChangeRecord> changes = new ArrayList<>();
    for (long id = firstId; id <= lastId; id++) {
      changes.add(change(id, "TABLE", "ml1.cat1.schema1.table" + id));
    }
    return changes;
  }

  private static List<Long> ids(List<EntityChangeRecord> changes) {
    return changes.stream().map(EntityChangeRecord::getId).collect(Collectors.toList());
  }

  private static void mockSessionUtils(
      MockedStatic<SessionUtils> sessionUtils, EntityChangeLogMapper mapper) {
    sessionUtils
        .when(() -> SessionUtils.getWithoutCommit(any(), any()))
        .thenAnswer(
            invocation -> {
              Function<Object, Object> func = invocation.getArgument(1);
              return func.apply(mapper);
            });
  }
}
