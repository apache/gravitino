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

package org.apache.gravitino.metrics.source;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for {@code MetricsSource} recording a nonzero count but a zero duration once an
 * endpoint or method hasn't been invoked within the reservoir's window: {@link
 * SlidingTimeWindowArrayReservoir} discards every sample once it falls outside its fixed window, so
 * {@link Timer} and {@link Histogram} then report count-only. {@link
 * MetricsSource#getTimer(String)} and {@link MetricsSource#getHistogram(String)} now use {@link
 * ExponentiallyDecayingReservoir} instead, which decays sample weight over time rather than
 * expiring it on a fixed window, so infrequently-invoked operations keep reporting a real duration
 * for far longer (on the order of half a day with the default decay rate) than the old 60-second
 * window.
 *
 * <p>This is a mitigation, not a complete fix: {@link ExponentiallyDecayingReservoir} rescales its
 * samples' decayed weights periodically, and once that scaling factor underflows to zero in
 * double-precision arithmetic (in practice, after roughly 13.8 hours of inactivity with the default
 * decay rate), it clears every sample outright, reproducing the same "count survives, duration
 * reads zero" symptom the old reservoir showed at 60 seconds. {@link
 * #metricsSourceTimerEventuallyZeroesOutAfterVeryLongIdlePeriod()} documents that known, unresolved
 * boundary through the actual {@link MetricsSource} timer-creation path.
 */
public class TestReservoirIdleBehavior {

  /** A {@link Clock} whose tick/time only move when {@link #advance} is called. */
  private static class ManualClock extends Clock {
    private final AtomicLong nanos = new AtomicLong(0);

    @Override
    public long getTick() {
      return nanos.get();
    }

    @Override
    public long getTime() {
      return TimeUnit.NANOSECONDS.toMillis(nanos.get());
    }

    void advance(long duration, TimeUnit unit) {
      nanos.addAndGet(unit.toNanos(duration));
    }
  }

  /**
   * A {@code MetricsSource} whose reservoirs are driven by a {@link ManualClock} instead of real
   * wall-clock time, so idle periods can be simulated deterministically while still going through
   * the production {@link MetricsSource#getTimer(String)}/{@link
   * MetricsSource#getHistogram(String)} methods rather than constructing reservoirs directly.
   */
  private static class ManualClockMetricsSource extends MetricsSource {
    private final ManualClock clock;

    ManualClockMetricsSource(ManualClock clock) {
      super("test-manual-clock");
      this.clock = clock;
    }

    @Override
    protected Reservoir newReservoir() {
      return new ExponentiallyDecayingReservoir(1028, 0.015, clock);
    }
  }

  @Test
  void slidingTimeWindowReservoirZeroesOutAfterIdlePeriod() {
    ManualClock clock = new ManualClock();
    Histogram histogram =
        new Histogram(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS, clock));

    for (int i = 1; i <= 10; i++) {
      histogram.update(i * 100L);
    }
    Assertions.assertTrue(histogram.getSnapshot().getMax() > 0);

    // Simulate the endpoint going quiet for longer than the 60-second window.
    clock.advance(61, TimeUnit.SECONDS);

    Assertions.assertEquals(10, histogram.getCount(), "count must survive the idle period");
    Assertions.assertEquals(
        0,
        histogram.getSnapshot().getMax(),
        "this is the bug: the old reservoir silently zeroes out duration stats once idle");
  }

  @Test
  void exponentiallyDecayingReservoirSurvivesShortIdlePeriod() {
    ManualClock clock = new ManualClock();
    Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir(1028, 0.015, clock));

    for (int i = 1; i <= 10; i++) {
      histogram.update(i * 100L);
    }
    Assertions.assertTrue(histogram.getSnapshot().getMax() > 0);

    // Same idle period the old reservoir already failed at; the new one must not go to zero here.
    clock.advance(61, TimeUnit.SECONDS);

    Assertions.assertEquals(10, histogram.getCount());
    Assertions.assertTrue(
        histogram.getSnapshot().getMax() > 0,
        "ExponentiallyDecayingReservoir must keep reporting real duration data after a short "
            + "idle period instead of collapsing to zero");
  }

  @Test
  void metricsSourceTimerUsesExponentiallyDecayingReservoirByDefault() {
    MetricsSource metricsSource = new ManualClockMetricsSource(new ManualClock());
    Timer timer = metricsSource.getTimer("op.total");
    timer.update(100, TimeUnit.MILLISECONDS);

    // Exercised through the real production method, not a directly-constructed reservoir, so this
    // would fail if MetricsSource ever reverted to SlidingTimeWindowArrayReservoir.
    Assertions.assertTrue(timer.getSnapshot().getMax() > 0);
  }

  @Test
  void metricsSourceTimerEventuallyZeroesOutAfterVeryLongIdlePeriod() {
    ManualClock clock = new ManualClock();
    MetricsSource metricsSource = new ManualClockMetricsSource(clock);
    Timer timer = metricsSource.getTimer("op.total");

    for (int i = 1; i <= 10; i++) {
      timer.update(i * 100L, TimeUnit.MILLISECONDS);
    }
    Assertions.assertTrue(timer.getSnapshot().getMax() > 0);

    // Advance in increments, calling getSnapshot() along the way, to mirror periodic Prometheus
    // scraping every 30s during the idle period rather than a single large jump.
    for (int i = 0; i < 1680; i++) { // 1680 * 30s = 14h
      clock.advance(30, TimeUnit.SECONDS);
      timer.getSnapshot();
    }

    Assertions.assertEquals(10, timer.getCount(), "count must still survive the idle period");
    Assertions.assertEquals(
        0,
        timer.getSnapshot().getMax(),
        "known limitation: ExponentiallyDecayingReservoir's rescale() clears every sample once "
            + "its decayed weight underflows to zero, so a sufficiently long idle period (roughly "
            + "half a day with the default decay rate) still reproduces the original bug");
  }
}
