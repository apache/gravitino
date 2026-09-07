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
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for {@code MetricsSource} recording a nonzero count but a zero duration once an
 * endpoint or method hasn't been invoked within the reservoir's window: {@link
 * SlidingTimeWindowArrayReservoir} discards every sample once it falls outside its fixed window, so
 * {@link com.codahale.metrics.Timer} and {@link Histogram} then report count-only. {@link
 * MetricsSource#getTimer(String)} and {@link MetricsSource#getHistogram(String)} now use {@link
 * ExponentiallyDecayingReservoir} instead, which decays sample weight over time rather than
 * discarding samples outright, so it keeps reporting the last known duration distribution.
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
  void exponentiallyDecayingReservoirSurvivesIdlePeriod() {
    ManualClock clock = new ManualClock();
    Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir(1028, 0.015, clock));

    for (int i = 1; i <= 10; i++) {
      histogram.update(i * 100L);
    }
    Assertions.assertTrue(histogram.getSnapshot().getMax() > 0);

    // Same idle period as above, but the reservoir MetricsSource now uses must not go to zero.
    clock.advance(61, TimeUnit.SECONDS);

    Assertions.assertEquals(10, histogram.getCount());
    Assertions.assertTrue(
        histogram.getSnapshot().getMax() > 0,
        "ExponentiallyDecayingReservoir must keep reporting real duration data after idle "
            + "periods instead of collapsing to zero");
  }
}
