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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import java.util.concurrent.atomic.AtomicLong;

/** Process-local metrics for the entity change log poller and its entity-cache listener. */
public class EntityChangeLogMetricsSource extends MetricsSource {
  private final AtomicLong dbTailId = new AtomicLong();
  private final AtomicLong cursorId = new AtomicLong();
  private final AtomicLong lastSuccessfulPollMs = new AtomicLong();
  private final Counter pollFailures = getCounter("poll-failures-total");
  private final Counter listenerFailures = getCounter("listener-failures-total");
  private final Counter recordsFetched = getCounter("records-fetched-total");
  private final Counter recordsDelivered = getCounter("records-delivered-total");
  private final Counter recordsApplied = getCounter("records-applied-total");
  private final Counter invalidationFailures = getCounter("invalidation-failures-total");
  private final Counter fallbackClears = getCounter("fallback-clears-total");
  private final Histogram batchSize = getHistogram("batch-size-records");
  private final Timer pollDuration = getTimer("poll-duration");

  /** Creates and registers the nonblocking gauges for one server's change log. */
  public EntityChangeLogMetricsSource() {
    super("entity-change-log");
    registerGauge("db-tail-id", (Gauge<Long>) dbTailId::get);
    registerGauge("cursor-id", (Gauge<Long>) cursorId::get);
    registerGauge("record-lag", (Gauge<Long>) () -> Math.max(0, dbTailId.get() - cursorId.get()));
    registerGauge(
        "seconds-since-last-successful-poll",
        (Gauge<Long>)
            () -> {
              long last = lastSuccessfulPollMs.get();
              return last == 0 ? -1 : Math.max(0, (System.currentTimeMillis() - last) / 1000);
            });
  }

  /** Records the database tail sampled by a poll, without querying from the gauge. */
  public void setDbTailId(long id) {
    dbTailId.set(id);
  }

  /** Records the cursor after a successful delivery. */
  public void setCursorId(long id) {
    cursorId.set(id);
  }

  /** Records a successful database poll, including an empty result. */
  public void pollSucceeded(int count) {
    lastSuccessfulPollMs.set(System.currentTimeMillis());
    recordsFetched.inc(count);
    batchSize.update(count);
  }

  /** Records a failed poll query or cycle. */
  public void pollFailed() {
    pollFailures.inc();
  }

  /** Records a listener delivery that failed, attributed by its stable class name. */
  public void listenerFailed(String listenerName) {
    listenerFailures.inc();
    getCounter("listener-failures." + listenerName.replace('.', '_') + "-total").inc();
  }

  /** Records the rows delivered to one listener without an exception. */
  public void recordsDelivered(String listenerName, int count) {
    recordsDelivered.inc(count);
    getCounter("records-delivered." + listenerName.replace('.', '_') + "-total").inc(count);
  }

  /** Records targeted entity-cache invalidations that completed successfully. */
  public void recordsApplied(int count) {
    recordsApplied.inc(count);
  }

  /** Records a targeted entity-cache invalidation failure. */
  public void invalidationFailed() {
    invalidationFailures.inc();
  }

  /** Records a successful full-cache clear used as a recovery fallback. */
  public void fallbackCleared() {
    fallbackClears.inc();
  }

  /** Starts a poll duration measurement. */
  public Timer.Context timePoll() {
    return pollDuration.time();
  }
}
