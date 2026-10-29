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

/**
 * Counters of one node's Redis entity cache client. They make a cache that stopped working visible:
 * every fill rejected, every read failing, or invalidations not reaching Redis.
 */
public class RedisEntityCacheMetricsSource extends MetricsSource {
  private final Counter hits = getCounter("hits-total");
  private final Counter misses = getCounter("misses-total");
  private final Counter staleReads = getCounter("stale-reads-total");
  private final Counter fillsWritten = getCounter("fills-written-total");
  private final Counter fillsRejected = getCounter("fills-rejected-total");
  private final Counter fillsSkipped = getCounter("fills-skipped-total");
  private final Counter failures = getCounter("redis-failures-total");
  private final Counter invalidationFailures = getCounter("invalidation-failures-total");

  /** Creates the counters of one node's Redis entity cache. */
  public RedisEntityCacheMetricsSource() {
    super("redis-entity-cache");
  }

  /** Records a read served from Redis. */
  public void hit() {
    hits.inc();
  }

  /** Records a read that found no servable value. */
  public void miss() {
    misses.inc();
  }

  /** Records a value found under an outdated stamp, which was deleted and treated as a miss. */
  public void staleRead() {
    staleReads.inc();
  }

  /** Records a fill that was written. */
  public void fillWritten() {
    fillsWritten.inc();
  }

  /** Records a fill refused because an invalidation committed while the entity was loading. */
  public void fillRejected() {
    fillsRejected.inc();
  }

  /** Records a fill not attempted because no miss on the same thread recorded its stamp. */
  public void fillSkipped() {
    fillsSkipped.inc();
  }

  /** Records a Redis command that failed or timed out. */
  public void failure() {
    failures.inc();
  }

  /** Records an invalidation that could not reach Redis after its retry. */
  public void invalidationFailed() {
    invalidationFailures.inc();
  }
}
