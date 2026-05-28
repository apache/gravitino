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
package org.apache.gravitino.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A Caffeine-backed implementation of {@link GravitinoCache}. Supports configurable TTL and maximum
 * size.
 *
 * <p>A fair {@link ReentrantReadWriteLock} serialises invalidations against reads/puts. Reads,
 * loader-backed gets, and puts share the read lock; {@code invalidate*} operations take the write
 * lock. See the field-level Javadoc on {@link #lock} for the races this guards against and the
 * cost/benefit reasoning. Callers that need a multi-step invalidation to appear atomic wrap their
 * sequence in {@link #runInvalidationBatch(Runnable)}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CaffeineGravitinoCache<K, V> implements GravitinoCache<K, V> {

  private final Cache<K, V> cache;

  /**
   * Serialises invalidations against reads/puts so a batched invalidation never appears
   * half-applied to concurrent readers. Two related races motivate this:
   *
   * <ol>
   *   <li><b>Single-node read/invalidate race (addressed here).</b> When the change poller drains a
   *       batch of stale keys and calls {@link #invalidate} per key, a reader can interleave
   *       between two invalidations and observe a half-applied batch — some entries already
   *       evicted, others still hot. The exclusive write lock makes each batch atomic from a
   *       reader's point of view: readers see either the pre-batch state or the post-batch state,
   *       never a mix. Callers that need multi-step atomicity wrap their sequence in {@link
   *       #runInvalidationBatch(Runnable)}.
   *   <li><b>Multi-node staleness window (NOT addressed here, and no lock can).</b> After a commit
   *       on node A, node B keeps returning stale data until B's poller runs (~poll interval, on
   *       the order of seconds). The race in (1) is a ~ms slice of that larger window and
   *       self-heals once the batch completes — the affected keys are gone from the cache, so the
   *       next read pulls fresh data. Callers that need read-after-write consistency must bypass
   *       the cache; nothing on node B can make node A's commit visible sooner.
   * </ol>
   *
   * <p>Cost: the poller runs every few seconds with small batches, so contention is rare; a fair RW
   * lock under no contention is on the order of nanoseconds. Dropping the lock would be defensible
   * — race (2) dominates the staleness budget — but keeping it makes the cache's invalidation
   * semantics easy to reason about and test, at a cost we won't measure on the hot path.
   *
   * <p>Fair mode prevents writer starvation: a parked {@code invalidate*} call queues new readers
   * behind it instead of letting them slip in ahead. Reads sit on the authorization hot path and
   * would otherwise starve the rarer invalidations.
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  /**
   * Creates a new CaffeineGravitinoCache with the given TTL and maximum size.
   *
   * @param ttlMs the time-to-live in milliseconds for cache entries (safety-net TTL)
   * @param maxSize the maximum number of entries in the cache
   */
  public CaffeineGravitinoCache(long ttlMs, long maxSize) {
    this(ttlMs, maxSize, Ticker.systemTicker());
  }

  CaffeineGravitinoCache(long ttlMs, long maxSize, Ticker ticker) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
            .maximumSize(maxSize)
            .ticker(ticker)
            .build();
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    return withReadLock(() -> Optional.ofNullable(cache.getIfPresent(key)));
  }

  @Override
  public V get(K key, Function<K, V> loader) {
    return withReadLock(
        () ->
            cache.get(
                key,
                k -> Objects.requireNonNull(loader.apply(k), "Cache loader must not return null")));
  }

  @Override
  public void put(K key, V value) {
    // Puts run under the shared read lock so that they may proceed concurrently with reads and
    // with other puts; only invalidations need exclusive access.
    withReadLock(() -> cache.put(key, value));
  }

  @Override
  public void invalidate(K key) {
    withWriteLock(() -> cache.invalidate(key));
  }

  @Override
  public void invalidateAll() {
    withWriteLock(cache::invalidateAll);
  }

  @Override
  public void invalidateByPrefix(String prefix) {
    // Prefix invalidation scans all keys. It is intended for infrequent structural invalidations
    // such as dropping or renaming an entity hierarchy, not for per-request hot paths.
    withWriteLock(
        () ->
            cache
                .asMap()
                .keySet()
                .removeIf(k -> k instanceof String && ((String) k).startsWith(prefix)));
  }

  @Override
  public void runInvalidationBatch(Runnable batch) {
    // The write lock is reentrant, so individual invalidate* calls inside the batch re-acquire it
    // without deadlocking.
    withWriteLock(batch);
  }

  @Override
  public long size() {
    return withReadLock(cache::estimatedSize);
  }

  @VisibleForTesting
  void cleanUp() {
    cache.cleanUp();
  }

  @Override
  public void close() {
    cache.invalidateAll();
    cache.cleanUp();
  }

  private <T> T withReadLock(Supplier<T> action) {
    lock.readLock().lock();
    try {
      return action.get();
    } finally {
      lock.readLock().unlock();
    }
  }

  private void withReadLock(Runnable action) {
    lock.readLock().lock();
    try {
      action.run();
    } finally {
      lock.readLock().unlock();
    }
  }

  private void withWriteLock(Runnable action) {
    lock.writeLock().lock();
    try {
      action.run();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
