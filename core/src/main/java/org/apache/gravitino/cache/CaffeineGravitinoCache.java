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
 * <p>A {@link ReentrantReadWriteLock} (fair) serialises invalidations against reads / puts so that
 * a batched invalidation cannot be observed in a half-applied state by concurrent readers. Reads,
 * loader-backed gets, and puts all hold the shared read lock and can run concurrently with each
 * other; {@code invalidate*} operations hold the exclusive write lock and block any reader until
 * they complete. Callers that need a multi-step invalidation to appear atomic can run it inside
 * {@link #runInvalidationBatch(Runnable)}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CaffeineGravitinoCache<K, V> implements GravitinoCache<K, V> {

  private final Cache<K, V> cache;

  /**
   * Fair mode prevents writer starvation: when an {@code invalidate*} call is parked waiting for
   * the write lock, new readers queue behind it instead of slipping in ahead. This matters here
   * because reads sit on the authorization hot path and would otherwise starve the rarer
   * invalidations.
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
