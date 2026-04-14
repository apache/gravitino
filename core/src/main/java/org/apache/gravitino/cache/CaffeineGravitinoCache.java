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
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A {@link GravitinoCache} implementation backed by a Caffeine cache with configurable TTL and
 * maximum size.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CaffeineGravitinoCache<K, V> implements GravitinoCache<K, V> {

  private final Cache<K, V> cache;

  /**
   * Creates a cache with the given maximum size and expiration, with no removal listener.
   *
   * @param maxSize maximum number of entries
   * @param expirationSecs expire-after-access duration in seconds
   */
  public CaffeineGravitinoCache(long maxSize, long expirationSecs) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(expirationSecs, TimeUnit.SECONDS)
            .maximumSize(maxSize)
            .executor(Runnable::run)
            .build();
  }

  /**
   * Creates a cache with the given maximum size, expiration, and a removal listener.
   *
   * @param maxSize maximum number of entries
   * @param expirationSecs expire-after-access duration in seconds
   * @param removalListener called on every eviction or explicit invalidation
   */
  public CaffeineGravitinoCache(
      long maxSize, long expirationSecs, RemovalListener<K, V> removalListener) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(expirationSecs, TimeUnit.SECONDS)
            .maximumSize(maxSize)
            .executor(Runnable::run)
            .removalListener(removalListener)
            .build();
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    return Optional.ofNullable(cache.getIfPresent(key));
  }

  @Override
  public void put(K key, V value) {
    cache.put(key, value);
  }

  @Override
  public void invalidate(K key) {
    cache.invalidate(key);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public long size() {
    return cache.estimatedSize();
  }

  /** Forces pending maintenance tasks (e.g. removal listener calls) to run synchronously. */
  public void cleanUp() {
    cache.cleanUp();
  }

  @Override
  public void close() {
    cache.cleanUp();
  }
}
