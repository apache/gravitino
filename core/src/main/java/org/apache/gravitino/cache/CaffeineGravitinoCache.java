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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Caffeine-backed implementation of {@link GravitinoCache}. Supports configurable TTL and maximum
 * size.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CaffeineGravitinoCache<K, V> implements GravitinoCache<K, V> {

  private final Cache<K, V> cache;

  /**
   * Creates a new CaffeineGravitinoCache with the given TTL and maximum size.
   *
   * @param ttlMs the time-to-live in milliseconds for cache entries (safety-net TTL)
   * @param maxSize the maximum number of entries in the cache
   */
  public CaffeineGravitinoCache(long ttlMs, long maxSize) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
            .maximumSize(maxSize)
            .build();
  }

  @Override
  public Optional<V> getIfPresent(K key) {
    V value = cache.getIfPresent(key);
    return Optional.ofNullable(value);
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
  public void invalidateByPrefix(String prefix) {
    cache.asMap().keySet().removeIf(k -> k.toString().startsWith(prefix));
  }

  @Override
  public long size() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  @Override
  public void close() {
    cache.invalidateAll();
    cache.cleanUp();
  }
}
