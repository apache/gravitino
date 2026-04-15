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
 * A {@link GravitinoCache} implementation backed by a Caffeine cache.
 *
 * <p>{@link #invalidateByPrefix(String)} performs an O(n) scan over the cache key set. This is
 * bounded and acceptable because DDL operations (which trigger invalidation) are rare.
 */
public class CaffeineGravitinoCache<K, V> implements GravitinoCache<K, V> {

  private final Cache<K, V> cache;

  public CaffeineGravitinoCache(long maxSize, long ttlSeconds) {
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
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
  @SuppressWarnings("unchecked")
  public void invalidateByPrefix(String prefix) {
    cache.asMap().keySet().stream()
        .filter(k -> ((String) k).startsWith(prefix))
        .forEach(cache::invalidate);
  }

  @Override
  public long size() {
    return cache.estimatedSize();
  }

  @Override
  public void close() {
    cache.invalidateAll();
    cache.cleanUp();
  }
}
