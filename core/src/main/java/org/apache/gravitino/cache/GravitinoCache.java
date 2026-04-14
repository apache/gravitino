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

import java.io.Closeable;
import java.util.Optional;

/**
 * A lightweight general-purpose cache interface used by the authorization layer.
 *
 * <p>Implementations include {@link CaffeineGravitinoCache} (backed by Caffeine) and {@link
 * NoOpsGravitinoCache} (no-op, for tests or when caching is disabled).
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public interface GravitinoCache<K, V> extends Closeable {

  /**
   * Returns the cached value for the given key, or {@link Optional#empty()} if absent.
   *
   * @param key the cache key
   * @return the cached value wrapped in {@link Optional}, or empty if not present
   */
  Optional<V> getIfPresent(K key);

  /**
   * Inserts or overwrites the entry for the given key.
   *
   * @param key the cache key
   * @param value the value to cache
   */
  void put(K key, V value);

  /**
   * Removes the entry for the given key if it is present.
   *
   * @param key the cache key to evict
   */
  void invalidate(K key);

  /** Removes all entries from the cache. */
  void invalidateAll();

  /**
   * Returns an estimate of the number of entries currently in the cache.
   *
   * @return estimated size
   */
  long size();
}
