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
 * A general-purpose cache interface used by the authorization subsystem. Implementations include a
 * Caffeine-backed cache and a no-op cache for testing.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface GravitinoCache<K, V> extends Closeable {

  /**
   * Returns the value associated with the key, or empty if not present.
   *
   * @param key the cache key
   * @return an Optional containing the cached value, or empty if absent
   */
  Optional<V> getIfPresent(K key);

  /**
   * Associates the value with the key in the cache.
   *
   * @param key the cache key
   * @param value the value to cache
   */
  void put(K key, V value);

  /**
   * Removes the entry for the given key.
   *
   * @param key the cache key to invalidate
   */
  void invalidate(K key);

  /** Removes all entries from the cache. */
  void invalidateAll();

  /**
   * Evicts all entries whose key (as a String) starts with the given prefix. Only meaningful when K
   * = String. Used by metadataIdCache for hierarchical cascade invalidation: dropping a catalog
   * evicts the catalog entry plus all schema/table/fileset/... entries beneath it.
   *
   * @param prefix the prefix to match against key strings
   */
  void invalidateByPrefix(String prefix);

  /**
   * Returns the approximate number of entries in the cache.
   *
   * @return the cache size
   */
  long size();
}
