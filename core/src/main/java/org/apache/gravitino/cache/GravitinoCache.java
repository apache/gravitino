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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
   * Returns the value associated with the key, loading it if necessary. Implementations with real
   * storage should make the load atomic for the same key, and should serialise concurrent reads
   * against any {@link #invalidate(Object) invalidate} call so a reader cannot observe a partially
   * invalidated cache.
   *
   * @param key the cache key
   * @param loader the loader invoked when the key is absent
   * @return the cached or loaded value
   */
  default V get(K key, Function<K, V> loader) {
    Optional<V> value = getIfPresent(key);
    if (value.isPresent()) {
      return value.get();
    }
    V loaded = Objects.requireNonNull(loader.apply(key), "Cache loader must not return null");
    put(key, loaded);
    return loaded;
  }

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
   * Evicts all entries whose key is a String and starts with the given prefix. Only meaningful when
   * K = String. Used by metadataIdCache for path-based invalidation: dropping a catalog evicts the
   * catalog entry plus all schema/table/fileset/... entries under that catalog name path.
   *
   * @param prefix the prefix to match against key strings
   */
  void invalidateByPrefix(String prefix);

  /**
   * Runs {@code batch} so that all {@link #invalidate}, {@link #invalidateAll}, and {@link
   * #invalidateByPrefix} calls it makes appear atomic to concurrent readers — i.e. readers see
   * either the state from before the batch or the state after, never an intermediate half-applied
   * state.
   *
   * <p>Default implementation: just runs the runnable, suitable for caches without real storage
   * (e.g. {@link NoOpsGravitinoCache}). Storage-backed implementations should hold an exclusive
   * lock for the duration of the batch.
   *
   * @param batch the invalidation sequence to run atomically
   */
  default void runInvalidationBatch(Runnable batch) {
    batch.run();
  }

  /**
   * Returns the approximate number of entries in the cache.
   *
   * @return the cache size
   */
  long size();
}
