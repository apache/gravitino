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
 * A generic cache abstraction for Gravitino's auth path.
 *
 * <p>Implementations must be thread-safe. The {@code invalidateByPrefix} method is only meaningful
 * when {@code K = String}; callers are responsible for ensuring key type compatibility.
 */
public interface GravitinoCache<K, V> extends Closeable {

  /** Returns the value for {@code key} if present, otherwise {@link Optional#empty()}. */
  Optional<V> getIfPresent(K key);

  /** Associates {@code value} with {@code key} in the cache. */
  void put(K key, V value);

  /** Discards the entry for {@code key} if it exists. */
  void invalidate(K key);

  /** Discards all entries in the cache. */
  void invalidateAll();

  /**
   * Evicts all entries whose key starts with the given {@code prefix}.
   *
   * <p>Only applicable when {@code K = String}. Used by {@code metadataIdCache} for cascade
   * invalidation: dropping a catalog evicts the catalog entry plus all schema/table/fileset/...
   * entries beneath it in one call.
   */
  void invalidateByPrefix(String prefix);

  /** Returns the approximate number of entries in the cache. */
  long size();

  @Override
  void close();
}
