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

import org.apache.gravitino.Entity;

/**
 * {@code EntityCache} is a cache interface in Gravitino designed to accelerate metadata access for
 * entities. It maintains a bidirectional mapping between an {@link Entity} and its {@code
 * NameIdentifier}. The cache also supports cascading removal of entries, ensuring related
 * sub-entities are cleared together.
 */
public interface EntityCache extends SupportsEntityStoreCache, SupportsRelationEntityCache {
  /**
   * Clears all entries from the cache, including data and index, resetting it to an empty state.
   */
  void clear();

  /**
   * Returns the number of entries in the cache.
   *
   * @return The number of entries in the cache
   */
  long size();

  /**
   * Executes the given action within a cache context.
   *
   * @param action The action to cache
   * @param <E> The type of exception that may be thrown
   * @throws E if the action throws an exception of type E
   */
  <E extends Exception> void withCacheLock(EntityCacheKey key, ThrowingRunnable<E> action) throws E;

  /**
   * Executes the given action within a cache context and returns the result.
   *
   * @param action The action to cache
   * @return The result of the action
   * @param <E> The type of exception that may be thrown
   * @param <T> The type of the result
   * @throws E if the action throws an exception of type E
   */
  <T, E extends Exception> T withCacheLock(EntityCacheKey key, ThrowingSupplier<T, E> action)
      throws E;

  /**
   * A functional interface that represents a supplier that may throw an exception.
   *
   * @param <T> The type of result supplied by this supplier
   * @param <E> The type of exception that may be thrown
   * @see java.util.function.Supplier
   */
  @FunctionalInterface
  interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
  }

  /**
   * A functional interface that represents a runnable that may throw an exception.
   *
   * @param <E> The type of exception that may be thrown
   * @see java.lang.Runnable
   */
  @FunctionalInterface
  interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
  }
}
