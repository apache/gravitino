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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;

/** A cache implementation that does not cache anything. */
public class NoOpsCache extends BaseEntityCache {
  private final ReentrantLock opLock = new ReentrantLock();
  /**
   * Constructs a new {@link BaseEntityCache} instance.
   *
   * @param config The cache configuration
   */
  public NoOpsCache(Config config) {
    super(config);
  }

  /** {@inheritDoc} */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    // do nothing
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    // do nothing
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> void withCacheLock(ThrowingRunnable<E> action) throws E {
    try {
      opLock.lockInterruptibly();
      try {
        action.run();
      } finally {
        opLock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while waiting for lock", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T, E extends Exception> T withCacheLock(ThrowingSupplier<T, E> action) throws E {
    try {
      opLock.lockInterruptibly();
      try {
        return action.get();
      } finally {
        opLock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Thread was interrupted while waiting for lock", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    // do nothing
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
    // do nothing
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<List<E>> getIfPresent(
      SupportsRelationOperations.Type relType,
      NameIdentifier nameIdentifier,
      Entity.EntityType identType) {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(
      NameIdentifier ident,
      Entity.EntityType type,
      SupportsRelationOperations.Type relType,
      List<E> entities) {
    // do nothing
  }
}
