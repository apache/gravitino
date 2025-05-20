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
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a meta cache using Caffeine cache. */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());

  /** Singleton instance */
  private static volatile CaffeineEntityCache INSTANCE = null;

  /** Cache part */
  private final Cache<EntityCacheKey, Entity> cacheData;
  /** Index part */
  private RadixTree<EntityCacheKey> cacheIndex;

  private final ReentrantLock opLock = new ReentrantLock();

  @VisibleForTesting
  public static void resetForTest() {
    INSTANCE = null;
  }

  /**
   * Returns the instance of MetaCacheCaffeine based on the cache configuration and entity store.
   *
   * @param cacheConfig The cache configuration
   * @param entityStore The entity store to load entities from the database
   * @return The instance of {@link CaffeineEntityCache}
   */
  public static CaffeineEntityCache getInstance(CacheConfig cacheConfig, EntityStore entityStore) {
    if (INSTANCE == null) {
      synchronized (CaffeineEntityCache.class) {
        if (INSTANCE == null) {
          INSTANCE = new CaffeineEntityCache(cacheConfig, entityStore);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Returns the instance of MetaCacheCaffeine, if it is initialized, otherwise throws an exception.
   *
   * @return If INSTANCE initialized, returns the instance, otherwise throws an exception.
   */
  public static CaffeineEntityCache getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Illegal state: instance not initialized");
    }

    return INSTANCE;
  }

  /**
   * Constructs a new MetaCacheCaffeine.
   *
   * @param cacheConfig the cache configuration
   * @param entityStore The entity store to load entities from the database
   */
  private CaffeineEntityCache(CacheConfig cacheConfig, EntityStore entityStore) {
    super(cacheConfig, entityStore);
    cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    /**
     * Executor for async cache cleanup, when a cache expires then use this executor to sync other
     * cache and index trees
     */
    ThreadPoolExecutor cleanupExec =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(100),
            r -> {
              Thread t = new Thread(r, "CaffeineMetaCache-Cleanup");
              t.setDaemon(true);
              return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

    Caffeine<EntityCacheKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                invalidateExpiredItemByMetadata(value);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    this.cacheData = cacheDataBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoad(NameIdentifier ident, Entity.EntityType type) throws IOException {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    Entity cachedEntity = cacheData.getIfPresent(EntityCacheKey.of(ident, type));

    if (cachedEntity != null) {
      return cachedEntity;
    }

    return withLockAndThrow(
        () -> {
          Entity entity = entityStore.get(ident, type, getEntityClass(type));
          syncMetadataToCache(entity);

          return entity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          Entity cachedEntity = cacheData.getIfPresent(EntityCacheKey.of(ident, type));
          return Optional.ofNullable((E) cachedEntity);
        });
  }

  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type);

          boolean existed = contains(ident, type);
          if (existed) {
            invalidateEntities(entityCacheKey);
          }

          return existed;
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    return withLock(() -> cacheData.getIfPresent(EntityCacheKey.of(ident, type)) != null);
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return cacheData.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    withLock(
        () -> {
          cacheData.invalidateAll();
          cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    withLock(
        () -> {
          syncMetadataToCache(entity);
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> void withCacheLock(ThrowingRunnable<E> action) throws E {
    withLockAndThrow(action);
  }

  /** {@inheritDoc} */
  @Override
  public <E, T extends Exception> E withCacheLock(ThrowingSupplier<E, T> action) throws T {
    return withLockAndThrow(action);
  }

  /**
   * Synchronizes the metadata to the cache.
   *
   * @param metadata The metadata to synchronize
   */
  private void syncMetadataToCache(Entity metadata) {
    NameIdentifier nameIdent = getIdentFromMetadata(metadata);
    Entity.EntityType type = metadata.type();
    EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdent, type);

    cacheData.put(entityCacheKey, metadata);
    cacheIndex.put(entityCacheKey.toString(), entityCacheKey);
  }

  /**
   * Removes the expired metadata from the cache. This method is a hook method for the Cache, when
   * an entry expires, it will call this method.
   *
   * @param metadata The metadata to remove,
   */
  @Override
  protected void invalidateExpiredItemByMetadata(Entity metadata) {
    NameIdentifier identifier = getIdentFromMetadata(metadata);
    Entity.EntityType type = metadata.type();
    EntityCacheKey cacheKey = EntityCacheKey.of(identifier, type);

    withLock(
        () -> {
          cacheIndex.remove(cacheKey.toString());
        });
  }

  /**
   * Returns a new instance of Caffeine cache builder.
   *
   * @param cacheConfig The cache configuration
   * @param <KEY> The key type
   * @param <VALUE> The value type
   * @return The new instance of Caffeine cache builder
   */
  private <KEY, VALUE> Caffeine<KEY, VALUE> newBaseBuilder(CacheConfig cacheConfig) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();

    if (cacheConfig.isWeigherEnabled()) {
      builder.maximumWeight(cacheConfig.getMaxWeight());
      builder.weigher(EntityCacheWeigher.getInstance());
    } else {
      builder.maximumSize(cacheConfig.getMaxSize());
    }

    if (cacheConfig.isExpirationEnabled()) {
      builder.expireAfterWrite(
          cacheConfig.getExpirationTime(), cacheConfig.getExpirationTimeUnit());
    }

    if (cacheConfig.isCacheStatusEnabled()) {
      builder.recordStats();
    }

    return (Caffeine<KEY, VALUE>) builder;
  }

  /**
   * Invalidates the entities by the given metadata key.
   *
   * @param entityCacheKey The metadata key to invalidate the entities
   */
  private void invalidateEntities(EntityCacheKey entityCacheKey) {
    Iterable<EntityCacheKey> keyToRemove =
        cacheIndex.getValuesForKeysStartingWith(entityCacheKey.identifier().toString());

    cacheData.invalidateAll(keyToRemove);
    keyToRemove.forEach(key -> cacheIndex.remove(key.toString()));
  }

  /**
   * Runs the given action with the lock.
   *
   * @param action The action to run with the lock
   */
  private void withLock(Runnable action) {
    opLock.lock();
    try {
      action.run();
    } finally {
      opLock.unlock();
    }
  }

  /**
   * Runs the given action with the lock and returns the result.
   *
   * @param action The action to run with the lock
   * @return The result of the action
   * @param <T> The type of the result
   */
  private <T> T withLock(Supplier<T> action) {
    opLock.lock();
    try {
      return action.get();
    } finally {
      opLock.unlock();
    }
  }

  /**
   * Runs the given action with the lock and throws the exception if it occurs.
   *
   * @param action The action to run with the lock
   * @param <T> The type of the result
   * @return The result of the action
   * @throws IOException If an exception occurs during the action
   */
  private <T, E extends Exception> T withLockAndThrow(ThrowingSupplier<T, E> action) throws E {
    opLock.lock();
    try {
      return action.get();
    } finally {
      opLock.unlock();
    }
  }

  /**
   * Runs the given action with the lock and throws the exception if it occurs.
   *
   * @param action The action to run with the lock
   * @param <E> The type of the exception
   * @throws E If an exception occurs during the action
   */
  private <E extends Exception> void withLockAndThrow(ThrowingRunnable<E> action) throws E {
    opLock.lock();
    try {
      action.run();
    } finally {
      opLock.unlock();
    }
  }
}
