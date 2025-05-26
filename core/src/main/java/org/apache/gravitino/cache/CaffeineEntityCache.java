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
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a meta cache using Caffeine cache. */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());

  /** Singleton instance */
  private static volatile CaffeineEntityCache INSTANCE = null;

  /** Cache part */
  private final Cache<EntityCacheKey, List<Entity>> cacheData;

  /** Index part */
  private RadixTree<EntityCacheKey> cacheIndex;

  private final ReentrantLock opLock = new ReentrantLock();
  ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

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
     * cache and index trees.
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

    Caffeine<EntityCacheKey, List<Entity>> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                invalidateExpiredItem(key);
              } catch (Throwable t) {
                LOG.error("Failed to remove entity value={} from cache asynchronously", value, t);
              }
            });

    this.cacheData = cacheDataBuilder.build();

    if (cacheConfig.isCacheStatusEnabled()) {
      startCacheStatsMonitor();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> List<E> getOrLoad(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType)
      throws IOException {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
    Preconditions.checkArgument(relType != null, "SupportsRelationOperations.Type cannot be null");

    return withLockAndThrow(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type, relType);
          List<Entity> entitiesFromCache = cacheData.getIfPresent(entityCacheKey);

          if (entitiesFromCache != null) {
            return convertEntity(entitiesFromCache);
          }

          List<E> entities = entityStore.listEntitiesByRelation(relType, ident, type);
          syncEntitiesToCache(
              entityCacheKey, entities.stream().map(e -> (Entity) e).collect(Collectors.toList()));

          return entities;
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> E getOrLoad(
      NameIdentifier ident, Entity.EntityType type) throws IOException {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    return withLockAndThrow(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type);
          List<Entity> entitiesFromCache = cacheData.getIfPresent(entityCacheKey);

          if (entitiesFromCache != null) {
            return convertEntity(entitiesFromCache.get(0));
          }

          E entityFromStore = entityStore.get(ident, type, getEntityClass(type));
          syncEntitiesToCache(entityCacheKey, entityFromStore);

          return entityFromStore;
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<List<E>> getIfPresent(
      SupportsRelationOperations.Type relType,
      NameIdentifier nameIdentifier,
      Entity.EntityType identType) {
    return withLock(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdentifier, identType, relType);

          List<Entity> entitiesFromCache = cacheData.getIfPresent(entityCacheKey);
          if (entitiesFromCache != null) {
            List<E> convertedEntities = convertEntity(entitiesFromCache);
            return Optional.of(convertedEntities);
          }

          return Optional.empty();
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type);
          List<Entity> entitiesFromCache = cacheData.getIfPresent(entityCacheKey);
          if (entitiesFromCache != null) {
            return Optional.of(convertEntity(entitiesFromCache.get(0)));
          }

          return Optional.empty();
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return withLock(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type);

          boolean existed = contains(ident, type);
          if (existed) {
            invalidateEntities(entityCacheKey.identifier());
          }

          return existed;
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          EntityCacheKey entityCacheKey = EntityCacheKey.of(ident, type);

          boolean existed = contains(ident, type);
          if (existed) {
            invalidateEntities(entityCacheKey.identifier());
          }

          return existed;
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return withLock(() -> cacheData.getIfPresent(EntityCacheKey.of(ident, type)) != null);
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
  public <E extends Entity & HasIdentifier> void put(
      E srcEntity, E destEntity, SupportsRelationOperations.Type relType) {
    withLock(
        () -> {
          NameIdentifier identifier = getIdentFromMetadata(srcEntity);
          EntityCacheKey entityCacheKey = EntityCacheKey.of(identifier, srcEntity.type(), relType);
          syncEntitiesToCache(entityCacheKey, destEntity);
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    withLock(
        () -> {
          NameIdentifier identifier = getIdentFromMetadata(entity);
          EntityCacheKey entityCacheKey = EntityCacheKey.of(identifier, entity.type());
          syncEntitiesToCache(entityCacheKey, entity);
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

  private void syncEntitiesToCache(EntityCacheKey key, List<Entity> entities) {
    List<Entity> entitiesFromCache = cacheData.getIfPresent(key);
    if (entitiesFromCache != null) {
      entitiesFromCache.addAll(entities);
      return;
    }
    cacheData.put(key, entities);
    cacheIndex.put(key.toString(), key);
  }

  private void syncEntitiesToCache(EntityCacheKey key, Entity entity) {
    cacheData.put(key, Lists.newArrayList(entity));
    cacheIndex.put(key.toString(), key);
  }

  /**
   * Removes the expired metadata from the cache. This method is a hook method for the Cache, when
   * an entry expires, it will call this method.
   *
   * @param key The key of the expired metadata
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    withLock(
        () -> {
          cacheIndex.remove(key.toString());
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
  @SuppressWarnings("unchecked")
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
   * @param identifier The identifier of the entities to invalidate
   */
  private void invalidateEntities(NameIdentifier identifier) {
    Iterable<EntityCacheKey> entityKeysToRemove =
        cacheIndex.getValuesForKeysStartingWith(identifier.toString());

    cacheData.invalidateAll(entityKeysToRemove);
    entityKeysToRemove.forEach(key -> cacheIndex.remove(key.toString()));
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

  /** Starts the cache stats monitor. */
  private void startCacheStatsMonitor() {
    scheduler.scheduleAtFixedRate(
        () -> {
          CacheStats stats = cacheData.stats();
          LOG.info(
              "[Cache Stats] hitRate={}, hitCount={}, missCount={}, loadSuccess={}, loadFailure={}, evictions={}",
              String.format("%.4f", stats.hitRate()),
              stats.hitCount(),
              stats.missCount(),
              stats.loadSuccessCount(),
              stats.loadFailureCount(),
              stats.evictionCount());
        },
        0,
        5,
        TimeUnit.MINUTES);
  }
}
