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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements the {@link org.apache.gravitino.cache.EntityCache} using Caffeine */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final int CACHE_CLEANUP_CORE_THREADS = 1;
  private static final int CACHE_CLEANUP_MAX_THREADS = 1;
  private static final int CACHE_CLEANUP_QUEUE_CAPACITY = 100;
  private static final int CACHE_MONITOR_PERIOD_MINUTES = 5;
  private static final int CACHE_MONITOR_INITIAL_DELAY_MINUTES = 0;
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());
  private final ReentrantLock opLock = new ReentrantLock();

  /** Cache part */
  private final Cache<EntityCacheKey, List<Entity>> cacheData;

  /** Index part */
  private RadixTree<EntityCacheKey> cacheIndex;

  private ScheduledExecutorService scheduler;

  /**
   * Constructs a new {@link CaffeineEntityCache}.
   *
   * @param cacheConfig the cache configuration
   */
  public CaffeineEntityCache(Config cacheConfig) {
    super(cacheConfig);
    this.cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    ThreadPoolExecutor cleanupExec = buildCleanupExecutor();
    Caffeine<EntityCacheKey, List<Entity>> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
                return;
              }
              try {
                invalidateExpiredItem(key);
              } catch (Throwable t) {
                LOG.error(
                    "Failed to remove entity key={} value={} from cache asynchronously, cause={}",
                    key,
                    value,
                    cause,
                    t);
              }
            });

    this.cacheData = cacheDataBuilder.build();

    if (cacheConfig.get(Configs.CACHE_STATS_ENABLED)) {
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
      startCacheStatsMonitor();
    }
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<List<E>> getIfPresent(
      SupportsRelationOperations.Type relType,
      NameIdentifier nameIdentifier,
      Entity.EntityType identType) {
    checkArguments(nameIdentifier, identType, relType);

    List<Entity> entitiesFromCache =
        cacheData.getIfPresent(EntityCacheKey.of(nameIdentifier, identType, relType));
    return Optional.ofNullable(entitiesFromCache).map(BaseEntityCache::convertEntities);
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);

    List<Entity> entitiesFromCache = cacheData.getIfPresent(EntityCacheKey.of(ident, type));

    return Optional.ofNullable(entitiesFromCache)
        .filter(l -> !l.isEmpty())
        .map(entities -> convertEntity(entities.get(0)));
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    checkArguments(ident, type, relType);

    return withLock(() -> invalidateEntities(ident));
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);

    return withLock(() -> invalidateEntities(ident));
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    checkArguments(ident, type, relType);
    return cacheData.getIfPresent(EntityCacheKey.of(ident, type, relType)) != null;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    return cacheData.getIfPresent(EntityCacheKey.of(ident, type)) != null;
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

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(
      NameIdentifier ident,
      Entity.EntityType type,
      SupportsRelationOperations.Type relType,
      List<E> entities) {
    checkArguments(ident, type, relType);
    Preconditions.checkArgument(entities != null, "Entities cannot be null");
    if (entities.isEmpty()) {
      return;
    }

    syncEntitiesToCache(
        EntityCacheKey.of(ident, type, relType),
        entities.stream().map(e -> (Entity) e).collect(Collectors.toList()));
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    withLock(
        () -> {
          invalidateOnKeyChange(entity);
          NameIdentifier identifier = getIdentFromEntity(entity);
          EntityCacheKey entityCacheKey = EntityCacheKey.of(identifier, entity.type());

          syncEntitiesToCache(entityCacheKey, Lists.newArrayList(entity));
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
    // Invalidate the cache if inserting the entity may affect related cache keys.
    // For example, inserting a model version changes the latest version of the model,
    // so the corresponding model cache entry should be invalidated.
    if (Objects.requireNonNull(entity.type()) == Entity.EntityType.MODEL_VERSION) {
      NameIdentifier modelIdent = ((ModelVersionEntity) entity).modelIdentifier();
      invalidate(modelIdent, Entity.EntityType.MODEL);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> void withCacheLock(ThrowingRunnable<E> action) throws E {
    Preconditions.checkArgument(action != null, "Action cannot be null");

    withLockAndThrow(action);
  }

  /** {@inheritDoc} */
  @Override
  public <E, T extends Exception> E withCacheLock(ThrowingSupplier<E, T> action) throws T {
    Preconditions.checkArgument(action != null, "Action cannot be null");

    return withLockAndThrow(action);
  }

  /**
   * Removes the expired entity from the cache. This method is a hook method for the Cache, when an
   * entry expires, it will call this method.
   *
   * @param key The key of the expired entity
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    withLock(
        () -> {
          cacheIndex.remove(key.toString());
        });
  }

  /**
   * Syncs the entities to the cache, if entities is too big and can not put to the cache, then it
   * will be removed from the cache and cacheIndex will not be updated.
   *
   * @param key The key of the entities.
   * @param newEntities The new entities to sync to the cache.
   */
  private void syncEntitiesToCache(EntityCacheKey key, List<Entity> newEntities) {
    List<Entity> existingEntities = cacheData.getIfPresent(key);

    if (existingEntities != null && key.relationType() != null) {
      List<Entity> merged = new ArrayList<>(existingEntities);
      merged.addAll(newEntities);

      cacheData.put(key, merged);
    } else {
      cacheData.put(key, newEntities);
    }

    if (cacheData.policy().getIfPresentQuietly(key) != null) {
      cacheIndex.put(key.toString(), key);
    }
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
  private <KEY, VALUE> Caffeine<KEY, VALUE> newBaseBuilder(Config cacheConfig) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();

    if (cacheConfig.get(Configs.CACHE_WEIGHER_ENABLED)) {
      builder.maximumWeight(EntityCacheWeigher.getMaxWeight());
      builder.weigher(EntityCacheWeigher.getInstance());
    } else {
      builder.maximumSize(cacheConfig.get(Configs.CACHE_MAX_ENTRIES));
    }

    if (cacheConfig.get(Configs.CACHE_EXPIRATION_TIME) > 0) {
      builder.expireAfterWrite(
          cacheConfig.get(Configs.CACHE_EXPIRATION_TIME), TimeUnit.MILLISECONDS);
    }

    if (cacheConfig.get(Configs.CACHE_STATS_ENABLED)) {
      builder.recordStats();
    }

    return (Caffeine<KEY, VALUE>) builder;
  }

  /**
   * Invalidates the entities by the given cache key.
   *
   * @param identifier The identifier of the entity to invalidate
   */
  private boolean invalidateEntities(NameIdentifier identifier) {
    List<EntityCacheKey> entityKeysToRemove =
        Lists.newArrayList(cacheIndex.getValuesForKeysStartingWith(identifier.toString()));

    cacheData.invalidateAll(entityKeysToRemove);
    entityKeysToRemove.forEach(key -> cacheIndex.remove(key.toString()));

    return !entityKeysToRemove.isEmpty();
  }

  /**
   * Runs the given action with the lock.
   *
   * @param action The action to run with the lock
   */
  private void withLock(Runnable action) {
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

  /**
   * Runs the given action with the lock and returns the result.
   *
   * @param action The action to run with the lock
   * @param <T> The type of the result
   * @return The result of the action
   */
  private <T> T withLock(Supplier<T> action) {
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

  /**
   * Runs the given action with the lock and throws the exception if it occurs.
   *
   * @param action The action to run with the lock
   * @param <T> The type of the result
   * @return The result of the action
   * @throws IOException If an exception occurs during the action
   */
  private <T, E extends Exception> T withLockAndThrow(ThrowingSupplier<T, E> action) throws E {
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

  /**
   * Runs the given action with the lock and throws the exception if it occurs.
   *
   * @param action The action to run with the lock
   * @param <E> The type of the exception
   * @throws E If an exception occurs during the action
   */
  private <E extends Exception> void withLockAndThrow(ThrowingRunnable<E> action) throws E {
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

  /**
   * Builds the cleanup executor.
   *
   * @return The cleanup executor
   */
  private ThreadPoolExecutor buildCleanupExecutor() {
    return new ThreadPoolExecutor(
        CACHE_CLEANUP_CORE_THREADS,
        CACHE_CLEANUP_MAX_THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(CACHE_CLEANUP_QUEUE_CAPACITY),
        r -> {
          Thread t = new Thread(r, "CaffeineEntityCache-Cleanup");
          t.setDaemon(true);
          return t;
        },
        new ThreadPoolExecutor.CallerRunsPolicy());
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
        CACHE_MONITOR_INITIAL_DELAY_MINUTES,
        CACHE_MONITOR_PERIOD_MINUTES,
        TimeUnit.MINUTES);
  }

  /**
   * Checks the arguments for the methods. All arguments must not be null.
   *
   * @param ident The identifier of the entity to check
   * @param type The type of the entity to check
   * @param relType The relation type of the entity to check
   */
  private void checkArguments(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    checkArguments(ident, type);
    Preconditions.checkArgument(relType != null, "relType cannot be null");
  }

  /**
   * Checks the arguments for the methods. All arguments must not be null.
   *
   * @param ident The identifier of the entity to check
   * @param type The type of the entity to check
   */
  private void checkArguments(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
  }
}
