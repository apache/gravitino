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
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.List;
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
import org.apache.gravitino.SupportsRelationOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a meta cache using Caffeine cache. */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());

  /** Singleton instance */
  private static volatile CaffeineEntityCache INSTANCE = null;

  /** Cache part */
  private final Cache<StoreEntityCacheKey, Entity> cacheData;

  private final Cache<RelationEntityCacheKey, List<Entity>> cacheRelations;
  /** Index part */
  private RadixTree<StoreEntityCacheKey> cacheDataIndex;

  private RadixTree<RelationEntityCacheKey> cacheRelationsIndex;

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
    cacheDataIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    cacheRelationsIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

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

    Caffeine<StoreEntityCacheKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                invalidateExpiredDataItemByMetadata(value);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    Caffeine<RelationEntityCacheKey, List<Entity>> cacheRelationsBuilder =
        newBaseBuilder(cacheConfig);
    cacheRelationsBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                invalidateExpiredRelationItemByMetadata(key);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    this.cacheData = cacheDataBuilder.build();
    this.cacheRelations = cacheRelationsBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> List<E> getOrLoad(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType)
      throws IOException {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
    Preconditions.checkArgument(relType != null, "SupportsRelationOperations.Type cannot be null");

    RelationEntityCacheKey relationEntityCacheKey = RelationEntityCacheKey.of(ident, type, relType);
    List<Entity> ifPresent = cacheRelations.getIfPresent(relationEntityCacheKey);

    if (ifPresent != null) {
      return convertSafe(ifPresent);
    }
    return withLockAndThrow(
        () -> {
          List<E> entities = entityStore.listEntitiesByRelation(relType, ident, type);
          syncEntityToRelationCache(ident, type, relType, toEntityList(entities));

          return entities;
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> E getOrLoad(
      NameIdentifier ident, Entity.EntityType type) throws IOException {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    Entity entityFromCache = cacheData.getIfPresent(StoreEntityCacheKey.of(ident, type));

    if (entityFromCache != null) {
      return convertSafe(entityFromCache);
    }

    return withLockAndThrow(
        () -> {
          E entityFromDb = entityStore.get(ident, type, getEntityClass(type));
          syncEntityToDataCache(entityFromDb);

          return entityFromDb;
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
          RelationEntityCacheKey relationEntityCacheKey =
              RelationEntityCacheKey.of(nameIdentifier, identType, relType);
          List<Entity> entitiesFromCache = cacheRelations.getIfPresent(relationEntityCacheKey);

          if (entitiesFromCache != null) {
            List<E> convertedEntities = convertSafe(entitiesFromCache);
            return Optional.of(convertedEntities);
          }
          return Optional.empty();
        });
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          E cachedEntity = (E) cacheData.getIfPresent(StoreEntityCacheKey.of(ident, type));
          return Optional.ofNullable(cachedEntity);
        });
  }

  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    return withLock(
        () -> {
          StoreEntityCacheKey storeEntityCacheKey = StoreEntityCacheKey.of(ident, type);

          boolean existed = contains(ident, type);
          if (existed) {
            invalidateEntities(storeEntityCacheKey.identifier());
          }

          return existed;
        });
  }

  @Override
  public boolean invalidate(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return withLock(
        () -> {
          RelationEntityCacheKey relationEntityCacheKey =
              RelationEntityCacheKey.of(ident, type, relType);

          boolean existed = contains(ident, type, relType);
          if (existed) {
            invalidateEntities(relationEntityCacheKey.identifier());
          }

          return existed;
        });
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    return withLock(() -> cacheData.getIfPresent(StoreEntityCacheKey.of(ident, type)) != null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
    return withLock(
        () -> cacheRelations.getIfPresent(RelationEntityCacheKey.of(ident, type, relType)) != null);
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfData() {
    return cacheData.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfRelations() {
    return cacheRelations.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public void clearStore() {
    withLock(
        () -> {
          cacheData.invalidateAll();

          cacheDataIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  @Override
  public void clearRelations(Entity entity) {
    withLock(
        () -> {
          cacheRelations.invalidateAll();

          cacheRelationsIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  @Override
  public void clear() {
    withLock(
        () -> {
          cacheData.invalidateAll();
          cacheRelations.invalidateAll();

          cacheDataIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
          cacheRelationsIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    withLock(
        () -> {
          syncEntityToDataCache(entity);
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(
      E srcEntity, E destEntity, SupportsRelationOperations.Type relType) {
    withLock(
        () -> {
          syncEntityToRelationCache(
              getIdentFromMetadata(srcEntity), srcEntity.type(), relType, destEntity);
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
  private void syncEntityToDataCache(Entity metadata) {
    NameIdentifier nameIdent = getIdentFromMetadata(metadata);
    Entity.EntityType type = metadata.type();
    StoreEntityCacheKey storeEntityCacheKey = StoreEntityCacheKey.of(nameIdent, type);

    cacheData.put(storeEntityCacheKey, metadata);
    cacheDataIndex.put(storeEntityCacheKey.toString(), storeEntityCacheKey);
  }

  private void syncEntityToRelationCache(
      NameIdentifier ident,
      Entity.EntityType type,
      SupportsRelationOperations.Type relType,
      Entity dstEntity) {
    RelationEntityCacheKey relationEntityCacheKey = RelationEntityCacheKey.of(ident, type, relType);
    List<Entity> cachedEntityList = cacheRelations.getIfPresent(relationEntityCacheKey);

    cacheRelationsIndex.put(relationEntityCacheKey.toString(), relationEntityCacheKey);
    if (cachedEntityList != null) {
      cachedEntityList.add(dstEntity);
    } else {
      cacheRelations.put(relationEntityCacheKey, Lists.newArrayList(dstEntity));
    }
  }

  private void syncEntityToRelationCache(
      NameIdentifier ident,
      Entity.EntityType type,
      SupportsRelationOperations.Type relType,
      List<Entity> dstEntities) {
    RelationEntityCacheKey relationEntityCacheKey = RelationEntityCacheKey.of(ident, type, relType);
    List<Entity> cachedEntityList = cacheRelations.getIfPresent(relationEntityCacheKey);
    if (cachedEntityList != null) {
      cachedEntityList.addAll(dstEntities);
    } else {
      cacheRelations.put(relationEntityCacheKey, dstEntities);
    }
  }

  /**
   * Removes the expired metadata from the cache. This method is a hook method for the Cache, when
   * an entry expires, it will call this method.
   *
   * @param metadata The metadata to remove,
   */
  @Override
  protected void invalidateExpiredDataItemByMetadata(Entity metadata) {
    NameIdentifier identifier = getIdentFromMetadata(metadata);
    Entity.EntityType type = metadata.type();
    StoreEntityCacheKey cacheKey = StoreEntityCacheKey.of(identifier, type);

    withLock(
        () -> {
          cacheDataIndex.remove(cacheKey.toString());
          Iterable<RelationEntityCacheKey> relationKeysToRemove =
              cacheRelationsIndex.getValuesForKeysStartingWith(identifier.toString());

          relationKeysToRemove.forEach(key -> cacheRelationsIndex.remove(key.toString()));
          cacheRelations.invalidateAll(relationKeysToRemove);
        });
  }

  @Override
  protected void invalidateExpiredRelationItemByMetadata(RelationEntityCacheKey key) {
    StoreEntityCacheKey storeEntityCacheKey = key.storeEntityCacheKey();
    withLock(
        () -> {
          cacheData.invalidate(storeEntityCacheKey);
          cacheRelationsIndex.remove(key.toString());
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
    Iterable<StoreEntityCacheKey> storeKeysToRemove =
        cacheDataIndex.getValuesForKeysStartingWith(identifier.toString());
    Iterable<RelationEntityCacheKey> relationKeysToRemove =
        cacheRelationsIndex.getValuesForKeysStartingWith(identifier.toString());

    cacheData.invalidateAll(storeKeysToRemove);
    cacheRelations.invalidateAll(relationKeysToRemove);

    storeKeysToRemove.forEach(key -> cacheDataIndex.remove(key.toString()));
    relationKeysToRemove.forEach(key -> cacheRelationsIndex.remove(key.toString()));
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
