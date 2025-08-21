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
import com.google.common.collect.Sets;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements the {@link org.apache.gravitino.cache.EntityCache} using Caffeine */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final int CACHE_CLEANUP_CORE_THREADS = 1;
  private static final int CACHE_CLEANUP_MAX_THREADS = 1;
  private static final int CACHE_CLEANUP_QUEUE_CAPACITY = 100;
  private static final int CACHE_MONITOR_PERIOD_MINUTES = 5;
  private static final int CACHE_MONITOR_INITIAL_DELAY_MINUTES = 0;
  private static final ExecutorService CLEANUP_EXECUTOR =
      new ThreadPoolExecutor(
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

  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());
  private final ReentrantLock opLock = new ReentrantLock();

  /** Cache data structure. cacheData[0] = Role1-KEY -> [catalog1, catalog2] */
  private final Cache<EntityCacheRelationKey, List<Entity>> cacheData;

  /**
   * Cache reverse index structure. cacheData[0] = Role1 -> [catalog1, catalog2] cacheData[1] =
   * catalog1 -> [tab1, tab2] reverseIndex[0] = catalog1-KEY -> Role1 reverseIndex[1] = catalog2-KEY
   * -> Role1 reverseIndex[3] = tab1-KEY -> catalog1 reverseIndex[4] = tab2-KEY -> catalog1
   */
  private RadixTree<EntityCacheKey> reverseIndex;

  /**
   * Cache Index structure. cacheData[0] = Role1 -> [catalog1, catalog2] cacheData[1] = catalog1 ->
   * [tab1, tab2] cacheIndex[0] = Role1-KEY -> Role1 cacheIndex[1] = Role1-KEY -> Role1
   */
  private RadixTree<EntityCacheRelationKey> cacheIndex;

  private ScheduledExecutorService scheduler;

  /**
   * Constructs a new {@link CaffeineEntityCache}.
   *
   * @param cacheConfig the cache configuration
   */
  public CaffeineEntityCache(Config cacheConfig) {
    super(cacheConfig);
    this.cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
    this.reverseIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    Caffeine<EntityCacheKey, List<Entity>> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(CLEANUP_EXECUTOR)
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
        cacheData.getIfPresent(EntityCacheRelationKey.of(nameIdentifier, identType, relType));
    return Optional.ofNullable(entitiesFromCache).map(BaseEntityCache::convertEntities);
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);

    List<Entity> entitiesFromCache = cacheData.getIfPresent(EntityCacheRelationKey.of(ident, type));

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
    return cacheData.getIfPresent(EntityCacheRelationKey.of(ident, type, relType)) != null;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    return cacheData.getIfPresent(EntityCacheRelationKey.of(ident, type)) != null;
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return cacheIndex.size();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    withLock(
        () -> {
          cacheData.invalidateAll();
          reverseIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
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
    withLock(
        () -> {
          if (entities.isEmpty()) {
            return;
          }

          syncEntitiesToCache(
              EntityCacheRelationKey.of(ident, type, relType),
              entities.stream().map(e -> (Entity) e).collect(Collectors.toList()));
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    withLock(
        () -> {
          invalidateOnKeyChange(entity);
          NameIdentifier identifier = getIdentFromEntity(entity);
          EntityCacheRelationKey entityCacheKey =
              EntityCacheRelationKey.of(identifier, entity.type());

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
          reverseIndex.remove(key.toString());
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
  private void syncEntitiesToCache(EntityCacheRelationKey key, List<Entity> newEntities) {
    List<Entity> existingEntities = cacheData.getIfPresent(key);

    if (existingEntities != null && key.relationType() != null) {
      Set<Entity> merged = Sets.newLinkedHashSet(existingEntities);
      merged.addAll(newEntities);
      newEntities = new ArrayList<>(merged);
    }

    // DEBUG code
    check_cache();

    List<String> allCacheIndexKeys1 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith("").forEach(
            k -> {
              allCacheIndexKeys1.add(k.toString());
            });

    List<String> allReverseIndexKeys1 = Lists.newArrayList();
    reverseIndex.getKeysStartingWith("").forEach(
            k -> {
              allReverseIndexKeys1.add(k.toString());
            });

    cacheData.put(key, newEntities);

    for (Entity entity : newEntities) {
        if (entity instanceof UserEntity) {
          // UserEntity is not supported in the cache, skip it.
          UserEntity userEntity = (UserEntity) entity;
          if (userEntity.roleNames() != null) {
            userEntity.roleNames().forEach(
                    role -> {
                      Namespace ns = NamespaceUtil.ofRole(userEntity.namespace().level(0));
                      NameIdentifier nameIdentifier = NameIdentifier.of(ns, role);
                      putReverseIndex(nameIdentifier, Entity.EntityType.ROLE, key);
                    });
          }
        }

//      Namespace ns = NamespaceUtil.ofRole(ident.namespace().level(0));
//      NameIdentifier nameIdentifier = NameIdentifier.of(ns, role);

      putReverseIndex(entity, key);
    }

    if (cacheData.policy().getIfPresentQuietly(key) != null) {
      cacheIndex.put(key.toString(), key);
    }

    List<String> allCacheIndexKeys2 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith("").forEach(
            k -> {
              allCacheIndexKeys2.add(k.toString());
            });

    List<String> allReverseIndexKeys2 = Lists.newArrayList();
    reverseIndex.getKeysStartingWith("").forEach(
            k -> {
              allReverseIndexKeys2.add(k.toString());
            });

    // DEBUG code
    check_cache();
  }

  private void check_cache() {
    // DEBUG code
    long cacheDataSize = cacheData.estimatedSize();
    int cacheIndexSize = cacheIndex.size();
    int reverseIndexSize = reverseIndex.size();
    if (cacheDataSize != cacheIndexSize
            || cacheDataSize != reverseIndexSize
            || cacheIndexSize != reverseIndexSize) {
      throw new IllegalStateException(
              String.format(
                      "Cache data size (%d) does not match cache index size (%d) or reverse index size (%d).",
                      cacheDataSize, cacheIndexSize, reverseIndexSize));
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

  private NameIdentifier getNameIdentifier(Entity entity) {
    NameIdentifier nameIdent =
        NameIdentifier.of(((HasIdentifier) entity).namespace(), ((HasIdentifier) entity).name());
    return nameIdent;
  }

  private EntityCacheKey makeEntityCacheKey(Entity entity) {
    NameIdentifier nameIdent = getNameIdentifier(entity);
    EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdent, entity.type());
    return entityCacheKey;
  }

  public void putReverseIndex(NameIdentifier nameIdentifier, Entity.EntityType type, EntityCacheRelationKey key) {
//    EntityCacheKey entityCacheKey = makeEntityCacheKey(entity);
    EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdentifier, type);
    String strEntityCacheKey = entityCacheKey.toString();
    List<EntityCacheKey> entityKeysToRemove =
        Lists.newArrayList(reverseIndex.getValuesForKeysStartingWith(strEntityCacheKey));
    String strEntityCacheKeyNo =
        String.format("%s-%d", strEntityCacheKey, entityKeysToRemove.size());
    reverseIndex.put(strEntityCacheKeyNo, key);
  }

  public void putReverseIndex(Entity entity, EntityCacheRelationKey key) {
    Preconditions.checkArgument(entity != null, "EntityCacheRelationKey cannot be null");

    if (entity instanceof HasIdentifier) {
      EntityCacheKey entityCacheKey = makeEntityCacheKey(entity);
      String strEntityCacheKey = entityCacheKey.toString();
      List<EntityCacheKey> entityKeysToRemove =
              Lists.newArrayList(reverseIndex.getValuesForKeysStartingWith(strEntityCacheKey));
      String strEntityCacheKeyNo =
              String.format("%s-%d", strEntityCacheKey, entityKeysToRemove.size());
      reverseIndex.put(strEntityCacheKeyNo, key);
    }
  }

  /**
   * Invalidates the entities by the given cache key.
   *
   * @param identifier The identifier of the entity to invalidate
   */
  private boolean invalidateEntities(NameIdentifier identifier) {
    List<String> allCacheIndexKeys1 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith("").forEach(
            k -> {
              allCacheIndexKeys1.add(k.toString());
            });

    List<String> relationCacheIndex2 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith(identifier.toString())
            .forEach(key -> {
              relationCacheIndex2.add(key.toString());
            });

    List<String> allReverseIndexKeys1 = Lists.newArrayList();
    reverseIndex.getKeysStartingWith("").forEach(
            k -> {
              allReverseIndexKeys1.add(k.toString());
            });

    List<EntityCacheKey> entityKeysToRemove =
        Lists.newArrayList(cacheIndex.getValuesForKeysStartingWith(identifier.toString()));

    // DEBUG code
    check_cache();

    Map<EntityCacheRelationKey, List<Entity>> relationEnitiesMap =
        cacheData.getAllPresent(entityKeysToRemove);
    // SCENE[1]
    // RECORD1 = Role1 -> [catalog1, catalog2]
    // RECORD2 = catalog1 -> [tab1, tab2]
    // INVALIDATE Role1, then need to remove RECORD1 and RECORD2
    relationEnitiesMap.forEach(
        (key, entities) -> {
          if (key.relationType() == null) {
            // If the relation type is null, it means it's a single entity, we can skip it.
            return;
          }
          entities.forEach(
              entity -> {
                NameIdentifier child = getNameIdentifier(entity);
                if (!child.equals(identifier)) {
                  invalidateEntities(child);
                }
              });
        });

    // SCENE[2]
    // RECORD1 = Role1 -> [catalog1, catalog2]
    // RECORD2 = catalog1 -> [tab1, tab2]
    // INVALIDATE catalog1, then need to remove RECORD1 and RECORD2
    //
    // SCENE[3]
    // RECORD1 = Metadata1 -> []
    // RECORD2 = Metadata1.Catalog1.tab1 -> []
    // INVALIDATE Metadata1, then need to remove RECORD1 and RECORD2
    List<EntityCacheKey> reverseKeysToRemove =
        Lists.newArrayList(reverseIndex.getValuesForKeysStartingWith(identifier.toString()));
    reverseKeysToRemove.forEach(
        key -> {
          // If the key is the same as the entity key, we can remove it from the cache.
          cacheData.invalidate(key);
          boolean removed1 = cacheIndex.remove(key.toString());
          // Remove from reverse index
          // Convert EntityCacheRelationKey to EntityCacheKey
          EntityCacheKey reverseKey = EntityCacheKey.of(key.identifier(), key.entityType());
          reverseIndex
              .getKeysStartingWith(reverseKey.toString())
              .forEach(
                  reverseIndexKey -> {
                    boolean removed2 = reverseIndex.remove(reverseIndexKey.toString());
                  });
        });

    long cacheDataSize = cacheData.estimatedSize();
    int cacheIndexSize = cacheIndex.size();
    int reverseIndexSize = reverseIndex.size();

    cacheData.invalidateAll(entityKeysToRemove);

    long cacheDataSize1 = cacheData.estimatedSize();
    int cacheIndexSize1 = cacheIndex.size();
    int reverseIndexSize1 = reverseIndex.size();

    List<String> allCacheIndexKeys11 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith("").forEach(
            k -> {
              allCacheIndexKeys11.add(k.toString());
            });

    List<String> relationCacheIndex21 = Lists.newArrayList();
    cacheIndex.getKeysStartingWith(identifier.toString())
            .forEach(key -> {
              relationCacheIndex21.add(key.toString());
            });

    List<String> allReverseIndexKeys11 = Lists.newArrayList();
    reverseIndex.getKeysStartingWith("").forEach(
            k -> {
              allReverseIndexKeys11.add(k.toString());
            });

    // DEBUG code
    check_cache();

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
