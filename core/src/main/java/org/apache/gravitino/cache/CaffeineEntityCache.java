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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

  /** Segmented locking for better concurrency */
  private final SegmentedLock segmentedLock;

  /** Cache data structure. */
  private final Cache<EntityCacheRelationKey, List<Entity>> cacheData;

  /** Cache reverse index structure. */
  private ReverseIndexCache reverseIndex;

  /** Cache Index structure. */
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
    this.reverseIndex = new ReverseIndexCache();

    // Initialize segmented lock
    int lockSegments = cacheConfig.get(Configs.CACHE_LOCK_SEGMENTS);
    this.segmentedLock = new SegmentedLock(lockSegments);

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

    return segmentedLock.withLock(
        EntityCacheRelationKey.of(ident, type, relType),
        () -> invalidateEntities(ident, type, Optional.of(relType)));
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    return segmentedLock.withLock(
        EntityCacheRelationKey.of(ident, type),
        () -> invalidateEntities(ident, type, Optional.empty()));
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
    segmentedLock.withGlobalLock(
        () -> {
          cacheData.invalidateAll();
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
    EntityCacheRelationKey entityCacheKey = EntityCacheRelationKey.of(ident, type, relType);
    segmentedLock.withLock(
        entityCacheKey,
        () -> {
          if (entities.isEmpty()) {
            return;
          }

          syncEntitiesToCache(
              entityCacheKey, entities.stream().map(e -> (Entity) e).collect(Collectors.toList()));
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    NameIdentifier identifier = getIdentFromEntity(entity);
    EntityCacheRelationKey entityCacheKey = EntityCacheRelationKey.of(identifier, entity.type());

    segmentedLock.withLock(
        entityCacheKey,
        () -> {
          invalidateOnKeyChange(entity);
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
  public <E extends Exception> void withCacheLock(
      EntityCacheKey key, EntityCache.ThrowingRunnable<E> action) throws E {
    Preconditions.checkArgument(key != null, "Key cannot be null");
    Preconditions.checkArgument(action != null, "Action cannot be null");

    segmentedLock.withLockAndThrow(key, action);
  }

  /** {@inheritDoc} */
  @Override
  public <E, T extends Exception> E withCacheLock(
      EntityCacheKey key, EntityCache.ThrowingSupplier<E, T> action) throws T {
    Preconditions.checkArgument(key != null, "Key cannot be null");
    Preconditions.checkArgument(action != null, "Action cannot be null");

    return segmentedLock.withLockAndThrow(key, action);
  }

  /**
   * Removes the expired entity from the cache. This method is a hook method for the Cache, when an
   * entry expires, it will call this method.
   *
   * @param key The key of the expired entity
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    segmentedLock.withLock(
        key,
        () -> {
          reverseIndex.remove(key);
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
    if (key.relationType() != null) return;

    List<Entity> existingEntities = cacheData.getIfPresent(key);

    if (existingEntities != null && key.relationType() != null) {
      Set<Entity> merged = Sets.newLinkedHashSet(existingEntities);
      merged.addAll(newEntities);
      newEntities = new ArrayList<>(merged);
    }

    cacheData.put(key, newEntities);

    for (Entity entity : newEntities) {
      reverseIndex.indexEntity(entity, key);
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
   * Invalidate entities with iterative BFS algorithm.
   *
   * @param identifier The identifier of the entity to invalidate
   */
  private boolean invalidateEntities(
      NameIdentifier identifier,
      Entity.EntityType type,
      Optional<SupportsRelationOperations.Type> relTypeOpt) {
    Queue<EntityCacheKey> queue = new ArrayDeque<>();

    EntityCacheKey valueForExactKey =
        cacheIndex.getValueForExactKey(
            relTypeOpt.isEmpty()
                ? EntityCacheKey.of(identifier, type).toString()
                : EntityCacheRelationKey.of(identifier, type, relTypeOpt.get()).toString());

    if (valueForExactKey == null) {
      // No key to remove
      return false;
    }

    queue.offer(valueForExactKey);

    while (!queue.isEmpty()) {
      EntityCacheKey currentKeyToRemove = queue.poll();

      cacheData.invalidate(currentKeyToRemove);
      cacheIndex.remove(currentKeyToRemove.toString());

      // Remove related entity keys
      List<EntityCacheKey> relatedEntityKeysToRemove =
          Lists.newArrayList(
              cacheIndex.getValuesForKeysStartingWith(currentKeyToRemove.identifier().toString()));
      queue.addAll(relatedEntityKeysToRemove);

      // Look up from reverse index to go to next depth
      List<EntityCacheKey> reverseKeysToRemove =
          Lists.newArrayList(
              reverseIndex.getValuesForKeysStartingWith(
                  currentKeyToRemove.identifier().toString()));
      reverseKeysToRemove.forEach(
          key -> {
            // Remove from reverse index
            // Convert EntityCacheRelationKey to EntityCacheKey
            reverseIndex
                .getKeysStartingWith(key.toString())
                .forEach(
                    reverseIndexKey -> {
                      reverseIndex.remove(reverseIndexKey.toString());
                    });
          });

      queue.addAll(reverseKeysToRemove);
    }

    return true;
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
