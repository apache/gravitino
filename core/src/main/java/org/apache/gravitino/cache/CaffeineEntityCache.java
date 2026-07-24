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
import com.google.common.collect.Sets;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link org.apache.gravitino.cache.EntityCache} using Caffeine.
 *
 * <p>The cache stores one entry per entity, keyed by the entity's {@code NameIdentifier} and type.
 * A radix-tree prefix index over the cache keys implements cascading removal: invalidating an
 * entity also drops every cached descendant entry (e.g. invalidating a catalog drops the cached
 * schemas and tables under it).
 *
 * <p>Relation query results are NOT cached by this implementation; relation and list operations
 * always fall back to the {@code EntityStore}. Only the self-contained metadata objects listed in
 * {@link #CACHEABLE_TYPES} are cached; every other type (user/group/role, model/model version,
 * function, and operational entities) is read straight from the {@code EntityStore}.
 */
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

  /**
   * Entity types this cache is allowed to hold. Only self-contained entities are cacheable: a stale
   * copy of one is at worst cosmetically old (an old comment, property, or job status), never a
   * wrong pointer, and each can be invalidated with a single one-to-one key drop.
   *
   * <p>Every other type is read straight from the store. user/group/role embed relation-derived
   * data (a role's securable objects, a user's / group's role names) that a per-node cache cannot
   * invalidate; model/model version and function carry a load-bearing pointer (a version URI, the
   * latest version, the function implementation) that would be silently wrong if served stale.
   */
  private static final Set<Entity.EntityType> CACHEABLE_TYPES =
      Sets.immutableEnumSet(
          Entity.EntityType.METALAKE,
          Entity.EntityType.CATALOG,
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.TOPIC,
          Entity.EntityType.VIEW,
          Entity.EntityType.FILESET,
          Entity.EntityType.TAG,
          Entity.EntityType.POLICY,
          Entity.EntityType.JOB);

  /** Segmented locking for better concurrency */
  private final SegmentedLock segmentedLock;

  /** Cache data structure. */
  private final Cache<EntityCacheKey, Entity> cacheData;

  /** Prefix index over cache keys, used for cascading removal of descendant entries. */
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

    // Initialize segmented lock
    int lockSegments = cacheConfig.get(Configs.CACHE_LOCK_SEGMENTS);
    this.segmentedLock = new SegmentedLock(lockSegments);

    Caffeine<EntityCacheKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(CLEANUP_EXECUTOR)
        .removalListener(
            (key, value, cause) -> {
              LOG.debug("Removed entity cache entry, key={}, cause={}", key, cause);
              if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
                return;
              }
              try {
                invalidateExpiredItem(key);
              } catch (Throwable t) {
                LOG.error(
                    "Failed to remove entity key={} from cache asynchronously, cause={}",
                    key,
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

  @VisibleForTesting
  public Cache<EntityCacheKey, Entity> getCacheData() {
    return this.cacheData;
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);

    Entity entityFromCache = cacheData.getIfPresent(EntityCacheKey.of(ident, type));

    return Optional.ofNullable(entityFromCache).map(BaseEntityCache::convertEntity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    return segmentedLock.withLock(
        key,
        () -> {
          invalidateHierarchy(key);
          return true;
        });
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
    return cacheIndex.size();
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    segmentedLock.withGlobalLock(
        () -> {
          cacheData.invalidateAll();
          cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    if (!CACHEABLE_TYPES.contains(entity.type())) {
      return;
    }

    NameIdentifier identifier = getIdentFromEntity(entity);
    EntityCacheKey entityCacheKey = EntityCacheKey.of(identifier, entity.type());

    segmentedLock.withLock(
        entityCacheKey,
        () -> {
          cacheData.put(entityCacheKey, entity);
          // If the entry was rejected (e.g. it exceeds the maximum weight), skip indexing it.
          if (cacheData.policy().getIfPresentQuietly(entityCacheKey) != null) {
            cacheIndex.put(entityCacheKey.toString(), entityCacheKey);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
    // Every cacheable entity is self-contained (see CACHEABLE_TYPES), so inserting one never
    // requires invalidating a different key. Kept for the SPI contract; implementations that cache
    // derived entries can override this.
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

  /** {@inheritDoc} */
  @Override
  public <T, E extends Exception> T withMultipleKeyCacheLock(
      List<EntityCacheKey> keys, EntityCache.ThrowingSupplier<T, E> action) throws E {
    Preconditions.checkArgument(keys != null, "Keys cannot be null");
    Preconditions.checkArgument(action != null, "Action cannot be null");

    return segmentedLock.withMultipleKeyLockAndThrow(keys, action);
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
          cacheIndex.remove(key.toString());
        });
  }

  /**
   * Removes the entry for the given key and all cached descendant entries. Descendants are found
   * through the prefix index: every child identifier starts with {@code parent identifier + "."},
   * so the scan is exact for children and never matches siblings sharing a name prefix (e.g. {@code
   * catalog1} vs {@code catalog10}).
   *
   * @param key The key of the entity whose subtree should be invalidated
   */
  private void invalidateHierarchy(EntityCacheKey key) {
    cacheData.invalidate(key);
    cacheIndex.remove(key.toString());

    String childPrefix = key.identifier().toString() + ".";
    List<EntityCacheKey> childKeys =
        Lists.newArrayList(cacheIndex.getValuesForKeysStartingWith(childPrefix));
    for (EntityCacheKey childKey : childKeys) {
      cacheData.invalidate(childKey);
      cacheIndex.remove(childKey.toString());
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
      builder.expireAfterAccess(
          cacheConfig.get(Configs.CACHE_EXPIRATION_TIME), TimeUnit.MILLISECONDS);
    }

    if (cacheConfig.get(Configs.CACHE_STATS_ENABLED)) {
      builder.recordStats();
    }

    return (Caffeine<KEY, VALUE>) builder;
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
   */
  private void checkArguments(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
  }
}
