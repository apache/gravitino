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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
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
 * {@link BaseEntityCache#isCacheable} are cached; every other type (user/group/role, model/model
 * version, function, job template, and any type not in that allowlist) is read straight from the
 * {@code EntityStore}.
 */
public class CaffeineEntityCache extends BaseEntityCache {
  private static final int CACHE_CLEANUP_CORE_THREADS = 1;
  private static final int CACHE_CLEANUP_MAX_THREADS = 1;
  private static final int CACHE_CLEANUP_QUEUE_CAPACITY = 100;
  private static final int CACHE_MONITOR_PERIOD_MINUTES = 5;
  private static final int CACHE_MONITOR_INITIAL_DELAY_MINUTES = 0;

  /**
   * Runs Caffeine's maintenance work and removal notifications.
   *
   * <p>The removal listener can still run while Caffeine holds its eviction lock: when this queue
   * is full, {@link ThreadPoolExecutor.CallerRunsPolicy} runs the task on the submitting thread,
   * which may be the one performing eviction; Caffeine also runs a task inline if an executor
   * throws on submission. Meanwhile a writer that holds a segment lock can block on the eviction
   * lock in {@code cacheData.put}, because Caffeine cleans up synchronously when its write buffer
   * is full. The removal listener must therefore never wait for a segment lock.
   */
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

  /**
   * Removes evicted and expired keys from the prefix index. The removal listener only hands keys to
   * this executor; the cleanup itself takes the key's segment lock, which is safe here because this
   * thread never holds a Caffeine lock.
   *
   * <p>The queue is unbounded and the executor is never shut down, so handing off a key never fails
   * or runs the cleanup on Caffeine's thread. Each task holds only a key, so the backlog is bounded
   * by the number of pending evictions. A segment held across a slow store read delays the cleanup
   * behind it, which only lets {@link #size()} over-count and the stale keys use memory for longer.
   */
  private static final ExecutorService INDEX_CLEANUP_EXECUTOR =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "CaffeineEntityCache-IndexCleanup");
            t.setDaemon(true);
            return t;
          });

  private static final Logger LOG = LoggerFactory.getLogger(CaffeineEntityCache.class.getName());

  /**
   * Separates {@link NameIdentifier} levels in a cache key. See {@link
   * #invalidateHierarchy(EntityCacheKey)} for why it is not the only child boundary.
   */
  private static final String NAME_LEVEL_BOUNDARY = ".";

  /** Segmented locking for better concurrency */
  private final SegmentedLock segmentedLock;

  /** Cache data structure. */
  private final Cache<EntityCacheKey, Entity> cacheData;

  /**
   * Prefix index over cache keys, used for cascading removal of descendant entries.
   *
   * <p>The tree itself is thread-safe, but {@link #clear()} swaps in a brand-new tree, so the field
   * is {@code volatile} to publish that swap to readers that do not hold a segment lock (see {@link
   * #size()}).
   */
  private volatile RadixTree<EntityCacheKey> cacheIndex;

  private final Executor indexCleanupExecutor;

  private ScheduledExecutorService scheduler;

  /**
   * Constructs a new {@link CaffeineEntityCache}.
   *
   * @param cacheConfig the cache configuration
   */
  public CaffeineEntityCache(Config cacheConfig) {
    this(cacheConfig, CLEANUP_EXECUTOR, INDEX_CLEANUP_EXECUTOR);
  }

  /**
   * Constructs a new {@link CaffeineEntityCache} with the given executors.
   *
   * @param cacheConfig the cache configuration
   * @param maintenanceExecutor the executor Caffeine uses for maintenance and removal notifications
   * @param indexCleanupExecutor the executor that removes evicted and expired keys from the prefix
   *     index; it must not run a task on the submitting thread while that thread may hold a
   *     Caffeine lock
   */
  @VisibleForTesting
  CaffeineEntityCache(
      Config cacheConfig, Executor maintenanceExecutor, Executor indexCleanupExecutor) {
    super(cacheConfig);
    this.indexCleanupExecutor = indexCleanupExecutor;
    this.cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    // Initialize segmented lock
    int lockSegments = cacheConfig.get(Configs.CACHE_LOCK_SEGMENTS);
    this.segmentedLock = new SegmentedLock(lockSegments);

    Caffeine<EntityCacheKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(maintenanceExecutor)
        .removalListener(
            (key, value, cause) -> {
              LOG.debug("Removed entity cache entry, key={}, cause={}", key, cause);
              if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
                return;
              }
              // This listener can run while Caffeine holds its eviction lock, and a writer can hold
              // a segment lock while waiting for that eviction lock in cacheData.put. Taking the
              // segment lock here would close that cycle, so only hand the key off.
              scheduleIndexCleanup(key, cause);
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

  /**
   * {@inheritDoc}
   *
   * <p>Read from the prefix index without holding a segment lock, so the result is a point-in-time
   * estimate: entries concurrently added or cascaded away may or may not be counted. It may also
   * briefly exceed the number of live entries, because entries evicted by Caffeine are removed from
   * the index asynchronously by the index cleanup executor.
   */
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
  protected <E extends Entity & HasIdentifier> void doPut(E entity) {
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
    // Every cacheable entity is self-contained (see BaseEntityCache#isCacheable), so inserting one
    // never requires invalidating a different key. Kept for the SPI contract; implementations that
    // cache derived entries can override this.
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
   * Removes a key that Caffeine evicted or expired from the prefix index. Runs on the index cleanup
   * executor, never on a thread that holds a Caffeine lock.
   *
   * <p>A put can add the key back between Caffeine removing it and this cleanup running. That put
   * indexes the key under the same segment lock, so the index entry is removed only when the key is
   * still absent from the cache; otherwise it belongs to the live entry.
   *
   * @param key The key of the evicted or expired entity
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    segmentedLock.withLock(
        key,
        () -> {
          if (cacheData.policy().getIfPresentQuietly(key) == null) {
            cacheIndex.remove(key.toString());
          }
        });
  }

  /**
   * Hands an evicted or expired key to the index cleanup executor without blocking.
   *
   * @param key The key Caffeine removed
   * @param cause Why Caffeine removed it
   */
  private void scheduleIndexCleanup(EntityCacheKey key, RemovalCause cause) {
    try {
      indexCleanupExecutor.execute(
          () -> {
            try {
              invalidateExpiredItem(key);
            } catch (Throwable t) {
              LOG.error(
                  "Failed to remove entity key={} from the cache index, cause={}", key, cause, t);
            }
          });
    } catch (RejectedExecutionException e) {
      // Unreachable with the production executor, which is unbounded and never shut down. Never
      // fall
      // back to running the cleanup here: this thread may hold Caffeine's eviction lock. A key left
      // in the index only costs memory; a later invalidation of it or an ancestor removes it.
      LOG.error("Failed to schedule cache index cleanup for key={}, cause={}", key, cause, e);
    }
  }

  /**
   * Removes the entry for the given key and all cached descendant entries. Descendants are found
   * through the prefix index, scanning once per child boundary:
   *
   * <ul>
   *   <li>{@code "."} separates {@link NameIdentifier} levels, so it matches ordinary children such
   *       as the tables of a schema.
   *   <li>The {@link HierarchicalSchemaUtil#schemaSeparator() schema separator} joins nested {@code
   *       HierarchicalSchema} levels <em>inside</em> a single name level, so it matches nested
   *       schemas such as {@code raw:events:2024} under {@code raw:events}. Without this pass those
   *       descendants would survive until their TTL expires. The cache sits above the storage
   *       layer, where schema names are still logical, so the boundary is the configured external
   *       separator and not the physical one the entity store writes to the backend. Only a schema
   *       can carry nested levels, so this pass is skipped for every other entity type; a catalog
   *       still reaches its nested schemas through the {@code "."} pass above.
   * </ul>
   *
   * <p>Matching on a boundary rather than the bare identifier is what keeps the scan exact: it
   * never matches siblings sharing a name prefix, neither {@code catalog1} vs {@code catalog10} nor
   * {@code raw:events} vs {@code raw:events2}. Because the index matches on the whole key string,
   * the separator pass already collects descendants at any depth, so no recursion is needed.
   *
   * @param key The key of the entity whose subtree should be invalidated
   */
  private void invalidateHierarchy(EntityCacheKey key) {
    cacheData.invalidate(key);
    cacheIndex.remove(key.toString());

    String identifier = key.identifier().toString();
    invalidateDescendants(identifier + NAME_LEVEL_BOUNDARY);
    if (key.entityType() == Entity.EntityType.SCHEMA) {
      invalidateDescendants(identifier + HierarchicalSchemaUtil.schemaSeparator());
    }
  }

  /**
   * Removes every cached entry whose key starts with the given prefix.
   *
   * @param keyPrefix The prefix that identifies the descendants to remove
   */
  private void invalidateDescendants(String keyPrefix) {
    List<EntityCacheKey> childKeys =
        Lists.newArrayList(cacheIndex.getValuesForKeysStartingWith(keyPrefix));
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
      // Expire after write, not after access. The TTL is the safety net for a cross-node
      // invalidation that never arrives (a lost entity_change_log row, a stalled poller). With an
      // access-based TTL a stale entry that keeps being read would never expire, so a single missed
      // invalidation would become permanent on exactly the hottest keys. A write-based TTL bounds
      // that staleness to expireTimeInMs.
      builder.expireAfterWrite(
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
