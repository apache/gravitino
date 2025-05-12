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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a meta cache using Caffeine cache. */
public class CaffeineMetaCache extends BaseMetaCache {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(CaffeineMetaCache.class.getName());

  /** Singleton instance */
  private static volatile CaffeineMetaCache INSTANCE;

  /** Cache part */
  private final Cache<MetaCacheKey, Entity> cacheData;
  /** Index part */
  private RadixTree<MetaCacheKey> cacheIndex;

  private final ReentrantLock opLock = new ReentrantLock();
  private final ScheduledExecutorService statsScheduler;

  @VisibleForTesting
  static void resetForTest() {
    INSTANCE = null;
  }

  /**
   * Returns the instance of MetaCacheCaffeine based on the cache configuration and entity store.
   *
   * @param cacheConfig The cache configuration
   * @param entityStore The entity store to load entities from the database
   * @return The instance of {@link CaffeineMetaCache}
   */
  public static CaffeineMetaCache getInstance(CacheConfig cacheConfig, EntityStore entityStore) {
    if (INSTANCE == null) {
      synchronized (CaffeineMetaCache.class) {
        if (INSTANCE == null) {
          INSTANCE = new CaffeineMetaCache(cacheConfig, entityStore);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Returns the instance of MetaCacheCaffeine based on the cache configuration.
   *
   * @param cacheConfig The cache configuration
   * @return The instance of {@link CaffeineMetaCache}
   */
  public static CaffeineMetaCache getInstance(CacheConfig cacheConfig) {
    return getInstance(cacheConfig, null);
  }

  /**
   * Constructs a new MetaCacheCaffeine.
   *
   * @param cacheConfig the cache configuration
   * @param entityStore The entity store to load entities from the database
   */
  private CaffeineMetaCache(CacheConfig cacheConfig, EntityStore entityStore) {
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

    Caffeine<MetaCacheKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                removeExpiredMetadataFromDataCache(value);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    this.cacheData = cacheDataBuilder.build();
    this.statsScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "CaffeineMetaCache-Stats");
              t.setDaemon(true);
              return t;
            });

    initStatsScheduler();
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataById(Long id, Entity.EntityType type) {
    Preconditions.checkArgument(id != null, "Id cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    MetaCacheKey metaCacheKey = MetaCacheKey.of(id, type);

    return cacheData.get(
        metaCacheKey,
        key -> {
          Entity entity = loadMetadataFromDBById(id, type);
          if (entity == null) {
            throw new RuntimeException("Entity not found for id=" + id + " type=" + type);
          }
          syncMetadataToIndex(entity);

          return entity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    MetaCacheKey idKey = cacheIndex.getValueForExactKey(ident.toString());
    if (idKey != null) {
      return cacheData.getIfPresent(idKey);
    }
    try {
      Entity entity = loadMetadataFromDBByName(ident, type);
      if (entity == null) {
        throw new RuntimeException("Entity not found for name=" + ident + " type=" + type);
      }
      syncMetadataToCache(entity);

      return entity;
    } catch (IOException e) {
      LOG.error("Error while loading entity by name", e);
      throw new RuntimeException("Error while loading entity by name", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void removeById(Long id, Entity.EntityType type) {
    Entity entity = cacheData.getIfPresent(MetaCacheKey.of(id, type));
    if (entity == null) {
      return;
    }

    removeByMetadata(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void removeByName(NameIdentifier ident) {
    MetaCacheKey idKey = cacheIndex.getValueForExactKey(ident.toString());
    if (idKey == null) {
      return;
    }
    Entity entity = cacheData.getIfPresent(idKey);

    removeByMetadata(entity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsById(Long id, Entity.EntityType type) {
    return cacheData.asMap().containsKey(MetaCacheKey.of(id, type));
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsByName(NameIdentifier ident) {
    return cacheIndex.getValueForExactKey(ident.toString()) != null;
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheData() {
    return cacheData.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheIndex() {
    return cacheIndex.size();
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
  public void put(Entity entity) {
    syncMetadataToCache(entity);
  }

  /**
   * Synchronizes the Metadata to the index.
   *
   * @param metadata The Metadata to synchronize
   */
  private void syncMetadataToIndex(Entity metadata) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromMetadata(metadata);
    long id = CacheUtils.getIdFromMetadata(metadata);

    withLock(
        () -> {
          cacheIndex.put(nameIdent.toString(), MetaCacheKey.of(id, metadata.type()));
        });
  }

  /**
   * Synchronizes the metadata to the cache.
   *
   * @param metadata The metadata to synchronize
   */
  private void syncMetadataToCache(Entity metadata) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromMetadata(metadata);
    long id = CacheUtils.getIdFromMetadata(metadata);

    withLock(
        () -> {
          cacheData.put(MetaCacheKey.of(id, metadata.type()), metadata);
          cacheIndex.put(nameIdent.toString(), MetaCacheKey.of(id, metadata.type()));
        });
  }

  /**
   * Removes the expired metadata from the cache. This method is a hook method for the Cache, when
   * an entry expires, it will call this method.
   *
   * @param metadata The metadata to remove,
   */
  @Override
  protected void removeExpiredMetadataFromDataCache(Entity metadata) {
    NameIdentifier identifier = CacheUtils.getIdentFromMetadata(metadata);

    withLock(
        () -> {
          cacheIndex.remove(identifier.toString());
        });
  }

  /**
   * Removes the metadata from the cache. This method will also remove all metadata with the same
   * prefix.
   *
   * @param rootMetadata The root metadata to remove.
   */
  private void removeByMetadata(Entity rootMetadata) {
    NameIdentifier prefix = CacheUtils.getIdentFromMetadata(rootMetadata);

    withLock(
        () -> {
          // 1. find all keys starting with prefix
          List<MetaCacheKey> toRemovedId = Lists.newArrayList();
          List<String> toRemovedIdent = Lists.newArrayList();

          for (MetaCacheKey idKey : cacheIndex.getValuesForKeysStartingWith(prefix.toString())) {
            Entity entityToRemove = cacheData.getIfPresent(idKey);
            toRemovedId.add(idKey);

            if (entityToRemove != null) {
              toRemovedIdent.add(CacheUtils.getIdentFromMetadata(entityToRemove).toString());
            }
          }

          if (toRemovedId.isEmpty()) {
            return;
          }

          // 2, clean up id cache and name cache
          cacheData.invalidateAll(toRemovedId);
          // 3. remove from index tree
          toRemovedIdent.forEach(ident -> cacheIndex.remove(ident));
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

    builder.maximumSize(cacheConfig.getMaxSize());

    if (cacheConfig.isExpirationEnabled()) {
      builder.expireAfterWrite(
          cacheConfig.getExpirationTime(), cacheConfig.getExpirationTimeUnit());
    }

    if (cacheConfig.isCacheStatusEnabled()) {
      builder.recordStats();
    }

    return (Caffeine<KEY, VALUE>) builder;
  }

  /** Initializes the stats scheduler. */
  private void initStatsScheduler() {
    if (!cacheConfig.isCacheStatusEnabled()) {
      return;
    }

    statsScheduler.scheduleAtFixedRate(
        () -> {
          try {
            CacheStats idStats = cacheData.stats();
            LOG.info(
                "CacheStats [byId] hitRate={} missRate={} avgLoadTime={}ms",
                idStats.hitRate(),
                idStats.missRate(),
                idStats.averageLoadPenalty() / 1_000_000.0);
          } catch (Throwable t) {
            LOG.warn("Error while logging cache stats", t);
          }
        },
        1,
        1,
        TimeUnit.MINUTES);
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
}
