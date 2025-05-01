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
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
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
  private final Cache<NameIdentifier, Entity> byName;

  private final Cache<Pair<Long, Entity.EntityType>, Entity> byId;
  /** Index part */
  private RadixTree<NameIdentifier> indexTree;

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
    indexTree = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
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

    Caffeine<NameIdentifier, Entity> nameBuilder = newBaseBuilder(cacheConfig);
    Caffeine<Pair<Long, Entity.EntityType>, Entity> idBuilder = newBaseBuilder(cacheConfig);

    nameBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }

              try {
                LOG.debug("Expired [byName] key={}, scheduling removal", key);
                removeExpiredEntityFromNameCache(value);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    idBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                LOG.debug("Expired [byId] key={}, scheduling removal", key);
                removeExpiredEntityFromIdCache(value);
              } catch (Throwable t) {
                LOG.error("Async removal failed for {}", value, t);
              }
            });

    this.byName = nameBuilder.build();
    this.byId = idBuilder.build();
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
    Pair<Long, Entity.EntityType> pair = Pair.of(id, type);

    if (LOG.isDebugEnabled()) {
      LOG.debug("getById: checking cache for key={} type={}", id, type);
    }

    return byId.get(
        pair,
        key -> {
          LOG.info("Cache miss [byId] for id={}, type={}, loading from DB", id, type);
          Entity entity = loadEntityFromDBById(id, type);
          syncFromIdCache(entity);

          return entity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getByName: checking cache for key={} type={}", ident, type);
    }
    return byName.get(
        ident,
        key -> {
          LOG.info("Cache miss [byName] for name={}, type={}, loading from DB", ident, type);
          try {
            Entity entity = loadEntityFromDBByName(key, type);
            syncFromNameCache(entity);

            return entity;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public void removeById(Long id, Entity.EntityType type) {
    Entity entity = byId.getIfPresent(Pair.of(id, type));
    if (entity == null) {
      throw new IllegalArgumentException(
          "Entity with id " + id + " and type " + type + " not found in cache");
    }

    removeFromEntity(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void removeByName(NameIdentifier ident) {
    Entity entity = byName.getIfPresent(ident);

    if (entity == null) {
      throw new IllegalArgumentException("Entity with name " + ident + " not found in cache");
    }

    removeFromEntity(entity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsById(Long id, Entity.EntityType type) {
    return withLock(() -> byId.asMap().containsKey(Pair.of(id, type)));
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsByName(NameIdentifier ident) {
    return withLock(() -> byName.asMap().containsKey(ident));
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheData() {
    return withLock(byId::estimatedSize);
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheIndex() {
    return withLock(() -> indexTree.size());
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    withLock(
        () -> {
          LOG.info("Clearing entire cache and rebuilding indexTree");
          byId.invalidateAll();
          byName.invalidateAll();
          indexTree = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  private void syncFromNameCache(Entity entity) {
    long id = CacheUtils.getIdFromEntity(entity);
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);
    withLock(
        () -> {
          LOG.trace(
              "SyncFromNameCache: putting name={} for entity id={}",
              nameIdent,
              CacheUtils.getIdFromEntity(entity));

          byId.put(Pair.of(id, entity.type()), entity);
          indexTree.put(nameIdent.toString(), nameIdent);
        });
  }

  private void syncFromIdCache(Entity entity) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);

    withLock(
        () -> {
          LOG.trace(
              "SyncFromIdCache: putting name={} for entity id={}",
              nameIdent,
              CacheUtils.getIdFromEntity(entity));

          byName.put(nameIdent, entity);
          indexTree.put(nameIdent.toString(), nameIdent);
        });
  }

  private void removeExpiredEntityFromNameCache(Entity entity) {
    long id = CacheUtils.getIdFromEntity(entity);
    NameIdentifier identifier = CacheUtils.getIdentFromEntity(entity);

    withLock(
        () -> {
          LOG.trace(
              "Expired removal [byName]: removing id={} name={}",
              CacheUtils.getIdFromEntity(entity),
              identifier);

          byId.invalidate(Pair.of(id, entity.type()));
          indexTree.remove(identifier.toString());
        });
  }

  private void removeExpiredEntityFromIdCache(Entity entity) {
    NameIdentifier identifier = CacheUtils.getIdentFromEntity(entity);

    withLock(
        () -> {
          LOG.trace(
              "Expired removal [byId]: removing id={} name={}",
              CacheUtils.getIdFromEntity(entity),
              identifier);

          byName.invalidate(identifier);
          indexTree.remove(identifier.toString());
        });
  }

  private void removeFromEntity(Entity entity) {
    NameIdentifier prefix = CacheUtils.getIdentFromEntity(entity);

    withLock(
        () -> {
          LOG.info("Manual remove: prefix={}, removing matching entries", prefix);
          // 1. find all keys starting with prefix
          List<NameIdentifier> toRemovedIdent = Lists.newArrayList();
          for (NameIdentifier ident : indexTree.getValuesForKeysStartingWith(prefix.toString())) {
            toRemovedIdent.add(ident);
            indexTree.remove(ident.toString());
          }

          if (!toRemovedIdent.isEmpty()) {
            // 2. find which keys to remove from id cache
            List<Pair<Long, Entity.EntityType>> removedKeys = Lists.newArrayList();
            for (NameIdentifier ident : toRemovedIdent) {
              Entity cached = byName.getIfPresent(ident);
              if (cached != null) {
                removedKeys.add(Pair.of(CacheUtils.getIdFromEntity(cached), cached.type()));
              }
            }
            // 3, clean up id cache and name cache
            byName.invalidateAll(toRemovedIdent);
            byId.invalidateAll(removedKeys);
          }
        });
  }

  private <KEY, VALUE> Caffeine<KEY, VALUE> newBaseBuilder(CacheConfig cacheConfig) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();

    if (cacheConfig.isWeightedCacheEnabled()) {
      builder
          .weigher(MetadataEntityWeigher.getInstance())
          .maximumWeight(cacheConfig.getEntityCacheWeigherTarget());
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

  private void initStatsScheduler() {
    if (!cacheConfig.isCacheStatusEnabled()) {
      return;
    }

    statsScheduler.scheduleAtFixedRate(
        () -> {
          try {
            CacheStats idStats = byId.stats();
            CacheStats nameStats = byName.stats();
            LOG.info(
                "CacheStats [byId] hitRate={} missRate={} avgLoadTime={}ms  |  [byName] hitRate={} missRate={} avgLoadTime={}ms",
                idStats.hitRate(),
                idStats.missRate(),
                idStats.averageLoadPenalty() / 1_000_000.0,
                nameStats.hitRate(),
                nameStats.missRate(),
                nameStats.averageLoadPenalty() / 1_000_000.0);
          } catch (Throwable t) {
            LOG.warn("Error while logging cache stats", t);
          }
        },
        1,
        1,
        TimeUnit.MINUTES);
  }

  private <T> T withLock(Supplier<T> fn) {
    opLock.lock();
    try {
      return fn.get();
    } finally {
      opLock.unlock();
    }
  }

  private void withLock(Runnable action) {
    opLock.lock();
    try {
      action.run();
    } finally {
      opLock.unlock();
    }
  }
}
