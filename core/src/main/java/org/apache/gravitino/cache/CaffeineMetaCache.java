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
import java.util.Objects;
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
  private final Cache<EntityKey, Entity> cacheData;
  /** Index part */
  private RadixTree<EntityKey> cacheIndex;

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

    Caffeine<EntityKey, Entity> cacheDataBuilder = newBaseBuilder(cacheConfig);

    cacheDataBuilder
        .executor(cleanupExec)
        .removalListener(
            (key, value, cause) -> {
              if (cause != RemovalCause.EXPIRED) {
                return;
              }
              try {
                LOG.debug("Expired [byId] key={}, scheduling removal", key);
                removeExpiredEntityFromDataCache(value);
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

    EntityKey entityKey = EntityKey.of(id, type);
    LOG.debug("getById: checking cache for key={} type={}", id, type);

    return cacheData.get(
        entityKey,
        key -> {
          LOG.debug("Cache miss [byId] for id={}, type={}, loading from DB", id, type);
          // ???: Should we check if the entity is null?
          Entity entity = loadEntityFromDBById(id, type);
          syncEntityToIndex(entity);

          return entity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    if (LOG.isDebugEnabled()) {
      LOG.debug("getByName: checking cache for key={} type={}", ident, type);
    }

    EntityKey idKey = cacheIndex.getValueForExactKey(ident.toString());
    if (idKey != null) {
      return cacheData.getIfPresent(idKey);
    }
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cache miss [byName] for name={}, type={}, loading from DB", ident, type);
      }
      // ???: Should we check if the entity is null?
      Entity entity = loadEntityFromDBByName(ident, type);
      syncEntityToCache(entity);

      return entity;
    } catch (IOException e) {
      LOG.error("Error while loading entity by name", e);
      throw new RuntimeException("Error while loading entity by name", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void removeById(Long id, Entity.EntityType type) {
    Entity entity = cacheData.getIfPresent(EntityKey.of(id, type));
    if (entity == null) {
      throw new IllegalArgumentException(
          "Entity with id " + id + " and type " + type + " not found in cache");
    }

    removeFromEntity(entity);
  }

  /** {@inheritDoc} */
  @Override
  public void removeByName(NameIdentifier ident) {
    EntityKey idKey = cacheIndex.getValueForExactKey(ident.toString());
    if (idKey == null) {
      throw new IllegalArgumentException("Entity with name " + ident + " not found in cache");
    }
    Entity entity = cacheData.getIfPresent(idKey);

    removeFromEntity(entity);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsById(Long id, Entity.EntityType type) {
    return cacheData.asMap().containsKey(EntityKey.of(id, type));
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
          LOG.info("Clearing entire cache and rebuilding indexTree");
          cacheData.invalidateAll();
          cacheIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());
        });
  }

  private void syncEntityToIndex(Entity entity) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);
    long id = CacheUtils.getIdFromEntity(entity);

    withLock(
        () -> {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SyncFromIdCache: putting name={} for entity id={}", nameIdent, id);
          }

          cacheIndex.put(nameIdent.toString(), EntityKey.of(id, entity.type()));
        });
  }

  private void syncEntityToCache(Entity entity) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);
    long id = CacheUtils.getIdFromEntity(entity);

    withLock(
        () -> {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SyncFromIndexCache: putting id={} for entity name={}", id, nameIdent);
          }

          cacheData.put(EntityKey.of(id, entity.type()), entity);
          cacheIndex.put(nameIdent.toString(), EntityKey.of(id, entity.type()));
        });
  }

  private void removeExpiredEntityFromDataCache(Entity entity) {
    NameIdentifier identifier = CacheUtils.getIdentFromEntity(entity);

    withLock(
        () -> {
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "Expired removal [byId]: removing id={} name={}",
                CacheUtils.getIdFromEntity(entity),
                identifier);
          }

          cacheIndex.remove(identifier.toString());
        });
  }

  private void removeFromEntity(Entity rootEntity) {
    NameIdentifier prefix = CacheUtils.getIdentFromEntity(rootEntity);

    withLock(
        () -> {
          LOG.debug("Manual remove: prefix={}, removing matching entries", prefix);
          // 1. find all keys starting with prefix
          List<EntityKey> toRemovedId = Lists.newArrayList();
          List<String> toRemovedIdent = Lists.newArrayList();

          for (EntityKey idKey : cacheIndex.getValuesForKeysStartingWith(prefix.toString())) {
            Entity entityToRemove = cacheData.getIfPresent(idKey);
            toRemovedId.add(idKey);

            if (entityToRemove != null) {
              toRemovedIdent.add(CacheUtils.getIdentFromEntity(entityToRemove).toString());
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

  private void withLock(Runnable action) {
    opLock.lock();
    try {
      action.run();
    } finally {
      opLock.unlock();
    }
  }

  /**
   * Represents a key for the cache.
   *
   * <p>The key consists of the entity id and type.
   */
  static final class EntityKey {
    private final Long id;
    private final Entity.EntityType type;

    /**
     * Creates a new instance of {@link EntityKey}.
     *
     * @param id The entity id
     * @param type The entity type
     * @return The new instance
     */
    public static EntityKey of(long id, Entity.EntityType type) {
      return new EntityKey(id, type);
    }

    /**
     * Creates a new instance of {@link EntityKey}.
     *
     * @param id The entity id
     * @param type The entity type
     */
    public EntityKey(Long id, Entity.EntityType type) {
      Preconditions.checkArgument(id != null, "Id cannot be null");
      Preconditions.checkArgument(type != null, "EntityType cannot be null");

      this.id = id;
      this.type = type;
    }

    /**
     * Returns the entity id.
     *
     * @return The entity id
     */
    public Long id() {
      return id;
    }

    /**
     * Returns the entity type.
     *
     * @return The entity type
     */
    public Entity.EntityType type() {
      return type;
    }

    /**
     * Compares two instances of {@link EntityKey}. The comparison is based on the entity id and
     * type.
     *
     * @param obj The object to compare to
     * @return {@code true} if the two instances are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof EntityKey)) return false;
      EntityKey other = (EntityKey) obj;
      return id.equals(other.id) && type.equals(other.type);
    }

    /**
     * Returns the hash code of this instance. The hash code is based on the entity id and type.
     *
     * @return The hash code of this instance
     */
    @Override
    public int hashCode() {
      return 31 * Objects.hashCode(id) + Objects.hashCode(type);
    }
  }
}
