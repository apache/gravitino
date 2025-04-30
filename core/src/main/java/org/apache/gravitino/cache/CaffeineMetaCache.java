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
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;

/** This class implements a meta cache using Caffeine cache. */
public class CaffeineMetaCache extends BaseMetaCache {
  /** Singleton instance */
  private static volatile CaffeineMetaCache INSTANCE;
  /** Cache part */
  private final Cache<NameIdentifier, Entity> byName;

  private final Cache<Pair<Long, Entity.EntityType>, Entity> byId;
  /** Index part */
  private final RadixTree<NameIdentifier> indexTree;

  private final ReentrantLock opLock = new ReentrantLock();

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

    Caffeine<NameIdentifier, Entity> nameBuilder = newBaseBuilder(cacheConfig);
    Caffeine<Pair<Long, Entity.EntityType>, Entity> idBuilder = newBaseBuilder(cacheConfig);

    nameBuilder.removalListener(
        (key, value, cause) -> {
          if (cause == RemovalCause.EXPIRED) {
            removeExpiredEntityFromNameCache(value);
          }
        });

    idBuilder.removalListener(
        (key, value, cause) -> {
          if (cause == RemovalCause.EXPIRED) {
            removeExpiredEntityFromIdCache(value);
          }
        });

    this.byName = nameBuilder.build();
    this.byId = idBuilder.build();
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataById(Long id, Entity.EntityType type) {
    Pair<Long, Entity.EntityType> pair = Pair.of(id, type);
    return byId.get(
        pair,
        key -> {
          Entity entity = loadEntityFromDBById(id, type);
          syncFromIdCache(entity);

          return entity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    return byName.get(
        ident,
        key -> {
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
  public synchronized boolean containsById(Long id, Entity.EntityType type) {
    return byId.asMap().containsKey(Pair.of(id, type));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsByName(NameIdentifier ident) {
    return byName.asMap().containsKey(ident);
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheData() {
    return byId.estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheIndex() {
    return indexTree.size();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void clear() {
    opLock.lock();
    try {
      byId.invalidateAll();
      byName.invalidateAll();
      Iterable<CharSequence> allKeys = indexTree.getKeysStartingWith("");
      allKeys.forEach(indexTree::remove);
    } finally {
      opLock.unlock();
    }
  }

  private void syncFromNameCache(Entity entity) {
    long id = CacheUtils.getIdFromEntity(entity);
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);
    opLock.lock();
    try {
      byId.put(Pair.of(id, entity.type()), entity);
      indexTree.put(nameIdent.toString(), nameIdent);
    } finally {
      opLock.unlock();
    }
  }

  private void syncFromIdCache(Entity entity) {
    NameIdentifier nameIdent = CacheUtils.getIdentFromEntity(entity);

    opLock.lock();
    try {
      byName.put(nameIdent, entity);
      indexTree.put(nameIdent.toString(), nameIdent);
    } finally {
      opLock.unlock();
    }
  }

  private void removeExpiredEntityFromNameCache(Entity entity) {
    long id = CacheUtils.getIdFromEntity(entity);
    NameIdentifier identifier = CacheUtils.getIdentFromEntity(entity);

    opLock.lock();
    try {
      byId.invalidate(Pair.of(id, entity.type()));
      indexTree.remove(identifier.toString());
    } finally {
      opLock.unlock();
    }
  }

  private void removeExpiredEntityFromIdCache(Entity entity) {
    NameIdentifier identifier = CacheUtils.getIdentFromEntity(entity);
    opLock.lock();
    try {
      byName.invalidate(identifier);
      indexTree.remove(identifier.toString());
    } finally {
      opLock.unlock();
    }
  }

  private void removeFromEntity(Entity entity) {
    NameIdentifier prefix = CacheUtils.getIdentFromEntity(entity);

    opLock.lock();
    try {
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
    } finally {
      opLock.unlock();
    }
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
}
