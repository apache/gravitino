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
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.glassfish.jersey.internal.guava.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaCacheCaffeine extends BaseMetaCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetaCacheCaffeine.class.getName());
  private final Cache<Entity, CacheKey> cache1;
  private final Cache<CacheKey, Entity> cache2;
  private final Map<Pair<Long, Entity.EntityType>, CacheKey> map1;
  private final Map<NameIdentifier, CacheKey> map2;
  private final Trie<String, NameIdentifier> indexTree;

  public MetaCacheCaffeine() {
    this(new CacheConfig());
  }

  public MetaCacheCaffeine(CacheConfig cacheConfig) {
    this(cacheConfig, null);
  }

  public MetaCacheCaffeine(CacheConfig cacheConfig, EntityStore entityStore) {
    super(entityStore);
    map1 = new ConcurrentHashMap<>();
    map2 = new ConcurrentHashMap<>();
    indexTree = new PatriciaTrie<>();

    Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder();

    if (cacheConfig.isWeightedCacheEnabled()) {
      caffeineBuilder.weigher(MetadataEntityWeigher.getInstance());
      caffeineBuilder.maximumWeight(cacheConfig.getEntityCacheWeigherTarget());
    } else {
      caffeineBuilder.maximumSize(cacheConfig.getMaxSize());
    }

    if (cacheConfig.isExpirationEnabled()) {
      caffeineBuilder.expireAfterWrite(
          cacheConfig.getExpirationTime(), cacheConfig.getExpirationTimeUnit());
    }

    this.cache1 = caffeineBuilder.build();
    this.cache2 = caffeineBuilder.build();
  }

  @Override
  public Entity getOrLoadMetadataById(Long id, Entity.EntityType type) {
    Pair<Long, Entity.EntityType> pair = Pair.of(id, type);

    if (map1.containsKey(pair)) {
      return cache2.getIfPresent(map1.get(pair));
    } else {
      Entity entity = loadEntityFromDBById(id, type);
      addCacheKey(entity);

      return entity;
    }
  }

  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    if (map2.containsKey(ident)) {
      return cache2.getIfPresent(map2.get(ident));
    } else {
      try {
        Entity entity = loadEntityFromDBByName(ident, type);
        addCacheKey(entity);

        return entity;
      } catch (IOException e) {
        LOG.error("Failed to load metadata by name: " + ident, e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void removeById(Long id, Entity.EntityType type) {
    CacheKey cacheKey = map1.get(Pair.of(id, type));
    if (cacheKey != null) {
      removeRelatedCache(cacheKey);
      removeRelatedIndex(cacheKey);
    }
  }

  @Override
  public void removeByName(NameIdentifier ident) {
    CacheKey cacheKey = map2.get(ident);
    if (cacheKey != null) {
      removeRelatedCache(cacheKey);
      removeRelatedIndex(cacheKey);
    }
  }

  @Override
  public boolean containsById(Long id, Entity.EntityType type) {
    return map1.containsKey(Pair.of(id, type));
  }

  @Override
  public boolean containsByName(NameIdentifier ident) {
    return map2.containsKey(ident);
  }

  @Override
  public long sizeOfCacheData() {
    return cache1.estimatedSize();
  }

  @Override
  public long sizeOfCacheIndex() {
    return indexTree.size();
  }

  private Entity loadEntityFromDBById(Long id, Entity.EntityType type) {
    Pair.of(id, type);
    // TODO: Implement this method
    return null;
  }

  private Entity loadEntityFromDBByName(NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    return entityStore.get(ident, type, CacheUtils.getEntityClass(type));
  }

  private synchronized void addCacheKey(Entity entity) {
    CacheKey cacheKey = new CacheKey(entity);
    cache1.put(entity, cacheKey);
    cache2.put(cacheKey, entity);

    map1.put(Pair.of(cacheKey.id(), cacheKey.entityType()), cacheKey);
    map2.put(cacheKey.nameIdent(), cacheKey);

    addIndex(cacheKey);
  }

  private void addIndex(CacheKey cacheKey) {
    NameIdentifier nameIdent = cacheKey.nameIdent();
    indexTree.put(nameIdent.toString(), nameIdent);
  }

  private synchronized void removeRelatedCache(CacheKey cacheKey) {
    String nameIdentStr = cacheKey.nameIdent().toString();

    SortedMap<String, NameIdentifier> prefixMap = indexTree.prefixMap(nameIdentStr);

    Set<CacheKey> removedKeys = Sets.newHashSet();
    Set<Entity> removedEntities = Sets.newHashSet();

    // 遍历前缀树
    for (NameIdentifier ident : prefixMap.values()) {
      // 1. 添加所有要删除的缓存
      CacheKey childCacheKey = map2.get(ident);
      Entity childEntity = cache2.getIfPresent(childCacheKey);

      removedKeys.add(childCacheKey);
      removedEntities.add(childEntity);
    }

    // 2. 删除缓存
    cache1.invalidateAll(removedEntities);
    cache2.invalidateAll(removedKeys);
  }

  private synchronized void removeRelatedIndex(CacheKey cacheKey) {
    String nameIdentStr = cacheKey.nameIdent().toString();

    SortedMap<String, NameIdentifier> prefixMap = indexTree.prefixMap(nameIdentStr);

    Set<String> removedIndexKeys = Sets.newHashSet();
    Set<Pair<Long, Entity.EntityType>> removedIdKeys = Sets.newHashSet();
    Set<NameIdentifier> removedNameKeys = Sets.newHashSet();

    for (NameIdentifier ident : prefixMap.values()) {
      // 1. 添加前缀树索引
      removedIndexKeys.add(ident.toString());
      // 2. 添加 ID 索引
      CacheKey childCacheKey = map2.get(ident);
      removedIdKeys.add(Pair.of(childCacheKey.id(), childCacheKey.entityType()));

      // 3. 添加 NameIdentifier 索引
      removedNameKeys.add(ident);
    }

    // 删除索引
    map1.keySet().removeAll(removedIdKeys);
    map2.keySet().removeAll(removedNameKeys);
    for (String removedIndexKey : removedIndexKeys) {
      indexTree.remove(removedIndexKey);
    }
  }
}
