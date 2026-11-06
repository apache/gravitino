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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class CachedEntityIdResolver implements EntityIdResolver {

  private final EntityCache entityCache;
  private final EntityIdResolver underlyingResolver;

  public CachedEntityIdResolver(EntityCache entityCache, EntityIdResolver underlyingResolver) {
    this.entityCache = entityCache;
    this.underlyingResolver = underlyingResolver;
  }

  /**
   * Resolves the ids of an identifier and its ancestors, looking all of them up in the cache at
   * once. Levels are taken from the cache from the identifier upwards until the first one that is
   * not cached; that level and everything above it is resolved by the underlying resolver.
   */
  @Override
  public NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
    if (!BaseEntityCache.isCacheable(type)) {
      return underlyingResolver.getEntityIds(nameIdentifier, type);
    }

    // The identifier first, then each ancestor up to the metalake.
    List<EntityCacheKey> chain = new ArrayList<>();
    NameIdentifier ident = nameIdentifier;
    Entity.EntityType levelType = type;
    chain.add(EntityCacheKey.of(ident, levelType));
    while (ident.hasNamespace()) {
      ident = NameIdentifierUtil.parentNameIdentifier(ident, levelType);
      levelType = NameIdentifierUtil.parentEntityType(levelType);
      chain.add(EntityCacheKey.of(ident, levelType));
    }

    List<Optional<HasIdentifier>> cached = entityCache.peekAll(chain);
    int firstMiss = 0;
    while (firstMiss < chain.size() && cached.get(firstMiss).isPresent()) {
      firstMiss++;
    }

    NamespacedEntityId ids;
    int level;
    if (firstMiss == chain.size()) {
      level = chain.size() - 1;
      ids = new NamespacedEntityId(cached.get(level).get().id());
    } else {
      EntityCacheKey missed = chain.get(firstMiss);
      ids = underlyingResolver.getEntityIds(missed.identifier(), missed.entityType());
      level = firstMiss;
    }
    for (int i = level - 1; i >= 0; i--) {
      ids = new NamespacedEntityId(cached.get(i).get().id(), ids.fullIds());
    }
    return ids;
  }

  @Override
  public Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
    return entityCache
        .peekAll(ImmutableList.of(EntityCacheKey.of(nameIdentifier, type)))
        .get(0)
        .map(HasIdentifier::id)
        .orElseGet(() -> underlyingResolver.getEntityId(nameIdentifier, type));
  }
}
