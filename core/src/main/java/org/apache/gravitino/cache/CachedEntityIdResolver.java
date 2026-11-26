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

import java.util.Optional;
import java.util.function.BiPredicate;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class CachedEntityIdResolver implements EntityIdResolver {

  private final EntityCache entityCache;
  private final EntityIdResolver underlyingResolver;
  private final BiPredicate<NameIdentifier, Entity.EntityType> allowCacheRead;

  public CachedEntityIdResolver(EntityCache entityCache, EntityIdResolver underlyingResolver) {
    this(entityCache, underlyingResolver, (ident, type) -> true);
  }

  /**
   * Creates a resolver that consults {@code allowCacheRead} before using a cache hit.
   *
   * @param entityCache The entity cache.
   * @param underlyingResolver Resolver used on a cache miss or when the predicate disallows a cache
   *     read.
   * @param allowCacheRead Whether a cache hit may be used for the given key.
   */
  public CachedEntityIdResolver(
      EntityCache entityCache,
      EntityIdResolver underlyingResolver,
      BiPredicate<NameIdentifier, Entity.EntityType> allowCacheRead) {
    this.entityCache = entityCache;
    this.underlyingResolver = underlyingResolver;
    this.allowCacheRead = allowCacheRead;
  }

  @Override
  public NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
    // All-or-nothing: never stitch a cached leaf id with parents from another resolution. Gate +
    // getIfPresent stay under the key lock (matching RelationalEntityStore#get); JDBC stays
    // outside.
    return tryCachedEntityIds(nameIdentifier, type)
        .orElseGet(() -> underlyingResolver.getEntityIds(nameIdentifier, type));
  }

  @Override
  public Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
    // Same all-or-nothing gate as getEntityIds: an ancestor pending/publishing blocks the leaf
    // cache hit so the two entry points stay consistent.
    return tryCachedEntityIds(nameIdentifier, type)
        .map(NamespacedEntityId::entityId)
        .orElseGet(() -> underlyingResolver.getEntityId(nameIdentifier, type));
  }

  /**
   * Returns a namespaced id only when every ancestor is also resolved from cache. Empty means the
   * caller must use the underlying resolver for the original identifier.
   */
  private Optional<NamespacedEntityId> tryCachedEntityIds(
      NameIdentifier nameIdentifier, Entity.EntityType type) {
    Optional<Long> cachedId =
        entityCache.withCacheLock(
            EntityCacheKey.of(nameIdentifier, type),
            () -> {
              if (!allowCacheRead.test(nameIdentifier, type)) {
                return Optional.empty();
              }
              return entityCache.getIfPresent(nameIdentifier, type).map(HasIdentifier::id);
            });
    if (cachedId.isEmpty()) {
      return Optional.empty();
    }
    if (!nameIdentifier.hasNamespace()) {
      return Optional.of(new NamespacedEntityId(cachedId.get()));
    }
    Optional<NamespacedEntityId> parents =
        tryCachedEntityIds(
            NameIdentifierUtil.parentNameIdentifier(nameIdentifier, type),
            NameIdentifierUtil.parentEntityType(type));
    return parents.map(p -> new NamespacedEntityId(cachedId.get(), p.fullIds()));
  }
}
