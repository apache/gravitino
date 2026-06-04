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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Two-tier metadata-id and owner resolution for {@link JcasbinAuthorizer}.
 *
 * <p>Each lookup is deduplicated within a single request via {@link AuthorizationRequestContext},
 * falls back to a shared {@link GravitinoCache} on a request miss, and finally issues a single DB
 * query on a cache miss. A successful DB fetch populates both tiers so subsequent calls — in this
 * request and later ones — hit the cache. The two underlying caches are invalidated externally by
 * the global entity change log poller, {@link JcasbinChangeListener} (owner changes), and by the
 * {@link org.apache.gravitino.authorization.GravitinoAuthorizer#handleMetadataOwnerChange} / {@link
 * org.apache.gravitino.authorization.GravitinoAuthorizer#handleEntityNameIdMappingChange} hooks
 * (local mutations).
 */
public class JcasbinAuthorizationLookups {

  private final GravitinoCache<String, Long> metadataIdCache;
  private final GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;

  /**
   * Creates a new lookups facade around the supplied caches. The caches are owned by the caller and
   * remain accessible for invalidation by other components (poller, change hooks).
   *
   * @param metadataIdCache path-based metadata object key → entity id
   * @param ownerRelCache {@code metadataObjectId} → {@link Optional} of {@link OwnerInfo}
   */
  public JcasbinAuthorizationLookups(
      GravitinoCache<String, Long> metadataIdCache,
      GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache) {
    this.metadataIdCache = metadataIdCache;
    this.ownerRelCache = ownerRelCache;
  }

  /**
   * Two-tier name→id lookup: the per-request map in {@code requestContext} dedups calls within the
   * same HTTP request; on a miss, the long-lived {@code metadataIdCache} is consulted, and finally
   * we fall back to a DB query via {@link MetadataIdConverter#getID}. Returns {@link
   * Optional#empty()} when the metadata object does not exist so callers can deny authorization.
   * Missing metadata objects are never cached as negative results: a later create for the same name
   * can be observed without waiting for cache eviction. Existing objects are invalidated by local
   * name-id mapping hooks and by the change-log poller on peer nodes.
   */
  public Optional<Long> resolveMetadataId(
      MetadataObject metadataObject, String metalake, AuthorizationRequestContext requestContext) {
    String cacheKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, metadataObject);
    try {
      // Both cache tiers load atomically and forbid caching null, so a missing object is signalled
      // by throwing through the loaders and translated back to Optional.empty() here. This caches
      // only positive results, never a negative one.
      return Optional.of(
          requestContext.computeMetadataIdIfAbsent(
              cacheKey,
              k -> metadataIdCache.get(k, ignored -> loadMetadataId(metadataObject, metalake))));
    } catch (NoSuchMetadataObjectException e) {
      return Optional.empty();
    }
  }

  private static Long loadMetadataId(MetadataObject metadataObject, String metalake) {
    return MetadataIdConverter.getID(metadataObject, metalake)
        .orElseThrow(
            () ->
                new NoSuchMetadataObjectException(
                    "Metadata object %s does not exist", metadataObject.fullName()));
  }

  /**
   * Two-tier owner lookup: request-level dedup first, then the shared {@code ownerRelCache}, and
   * finally a single {@code owner_meta} query. Both positive and negative DB results populate both
   * tiers so subsequent calls — within this request and from later requests — avoid a repeat query.
   */
  public Optional<OwnerInfo> resolveOwnerId(
      Long metadataId,
      MetadataObject.Type metadataType,
      AuthorizationRequestContext requestContext) {
    return requestContext.computeOwnerIfAbsent(
        metadataId,
        // Use the cache's atomic loader so concurrent misses on the same id collapse to one DB
        // query. Both present and absent results are cached so later requests skip the DB entirely.
        id -> ownerRelCache.get(id, k -> loadOwner(k, metadataType)));
  }

  private static Optional<OwnerInfo> loadOwner(Long id, MetadataObject.Type metadataType) {
    OwnerInfo ownerInfo =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            m -> m.selectOwnerByMetadataObjectIdAndType(id, metadataType.name()));
    return Optional.ofNullable(ownerInfo);
  }
}
