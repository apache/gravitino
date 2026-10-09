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
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinAuthorizationLookups.class);

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
   * we fall back to a DB query via {@link MetadataIdConverter#getIdForNormalizedObject}. Returns
   * {@link Optional#empty()} when the metadata object does not exist or normalization fails so
   * callers can deny authorization. Missing metadata objects are never cached as negative results:
   * a later create for the same name can be observed without waiting for cache eviction. Existing
   * objects are invalidated by local name-id mapping hooks and by the change-log poller on peer
   * nodes. Both cache tiers use names normalized by catalog capability. Normalization is
   * deduplicated per raw name within a request; a fresh request still resolves current catalog
   * rules before consulting the shared cache.
   */
  public Optional<Long> resolveMetadataId(
      MetadataObject metadataObject, String metalake, AuthorizationRequestContext requestContext) {
    return resolveMetadataIdResult(metadataObject, metalake, requestContext).metadataId();
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

  // Preserve normalization failures as distinct from missing objects for DENY-policy consumers.
  MetadataIdResolution resolveMetadataIdResult(
      MetadataObject metadataObject, String metalake, AuthorizationRequestContext requestContext) {
    MetadataObject cacheObject;
    try {
      // Use the same capability rules as ID resolution so hooks and peer change-log replay
      // evict every equivalent spelling from both cache tiers.
      cacheObject =
          requestContext.computeNormalizedMetadataObjectIfAbsent(
              JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, metadataObject),
              ignored -> MetadataIdConverter.normalizeMetadataObject(metadataObject, metalake));
    } catch (NotFoundException e) {
      return new MetadataIdResolution(Optional.empty(), false);
    } catch (RuntimeException e) {
      // Never fall back to the raw key: it can retain an ID after canonical-name invalidation.
      // Catch only normalization failures; entity-store and cache-loader failures still propagate.
      // Failures are not memoized, so a transient one can succeed on retry in the same request.
      // The cost: while a catalog cannot initialize, every object of that catalog looked up in a
      // request reloads it (connector initialization included) and logs this warning again. A
      // per-request, per-catalog failure memo or a once-per-request warning would bound that.
      LOG.warn(
          "Cannot normalize metadata object {}:{} in metalake {}; authorization lookup is unresolved",
          metadataObject.type(),
          metadataObject.fullName(),
          metalake,
          e);
      return new MetadataIdResolution(Optional.empty(), true);
    }
    String cacheKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, cacheObject);
    try {
      // Both cache tiers load atomically and forbid caching null, so a missing object is signalled
      // by throwing through the loaders and translated back to Optional.empty() here. This caches
      // only positive results, never a negative one. Load the same canonical name as the key,
      // rather than applying the request spelling again in the loader.
      return new MetadataIdResolution(
          Optional.of(
              requestContext.computeMetadataIdIfAbsent(
                  cacheKey,
                  k -> metadataIdCache.get(k, ignored -> loadMetadataId(cacheObject, metalake)))),
          false);
    } catch (NotFoundException e) {
      return new MetadataIdResolution(Optional.empty(), false);
    }
  }

  private static Long loadMetadataId(MetadataObject metadataObject, String metalake) {
    return MetadataIdConverter.getIdForNormalizedObject(metadataObject, metalake)
        .orElseThrow(
            () ->
                new NoSuchMetadataObjectException(
                    "Metadata object %s does not exist", metadataObject.fullName()));
  }

  private static Optional<OwnerInfo> loadOwner(Long id, MetadataObject.Type metadataType) {
    OwnerInfo ownerInfo =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            m -> m.selectOwnerByMetadataObjectIdAndType(id, metadataType.name()));
    return Optional.ofNullable(ownerInfo);
  }

  /** An ID lookup outcome, retaining capability failures for conservative DENY evaluation. */
  static final class MetadataIdResolution {
    private final Optional<Long> metadataId;
    private final boolean normalizationFailed;

    MetadataIdResolution(Optional<Long> metadataId, boolean normalizationFailed) {
      this.metadataId = metadataId;
      this.normalizationFailed = normalizationFailed;
    }

    /** Returns the metadata ID, or empty if the object is missing or could not be normalized. */
    Optional<Long> metadataId() {
      return metadataId;
    }

    /** Returns whether the catalog rules needed to normalize the object could not be resolved. */
    boolean normalizationFailed() {
      return normalizationFailed;
    }
  }
}
