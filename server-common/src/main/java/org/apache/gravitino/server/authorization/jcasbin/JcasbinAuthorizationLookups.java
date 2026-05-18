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

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Two-tier metadata-id and owner resolution for {@link JcasbinAuthorizer}.
 *
 * <p>Each lookup is deduplicated within a single request via {@link AuthorizationRequestContext},
 * falls back to a shared {@link GravitinoCache} on a request miss, and finally issues a single DB
 * query on a cache miss. A successful DB fetch populates both tiers so subsequent calls — in this
 * request and later ones — hit the cache. The two underlying caches are invalidated externally by
 * {@link JcasbinChangePoller} (HA peers) and by the {@link
 * org.apache.gravitino.authorization.GravitinoAuthorizer#handleMetadataOwnerChange} / {@link
 * org.apache.gravitino.authorization.GravitinoAuthorizer#handleEntityNameIdMappingChange} hooks
 * (local mutations).
 */
public class JcasbinAuthorizationLookups {

  /** Key separator for internal path-based cache keys. */
  static final String KEY_SEP = HierarchicalSchemaUtil.internalSeparator();

  private final GravitinoCache<String, Long> metadataIdCache;
  private final GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;

  /**
   * Creates a new lookups facade around the supplied caches. The caches are owned by the caller and
   * remain accessible for invalidation by other components (poller, change hooks).
   *
   * @param metadataIdCache path-based {@code metalake::catalog::schema::object::TYPE} → entity id
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
   * we fall back to a DB query via {@link MetadataIdConverter#getID}.
   */
  public Long resolveMetadataId(
      MetadataObject metadataObject, String metalake, AuthorizationRequestContext requestContext) {
    String cacheKey = buildCacheKey(metalake, metadataObject);
    return requestContext.computeMetadataIdIfAbsent(
        cacheKey,
        k ->
            metadataIdCache.get(k, ignored -> MetadataIdConverter.getID(metadataObject, metalake)));
  }

  /**
   * Two-tier owner lookup: request-level dedup first, then the shared {@code ownerRelCache}, and
   * finally a single {@code owner_meta} query. A successful DB fetch populates both tiers so
   * subsequent {@code isOwner} calls — in this request and later ones — hit the cache.
   */
  public Optional<OwnerInfo> resolveOwnerId(
      Long metadataId,
      MetadataObject.Type metadataType,
      AuthorizationRequestContext requestContext) {
    return requestContext.computeOwnerIfAbsent(
        metadataId,
        id ->
            ownerRelCache.get(
                id,
                ignored -> {
                  OwnerInfo ownerInfo =
                      SessionUtils.getWithoutCommit(
                          OwnerMetaMapper.class,
                          m -> m.selectOwnerByMetadataObjectIdAndType(id, metadataType.name()));
                  return ownerInfo == null ? Optional.empty() : Optional.of(ownerInfo);
                }));
  }

  /** Underlying metadata-id cache; exposed for invalidation by the change hooks and the poller. */
  public GravitinoCache<String, Long> metadataIdCache() {
    return metadataIdCache;
  }

  /** Underlying owner cache; exposed for invalidation by the change hooks and the poller. */
  public GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache() {
    return ownerRelCache;
  }

  /**
   * Builds a path-based cache key for the metadataIdCache. Container objects end with the internal
   * separator so a prefix invalidation can remove the container and all entries under the same name
   * path.
   *
   * <p>Examples: {@code metalake<sep>}, {@code metalake<sep>catalog<sep>}, {@code
   * metalake<sep>catalog<sep>schema<sep>}, {@code
   * metalake<sep>catalog<sep>schema<sep>table<sep>TABLE}.
   */
  @VisibleForTesting
  public static String buildCacheKey(String metalake, MetadataObject metadataObject) {
    if (metadataObject.type() == MetadataObject.Type.METALAKE) {
      return metalake + KEY_SEP;
    }
    StringBuilder sb = new StringBuilder(metalake);
    sb.append(KEY_SEP);
    // fullName uses '.' as separator, e.g. "catalog1.schema1.table1"
    String[] parts = metadataObject.fullName().split("\\.");
    sb.append(String.join(KEY_SEP, parts));
    if (isContainerType(metadataObject.type())) {
      // Trailing separator enables prefix-based invalidation.
      sb.append(KEY_SEP);
    } else {
      // Leaf nodes get the type suffix to avoid collisions
      sb.append(KEY_SEP);
      sb.append(metadataObject.type().name());
    }
    return sb.toString();
  }

  /** Returns true for entity types that can contain children (metalake, catalog, schema). */
  @VisibleForTesting
  public static boolean isContainerType(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE
        || type == MetadataObject.Type.CATALOG
        || type == MetadataObject.Type.SCHEMA;
  }
}
