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
package org.apache.gravitino.storage.relational;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Locale;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps a per-node {@link EntityCache} coherent across a multi-node cluster by replaying {@code
 * entity_change_log} rows written by other nodes.
 *
 * <p>Every ALTER/DROP row is replayed as a direct {@link EntityCache#invalidate(NameIdentifier,
 * EntityType)} for exactly the changed entity key. Because the cache indexes its keys by identifier
 * prefix, invalidating a container (for example a schema) cascades to its cached children on the
 * local node through that forward prefix scan; no reverse index is involved.
 *
 * <p>This listener is registered only for a {@link
 * org.apache.gravitino.cache.Coherence#LOCAL_PER_NODE} cache: a shared cache has a single
 * cluster-wide copy and nothing per-node to invalidate. It is called <em>synchronously</em> on the
 * poller thread, so it performs only fast, in-memory, idempotent invalidations.
 *
 * <p>The two failure modes of a replayed row are handled separately, because they mean different
 * things:
 *
 * <ul>
 *   <li>A <b>malformed row</b> (unknown entity type, undecodable full name) names no entity, so
 *       there is nothing to invalidate. It is logged and skipped, and the rest of the batch still
 *       applies.
 *   <li>A <b>failed invalidation</b> means this node may now serve stale metadata indefinitely. The
 *       whole cache is cleared instead, which is strictly stronger than the invalidation that
 *       failed and only costs a cold-cache penalty, since the cache is derived state. If even the
 *       clear fails the exception propagates, and {@link EntityChangeLogPoller} retries the batch
 *       and ultimately applies its configured listener failure action.
 * </ul>
 */
public class EntityCacheChangeLogListener implements EntityChangeLogListener {

  private static final Logger LOG = LoggerFactory.getLogger(EntityCacheChangeLogListener.class);

  private final EntityCache cache;

  /**
   * Creates a listener that invalidates the given entity store cache.
   *
   * @param cache the per-node entity store cache to keep coherent
   */
  public EntityCacheChangeLogListener(EntityCache cache) {
    Preconditions.checkArgument(cache != null, "cache cannot be null");
    this.cache = cache;
  }

  @Override
  public void onEntityChange(List<EntityChangeRecord> changes) {
    for (EntityChangeRecord change : changes) {
      EntityType type = entityType(change);
      NameIdentifier ident = identifier(change);
      if (type == null || ident == null) {
        // Already logged by the parsing helpers. A row that names no entity cannot invalidate
        // anything, so skipping it leaves no stale entry behind.
        continue;
      }

      try {
        LOG.debug("Invalidating entity cache due to entity change log: {} ({})", ident, type);
        cache.invalidate(ident, type);
      } catch (RuntimeException e) {
        // Dropping a single invalidation would leave this node serving that entity stale until it
        // expires. Clearing the whole cache is the safe superset, and it also covers the rest of
        // this batch, so there is nothing left to replay.
        LOG.error(
            "Failed to invalidate {} ({}) from the entity change log, clearing the local entity "
                + "cache to stay coherent",
            ident,
            type,
            e);
        cache.clear();
        return;
      }
    }
  }

  private EntityType entityType(EntityChangeRecord change) {
    if (change.getEntityType() == null) {
      LOG.warn("Invalid entity type in entity change log: null");
      return null;
    }
    try {
      return EntityType.valueOf(change.getEntityType().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.warn("Unknown entity type in entity change log: {}", change.getEntityType());
      return null;
    }
  }

  private NameIdentifier identifier(EntityChangeRecord change) {
    String fullName = change.getFullName();
    if (fullName == null || fullName.isEmpty()) {
      LOG.warn("Invalid full name in entity change log: {}", fullName);
      return null;
    }
    try {
      return EntityChangeLogNameIdentifierCodec.decode(fullName);
    } catch (IllegalArgumentException e) {
      LOG.warn("Undecodable full name in entity change log: {}", fullName, e);
      return null;
    }
  }
}
