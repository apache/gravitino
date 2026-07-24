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
      try {
        EntityType type = entityType(change);
        NameIdentifier ident = identifier(change);
        if (type == null || ident == null) {
          continue;
        }

        LOG.debug("Invalidating entity cache due to entity change log: {} ({})", ident, type);
        cache.invalidate(ident, type);
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to process entity change log record: fullName={}, entityType={}",
            change.getFullName(),
            change.getEntityType(),
            e);
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
    // The change log stores the entity's NameIdentifier#toString(), a dot-joined full name. Split
    // it back into levels the same way the catalog cache listener does; this is exact as long as
    // no name segment contains a dot.
    return NameIdentifier.of(fullName.split("\\."));
  }
}
