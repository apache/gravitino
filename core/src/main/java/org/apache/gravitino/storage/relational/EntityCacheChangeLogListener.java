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
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.metrics.source.EntityChangeLogMetricsSource;
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
 *       clear also fails, the exception goes up to {@link EntityChangeLogPoller}, which only logs
 *       it and moves on. The batch is never sent again, so this node may keep serving stale entries
 *       until they expire.
 * </ul>
 */
public class EntityCacheChangeLogListener implements EntityChangeLogListener {

  private static final Logger LOG = LoggerFactory.getLogger(EntityCacheChangeLogListener.class);

  /**
   * The two invalidation entry points this listener needs from the cache it keeps coherent. The
   * entity store hands in its own implementation so that it observes every invalidation, including
   * the ones replayed from other nodes (see {@code RelationalEntityStore#batchGet}).
   */
  public interface Target {
    /**
     * Invalidates the cache entry of the given entity, see {@link EntityCache#invalidate}.
     *
     * @param ident the identifier of the changed entity
     * @param type the type of the changed entity
     */
    void invalidate(NameIdentifier ident, EntityType type);

    /** Clears the whole cache, see {@link EntityCache#clear()}. */
    void clear();
  }

  private final Target target;
  private final EntityChangeLogMetricsSource metrics;

  /**
   * Creates a listener that invalidates the given entity store cache directly. Metrics from this
   * constructor are local to the listener and are not exported by the server metrics system.
   *
   * @param cache the per-node entity store cache to keep coherent
   */
  public EntityCacheChangeLogListener(EntityCache cache) {
    this(asTarget(cache), new EntityChangeLogMetricsSource());
  }

  /**
   * Creates a listener that invalidates the given entity store cache directly, with metrics shared
   * with the poller.
   *
   * @param cache the per-node entity store cache to keep coherent
   * @param metrics process-local change log metrics
   */
  public EntityCacheChangeLogListener(EntityCache cache, EntityChangeLogMetricsSource metrics) {
    this(asTarget(cache), metrics);
  }

  /**
   * Creates a listener that invalidates through the given target. Metrics from this constructor are
   * local to the listener and are not exported by the server metrics system.
   *
   * @param target the invalidation entry points of the per-node cache to keep coherent
   */
  public EntityCacheChangeLogListener(Target target) {
    this(target, new EntityChangeLogMetricsSource());
  }

  /**
   * Creates a listener that invalidates through the given target, with metrics shared with the
   * poller.
   *
   * @param target the invalidation entry points of the per-node cache to keep coherent
   * @param metrics process-local change log metrics
   */
  public EntityCacheChangeLogListener(Target target, EntityChangeLogMetricsSource metrics) {
    Preconditions.checkArgument(target != null, "target cannot be null");
    this.target = target;
    this.metrics = Preconditions.checkNotNull(metrics, "metrics cannot be null");
  }

  private static Target asTarget(EntityCache cache) {
    Preconditions.checkArgument(cache != null, "cache cannot be null");
    return new Target() {
      @Override
      public void invalidate(NameIdentifier ident, EntityType type) {
        cache.invalidate(ident, type);
      }

      @Override
      public void clear() {
        cache.clear();
      }
    };
  }

  @Override
  public void onEntityChange(List<EntityChangeRecord> changes) {
    long startNanos = System.nanoTime();
    int applied = 0;
    int skipped = 0;
    for (EntityChangeRecord change : changes) {
      EntityType type = entityType(change);
      NameIdentifier ident = identifier(change);
      if (type == null || ident == null) {
        // Already logged by the parsing helpers. A row that names no entity cannot invalidate
        // anything, so skipping it leaves no stale entry behind.
        skipped++;
        continue;
      }

      try {
        LOG.debug(
            "entityChangeLog invalidate changeId={} entityType={} operateType={} ident={} fullName={}",
            change.getId(),
            type,
            change.getOperateType(),
            ident,
            change.getFullName());
        target.invalidate(ident, type);
        applied++;
        metrics.recordsApplied(1);
      } catch (RuntimeException e) {
        metrics.invalidationFailed();
        // Dropping a single invalidation would leave this node serving that entity stale until it
        // expires. Clearing the whole cache is the safe superset, and it also covers the rest of
        // this batch, so there is nothing left to replay.
        LOG.error(
            "entityChangeLog targeted invalidation failed changeId={} entityType={} "
                + "operateType={} ident={} fullName={}; clearing full local entity cache",
            change.getId(),
            type,
            change.getOperateType(),
            ident,
            change.getFullName(),
            e);
        target.clear();
        metrics.fallbackCleared();
        LOG.debug(
            "entityChangeLog invalidate batch count={} applied={} skipped={} fallbackClear=true durationMs={}",
            changes.size(),
            applied,
            skipped,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        return;
      }
    }
    LOG.debug(
        "entityChangeLog invalidate batch count={} applied={} skipped={} fallbackClear=false durationMs={}",
        changes.size(),
        applied,
        skipped,
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
  }

  private EntityType entityType(EntityChangeRecord change) {
    if (change.getEntityType() == null) {
      LOG.warn("entityChangeLog malformed changeId={} field=entityType value=null", change.getId());
      return null;
    }
    try {
      return EntityType.valueOf(change.getEntityType().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "entityChangeLog malformed changeId={} field=entityType value={}",
          change.getId(),
          change.getEntityType());
      return null;
    }
  }

  private NameIdentifier identifier(EntityChangeRecord change) {
    String fullName = change.getFullName();
    if (fullName == null || fullName.isEmpty()) {
      LOG.warn(
          "entityChangeLog malformed changeId={} field=fullName value={}",
          change.getId(),
          fullName);
      return null;
    }
    try {
      return EntityChangeLogNameIdentifierCodec.decode(fullName);
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "entityChangeLog malformed changeId={} field=fullName value={}",
          change.getId(),
          fullName,
          e);
      return null;
    }
  }
}
