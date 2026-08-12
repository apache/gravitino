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
package org.apache.gravitino.catalog;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.EntityChangeLogListener;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invalidates {@link CatalogManager}'s local catalog cache from {@code entity_change_log}.
 *
 * <p>This listener is called <em>synchronously</em> in the poller thread. Implementations must not
 * block or perform expensive I/O; only fast, in-memory cache invalidations are permitted.
 *
 * <p>The poller requires each listener to be self-healing. This one heals within a single record:
 * it retries that record's own eviction (see {@link #invalidateWithRetry}) and then swallows the
 * failure, so one bad record never costs the rest of the batch. It must NOT recover by clearing the
 * catalog cache the way {@code EntityCacheChangeLogListener} and {@code JcasbinChangeListener} do,
 * because evicting a catalog this process still uses closes its in-use {@code IsolatedClassLoader}
 * (#11739). Giving up on one eviction is the cheaper failure: the catalog cache expires on access,
 * so staleness is bounded by {@code gravitino.catalog.cache.evictionIntervalMs}.
 */
public class CatalogChangeLogListener implements EntityChangeLogListener {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogChangeLogListener.class);

  /**
   * How many times one catalog eviction is attempted. This is the listener's whole self-healing
   * budget: see {@link #invalidateWithRetry} for why recovery cannot be broadened here.
   */
  private static final int MAX_INVALIDATION_ATTEMPTS = 2;

  private final CatalogManager catalogManager;

  /**
   * Creates a listener for a catalog manager.
   *
   * @param catalogManager the catalog manager whose local cache should be invalidated
   */
  public CatalogChangeLogListener(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  @Override
  public void onEntityChange(List<EntityChangeRecord> changes) {
    for (EntityChangeRecord change : changes) {
      try {
        if (!isCatalogChange(change)) {
          continue;
        }

        Optional<NameIdentifier> identOpt = catalogIdentifier(change);
        if (identOpt.isEmpty()) {
          continue;
        }
        NameIdentifier ident = identOpt.get();

        if (catalogManager.consumeLocalMutation(ident)) {
          LOG.debug(
              "Skipping catalog cache invalidation for local mutation: {}, change log id {}",
              ident,
              change.getId());
          continue;
        }

        // Logged at INFO on purpose: this tears down the cached catalog, including its connection
        // pool and isolated classloader, and it is the main cross-node effect of the change log.
        // CatalogManager logs the matching "Closing catalog" line when the eviction runs.
        LOG.info(
            "Invalidating catalog cache for {} due to a remote {} recorded in change log id {}",
            ident,
            change.getOperateType(),
            change.getId());
        invalidateWithRetry(ident, change);
      } catch (RuntimeException e) {
        // Deliberately not rethrown: see the class javadoc. The poller dispatches a batch once, so
        // rethrowing would only lose the remaining records of this batch as well.
        LOG.error(
            "Failed to process catalog change log record: id={}, fullName={}, entityType={}, "
                + "operateType={}",
            change.getId(),
            change.getFullName(),
            change.getEntityType(),
            change.getOperateType(),
            e);
      }
    }
  }

  /**
   * Evicts one catalog, retrying the eviction itself up to {@link #MAX_INVALIDATION_ATTEMPTS}
   * times.
   *
   * <p>Recovery here is deliberately narrow, and both limits are correctness requirements rather
   * than tuning choices:
   *
   * <ul>
   *   <li>It is scoped to the single identifier this change log record named. Clearing the whole
   *       catalog cache - the fallback the entity and JCasbin caches use - would evict catalogs
   *       this process is actively serving and close their in-use {@code IsolatedClassLoader}s,
   *       which is the permanent {@code NoClassDefFoundError} of #11739. Only the catalog that
   *       actually changed on another node may be torn down.
   *   <li>It retries the eviction only, never the {@link CatalogManager#consumeLocalMutation} probe
   *       that ran before it. That marker is single-shot, so re-consulting it would classify a
   *       local mutation as remote and tear down a catalog this node just mutated itself.
   * </ul>
   *
   * <p>If every attempt fails, the record is given up on: this node keeps serving that catalog from
   * cache until {@code gravitino.catalog.cache.evictionIntervalMs} expires it, so the failure is
   * logged at {@code ERROR}.
   */
  private void invalidateWithRetry(NameIdentifier ident, EntityChangeRecord change) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= MAX_INVALIDATION_ATTEMPTS; attempt++) {
      try {
        catalogManager.getCatalogCache().invalidate(ident);
        if (attempt > 1) {
          LOG.info("Evicted catalog {} on attempt {}", ident, attempt);
        }
        return;
      } catch (RuntimeException e) {
        lastFailure = e;
        LOG.warn(
            "Failed to evict catalog {} from the catalog cache on attempt {} of {}",
            ident,
            attempt,
            MAX_INVALIDATION_ATTEMPTS,
            e);
      }
    }

    LOG.error(
        "Giving up on evicting catalog {} after {} attempt(s) for change log id {}; this node may "
            + "serve it stale until the catalog cache eviction interval expires it",
        ident,
        MAX_INVALIDATION_ATTEMPTS,
        change.getId(),
        lastFailure);
  }

  private boolean isCatalogChange(EntityChangeRecord change) {
    if (change.getEntityType() == null) {
      return false;
    }
    return EntityType.CATALOG.name().equals(change.getEntityType().toUpperCase(Locale.ROOT));
  }

  private Optional<NameIdentifier> catalogIdentifier(EntityChangeRecord change) {
    if (change.getFullName() == null) {
      LOG.warn("Invalid catalog full name in entity change log: null");
      return Optional.empty();
    }

    NameIdentifier ident;
    try {
      ident = EntityChangeLogNameIdentifierCodec.decode(change.getFullName());
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid catalog full name in entity change log: {}", change.getFullName());
      return Optional.empty();
    }
    if (ident.namespace().length() != 1) {
      LOG.warn("Invalid catalog full name in entity change log: {}", change.getFullName());
      return Optional.empty();
    }
    return Optional.of(ident);
  }
}
