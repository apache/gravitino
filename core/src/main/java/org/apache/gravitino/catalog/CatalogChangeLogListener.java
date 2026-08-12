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

import java.util.ArrayList;
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
 * <p>The poller requires each listener to be self-healing, and this one recovers the same way
 * {@code EntityCacheChangeLogListener} and {@code JcasbinChangeListener} do: a failed eviction
 * clears the whole catalog cache, which is a strict superset of the eviction that failed and of the
 * rest of the batch. A malformed row is skipped instead, because it names no catalog and so leaves
 * nothing stale.
 *
 * <p>The listener consumes local-mutation markers for the whole batch before evicting anything. A
 * failed eviction or clear therefore cannot strand a marker that would make a later remote change
 * look local. If the clear itself fails, the exception reaches the poller, which logs it at {@code
 * ERROR} and advances its cursor; the affected catalog can then remain stale until it expires.
 *
 * <p><b>Cost of the clear:</b> evicting a cached catalog closes its {@code CatalogWrapper}, which
 * tears down the connection pool and the {@code IsolatedClassLoader}. A whole-cache clear therefore
 * also closes catalogs this process is actively serving; requests holding classes from a closed
 * loader can fail with {@code NoClassDefFoundError}, the failure mode of #11739. This is accepted
 * deliberately so that a stale catalog is never served: the alternative left this node serving the
 * changed catalog from cache for up to {@code gravitino.catalog.cache.evictionIntervalMs}. The
 * clear runs only on a failed eviction, which is off the normal path.
 */
public class CatalogChangeLogListener implements EntityChangeLogListener {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogChangeLogListener.class);

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
    List<CatalogInvalidation> remoteInvalidations = new ArrayList<>();
    for (EntityChangeRecord change : changes) {
      if (!isCatalogChange(change)) {
        continue;
      }

      Optional<NameIdentifier> identOpt = catalogIdentifier(change);
      if (identOpt.isEmpty()) {
        // Already logged. A row that names no catalog cannot leave a stale entry behind, so it is
        // skipped rather than escalated to a cache clear.
        continue;
      }
      NameIdentifier ident = identOpt.get();

      boolean localMutation;
      try {
        localMutation = catalogManager.consumeLocalMutation(ident);
      } catch (RuntimeException e) {
        // The identifier is valid, so this record may name a remote mutation. Treating an unknown
        // origin as remote can cause an unnecessary eviction, but skipping it can leave a stale
        // catalog cached after the poller advances its cursor.
        LOG.error(
            "Failed to check local mutation state for catalog {}, treating change log record id {} "
                + "as remote to avoid serving stale metadata",
            ident,
            change.getId(),
            e);
        localMutation = false;
      }

      if (localMutation) {
        LOG.debug(
            "Skipping catalog cache invalidation for local mutation: {}, change log id {}",
            ident,
            change.getId());
        continue;
      }

      remoteInvalidations.add(new CatalogInvalidation(change, ident));
    }

    for (CatalogInvalidation invalidation : remoteInvalidations) {
      EntityChangeRecord change = invalidation.change;
      NameIdentifier ident = invalidation.ident;
      // Logged at INFO on purpose: this tears down the cached catalog, including its connection
      // pool and isolated classloader, and it is the main cross-node effect of the change log.
      // CatalogManager logs the matching "Closing catalog" line when the eviction runs.
      LOG.info(
          "Invalidating catalog cache for {} due to a remote {} recorded in change log id {}",
          ident,
          change.getOperateType(),
          change.getId());

      try {
        catalogManager.getCatalogCache().invalidate(ident);
      } catch (RuntimeException e) {
        // The poller dispatches a batch once and never replays it, so dropping this eviction would
        // serve the catalog stale until the eviction interval expires. The whole cache is cleared
        // instead; see the class javadoc for the classloader cost this accepts.
        LOG.error(
            "Failed to evict catalog {} for change log id {}, clearing the whole catalog cache to "
                + "avoid serving it stale; catalogs in use by this node are closed as a result",
            ident,
            change.getId(),
            e);
        catalogManager.getCatalogCache().invalidateAll();
        return;
      }
    }
  }

  private static class CatalogInvalidation {
    private final EntityChangeRecord change;
    private final NameIdentifier ident;

    private CatalogInvalidation(EntityChangeRecord change, NameIdentifier ident) {
      this.change = change;
      this.ident = ident;
    }
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
