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
 * <p>The poller hands each batch to a listener only once, so a listener has to clean up after
 * itself when it fails. This one does what {@code EntityCacheChangeLogListener} and {@code
 * JcasbinChangeListener} do: if removing one catalog from the cache fails, it clears the whole
 * catalog cache, which also covers the entry it failed to remove and the rest of the batch. A row
 * that cannot be parsed is simply skipped, because it does not point at any catalog and so cannot
 * leave anything stale.
 *
 * <p>Before removing anything, the listener first goes through the whole batch and marks off the
 * changes this node made itself. Doing it in that order means a later failure cannot leave one of
 * those marks behind, which would otherwise make a future change from another node look like a
 * local one. If the clear itself fails, the exception goes up to the poller, which logs it at
 * {@code ERROR} and moves on, and the catalog stays stale until it expires.
 *
 * <p><b>What clearing costs:</b> dropping a catalog from the cache retires its {@code
 * CatalogWrapper}. Idle wrappers release their connection pools and {@code IsolatedClassLoader}s
 * immediately; wrappers with an active operation lease defer cleanup until their last lease is
 * closed, so an operation is never torn down mid-flight. Connector-backed metadata returned by an
 * operation is converted to a detached snapshot before the lease closes; later hooks and REST
 * serialization therefore do not depend on the retired wrapper. The clear only happens when a
 * normal removal failed, never during normal operation.
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
        // Already logged. This row does not point at any catalog, so there is nothing stale to
        // clean up. Just skip it instead of clearing the cache.
        continue;
      }
      NameIdentifier ident = identOpt.get();

      boolean localMutation;
      try {
        localMutation = catalogManager.consumeLocalMutation(ident);
      } catch (RuntimeException e) {
        // We could not tell whether this change came from this node or another one. The name is
        // valid, so assume it came from another node: the worst case is one extra cache removal,
        // while skipping it could leave an old catalog cached forever.
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
      // INFO on purpose: dropping the catalog from the cache retires its wrapper and eventually
      // closes the connection pool and isolated classloader, either immediately or after active
      // leases finish. This is the main thing the change log does across nodes.
      LOG.info(
          "Invalidating catalog cache for {} due to a remote {} recorded in change log id {}",
          ident,
          change.getOperateType(),
          change.getId());

      try {
        catalogManager.getCatalogCache().invalidate(ident);
      } catch (RuntimeException e) {
        // This batch will never be sent again, so giving up here would keep serving the old
        // catalog until it expires on its own. Clear the whole cache instead; active operation
        // leases defer resource cleanup until those operations finish.
        LOG.error(
            "Failed to evict catalog {} for change log id {}, clearing the whole catalog cache to "
                + "avoid serving it stale; resources in use are retired after their leases close",
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
    } catch (RuntimeException e) {
      // Catch every unchecked exception, not just IllegalArgumentException: if a future version of
      // the codec throws something else, one bad row must still be skipped instead of aborting the
      // whole batch, which would drop the invalidations already collected for the other rows.
      LOG.warn("Invalid catalog full name in entity change log: {}", change.getFullName(), e);
      return Optional.empty();
    }
    if (ident.namespace().length() != 1) {
      LOG.warn("Invalid catalog full name in entity change log: {}", change.getFullName());
      return Optional.empty();
    }
    return Optional.of(ident);
  }
}
