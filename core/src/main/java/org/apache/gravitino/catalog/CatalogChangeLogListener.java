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
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invalidates {@link CatalogManager}'s local catalog cache from {@code entity_change_log}.
 *
 * <p>This listener is called <em>synchronously</em> in the poller thread. Implementations must not
 * block or perform expensive I/O; only fast, in-memory cache invalidations are permitted.
 *
 * <p>This listener never propagates a failure to the poller, so the poller never retries a batch
 * for it. That is deliberate: local-mutation de-duplication ({@link
 * CatalogManager#consumeLocalMutation}) is single-shot, so re-delivering an already-applied batch
 * would invalidate a catalog this process mutated itself and close its still-in-use {@code
 * IsolatedClassLoader}. Dropping an invalidation is the cheaper failure: the catalog cache expires
 * on access, so staleness is bounded by {@code gravitino.catalog.cache.evictionIntervalMs}.
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
        catalogManager.getCatalogCache().invalidate(ident);
      } catch (RuntimeException e) {
        // Deliberately not rethrown: see the class javadoc. A dropped invalidation only costs
        // bounded staleness here, while a retry of an already-applied batch can tear down a
        // catalog that is still in use.
        LOG.warn(
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

    String[] names = change.getFullName().split("\\.");
    if (names.length != 2) {
      LOG.warn("Invalid catalog full name in entity change log: {}", change.getFullName());
      return Optional.empty();
    }
    return Optional.of(NameIdentifier.of(names[0], names[1]));
  }
}
