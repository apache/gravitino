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

import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ViewOperationDispatcher} is the operation dispatcher for view operations.
 *
 * <p>Currently only supports loadView(). Full CRUD operations (create, alter, drop) needs to be
 * added when Gravitino APIs support view management.
 */
public class ViewOperationDispatcher extends OperationDispatcher implements ViewDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(ViewOperationDispatcher.class);

  /**
   * Creates a new ViewOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for view operations.
   * @param store The EntityStore instance to be used for view operations.
   * @param idGenerator The IdGenerator instance to be used for view operations.
   */
  public ViewOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * Load view metadata by identifier from the catalog.
   *
   * <p>This method first checks if the view exists in Gravitino's EntityStore. If found, it loads
   * from the catalog and verifies consistency. If not found, it loads from the catalog and
   * auto-imports into EntityStore.
   *
   * @param ident The view identifier.
   * @return The loaded view metadata.
   * @throws NoSuchViewException If the view does not exist.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    LOG.info("Loading view: {}", ident);

    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          // Load view from the underlying catalog
          View catalogView =
              doWithCatalog(
                  getCatalogIdentifier(ident),
                  c -> c.doWithViewOps(v -> v.loadView(ident)),
                  NoSuchViewException.class);

          // Check if view exists in entity store
          GenericEntity viewEntity = null;
          try {
            viewEntity = store.get(ident, Entity.EntityType.VIEW, GenericEntity.class);
          } catch (NoSuchEntityException e) {
            // View not in store yet, will auto-import below
            LOG.debug("View {} not found in entity store, will auto-import", ident);
          } catch (IOException ioe) {
            LOG.warn("Failed to check if view {} exists in entity store", ident, ioe);
          }

          // If view doesn't exist in entity store, auto-import it
          if (viewEntity == null) {
            LOG.info("Auto-importing view {} into Gravitino entity store", ident);
            long uid = idGenerator.nextId();
            GenericEntity newViewEntity =
                GenericEntity.builder()
                    .withId(uid)
                    .withName(ident.name())
                    .withNamespace(ident.namespace())
                    .withEntityType(Entity.EntityType.VIEW)
                    .build();
            try {
              store.put(newViewEntity, false /* overwrite */);
              LOG.info("Successfully imported view {} into entity store with id {}", ident, uid);
            } catch (Exception e) {
              // Log but don't fail - view import is best-effort
              LOG.warn("Failed to import view {} into entity store: {}", ident, e.getMessage());
            }
          }

          return catalogView;
        });
  }
}
