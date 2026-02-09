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
import org.apache.gravitino.GravitinoEnv;
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
   * <p>This method first checks if the view exists in Gravitino's EntityStore. If not found, it loads from the catalog
   * and auto-imports into EntityStore.
   * @param ident The view identifier.
   * @return The loaded view metadata.
   * @throws NoSuchViewException If the view does not exist.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    LOG.info("Loading view: {}", ident);

    // First load with READ lock to check if view is already imported
    EntityCombinedView entityCombinedView =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadView(ident));

    if (!entityCombinedView.imported()) {
      // Load the schema to make sure the schema is imported.
      SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      schemaDispatcher.loadSchema(schemaIdent);

      // Import the view.
      entityCombinedView =
          TreeLockUtils.doWithTreeLock(schemaIdent, LockType.WRITE, () -> importView(ident));
    }

    return entityCombinedView;
  }

  /**
   * Internal method to load view and check if it exists in entity store.
   *
   * @param ident The view identifier.
   * @return EntityCombinedView containing the view and import status.
   * @throws NoSuchViewException If the view does not exist.
   */
  private EntityCombinedView internalLoadView(NameIdentifier ident)
      throws NoSuchViewException {
    // Load view from the underlying catalog
    View catalogView =
        doWithCatalog(
            getCatalogIdentifier(ident),
            c -> c.doWithViewOps(v -> v.loadView(ident)),
            NoSuchViewException.class);

    // Check if view exists in entity store
    try {
      GenericEntity viewEntity =
          store.get(ident, Entity.EntityType.VIEW, GenericEntity.class);
      return EntityCombinedView.of(catalogView, viewEntity).withImported(true);
    } catch (NoSuchEntityException e) {
      // View not in store yet
      LOG.debug("View {} not found in entity store", ident);
      return EntityCombinedView.of(catalogView).withImported(false);
    } catch (IOException ioe) {
      LOG.warn("Failed to check if view {} exists in entity store", ident, ioe);
      return EntityCombinedView.of(catalogView).withImported(false);
    }
  }

  /**
   * Import view into Gravitino entity store.
   *
   * @param ident The view identifier.
   * @return EntityCombinedView containing the view and import status.
   * @throws NoSuchViewException If the view does not exist.
   */
  private EntityCombinedView importView(NameIdentifier ident) throws NoSuchViewException {
    // Double-check if already imported (another thread might have imported between locks)
    EntityCombinedView entityCombinedView = internalLoadView(ident);

    if (entityCombinedView.imported()) {
      return entityCombinedView;
    }

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
      return EntityCombinedView.of(entityCombinedView.viewFromCatalog(), newViewEntity)
          .withImported(true);
    } catch (Exception e) {
      // Log but don't fail - view import is best-effort
      LOG.warn("Failed to import view {} into entity store: {}", ident, e.getMessage());
      return EntityCombinedView.of(entityCombinedView.viewFromCatalog()).withImported(false);
    }
  }
}
