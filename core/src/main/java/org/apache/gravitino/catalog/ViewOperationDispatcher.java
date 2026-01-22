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

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
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
   * <p>Delegates directly to the underlying catalog's ViewCatalog interface. Views are loaded from
   * the external catalog without caching in Gravitino's EntityStore.
   *
   * <p>TODO(#9746): Add entity storage support to cache view metadata in EntityStore.
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
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithViewOps(v -> v.loadView(ident)),
                NoSuchViewException.class));
  }
}
