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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.LocalCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.exceptions.RESTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves {@link IcebergCatalogWrapper} for lakehouse catalog/view ops via {@link
 * CatalogWrapperManager}: embedded IRC manager when aux, otherwise {@link
 * LocalCatalogWrapperManager}.
 *
 * <p>{@link #get()} loads the backend catalog and maps warehouse / unreachable REST failures into
 * Gravitino's create-time exception taxonomy.
 */
class IcebergCatalogWrapperProvider {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperProvider.class);

  private final String catalogName;
  private final IcebergConfig icebergConfig;
  private final CatalogWrapperManager manager;
  /** Non-null only for non-aux: lakehouse-created manager that this provider must close. */
  @Nullable private final LocalCatalogWrapperManager localOwned;

  private IcebergCatalogWrapperProvider(
      String catalogName,
      IcebergConfig icebergConfig,
      CatalogWrapperManager manager,
      @Nullable LocalCatalogWrapperManager localOwned) {
    this.catalogName = catalogName;
    this.icebergConfig = icebergConfig;
    this.manager = manager;
    this.localOwned = localOwned;
  }

  static IcebergCatalogWrapperProvider create(String catalogName, IcebergConfig icebergConfig) {
    try {
      IcebergRESTServerContext serverContext = IcebergRESTServerContext.getInstance();
      if (serverContext.isAuxMode()) {
        CatalogWrapperManager ircManager = serverContext.catalogWrapperManager();
        Preconditions.checkState(
            ircManager != null,
            "Internal CatalogWrapperManager is not available. "
                + "Aux lakehouse catalog requires Iceberg REST embedded in Gravitino server.");
        // Borrow IRC manager; do not close it with this provider.
        return new IcebergCatalogWrapperProvider(catalogName, icebergConfig, ircManager, null);
      }
    } catch (IllegalStateException e) {
      // IRC context not initialized (non-aux / unit tests).
    }
    LocalCatalogWrapperManager local = new LocalCatalogWrapperManager(icebergConfig);
    return new IcebergCatalogWrapperProvider(catalogName, icebergConfig, local, local);
  }

  IcebergCatalogWrapper get() {
    try {
      IcebergCatalogWrapper wrapper = manager.getCatalogWrapper(catalogName, icebergConfig);
      // Force backend load: REST warehouse / connectivity fail here, not at wrapper construction.
      wrapper.getCatalog();
      return wrapper;
    } catch (NoSuchWarehouseException e) {
      invalidate();
      throw new IllegalArgumentException(
          String.format(
              "The 'warehouse' value '%s' could not be resolved by the Iceberg REST server. On "
                  + "the REST backend 'warehouse' selects a catalog by name on the remote server "
                  + "and is not a storage location; remove 'warehouse' to use the server's default "
                  + "catalog, or set it to a catalog name/identifier that the server recognizes.",
              icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE)),
          e);
    } catch (RESTException e) {
      invalidate();
      throw IcebergCatalogOperations.handleRestException(e);
    }
  }

  void invalidate() {
    if (catalogName != null) {
      manager.invalidateCatalogWrapper(catalogName);
    }
  }

  void close() {
    if (localOwned == null) {
      return;
    }
    try {
      localOwned.close();
    } catch (Exception e) {
      LOG.warn("Failed to close local catalog wrapper manager", e);
    }
  }
}
