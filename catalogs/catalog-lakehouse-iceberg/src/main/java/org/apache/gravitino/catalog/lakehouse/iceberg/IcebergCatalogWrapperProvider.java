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
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapperCache;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.exceptions.RESTException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared resolver for {@link IcebergCatalogWrapper} used by {@link IcebergCatalogOperations} and
 * {@link IcebergViewCatalogOperations}.
 *
 * <p>Aux mode: IRC {@link CatalogWrapperManager} (pass metalake config so static IRC can still
 * create). Non-aux: lakehouse-owned {@link IcebergCatalogWrapperCache} with the same eviction/token
 * policy.
 *
 * <p>{@link #get()} loads the Iceberg backend catalog and maps {@link NoSuchWarehouseException} /
 * unreachable {@link RESTException} into Gravitino's create-time taxonomy (same as the pre-cache
 * eager {@code initialize} path).
 */
class IcebergCatalogWrapperProvider {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperProvider.class);

  private final String catalogName;
  private final IcebergConfig icebergConfig;
  private final boolean useIrc;
  private final IcebergCatalogWrapperCache localCache;

  private IcebergCatalogWrapperProvider(
      String catalogName,
      IcebergConfig icebergConfig,
      boolean useIrc,
      IcebergCatalogWrapperCache localCache) {
    this.catalogName = catalogName;
    this.icebergConfig = icebergConfig;
    this.useIrc = useIrc;
    this.localCache = localCache;
  }

  /**
   * Creates a provider for the given catalog. Aux defers IRC resolution to first {@link #get()} to
   * avoid re-entering catalog load during provider construction; callers that need create-time
   * validation (for example {@link IcebergCatalogOperations#initialize}) should invoke {@link
   * #get()} explicitly.
   *
   * @param catalogName catalog name
   * @param icebergConfig catalog configuration
   * @return provider
   */
  static IcebergCatalogWrapperProvider create(String catalogName, IcebergConfig icebergConfig) {
    if (auxIrcAvailable()) {
      return new IcebergCatalogWrapperProvider(catalogName, icebergConfig, true, null);
    }
    return new IcebergCatalogWrapperProvider(
        catalogName, icebergConfig, false, IcebergCatalogWrapperCache.create(icebergConfig));
  }

  /**
   * Returns the current catalog wrapper (IRC or local cache get-or-create) after loading the
   * Iceberg backend so warehouse / connectivity errors surface with the correct exception types.
   *
   * @return catalog wrapper
   */
  IcebergCatalogWrapper get() {
    try {
      IcebergCatalogWrapper wrapper = resolveWrapper();
      // Force backend load: REST warehouse resolution and connectivity fail here, not at wrapper
      // construction. Matches the pre-lazy-cache initialize() behavior.
      wrapper.getCatalog();
      return wrapper;
    } catch (NoSuchWarehouseException e) {
      invalidate();
      throw new IllegalArgumentException(unresolvableWarehouseMessage(), e);
    } catch (RESTException e) {
      invalidate();
      throw IcebergCatalogOperations.handleRestException(e);
    }
  }

  void invalidate() {
    if (useIrc) {
      try {
        CatalogWrapperManager manager =
            IcebergRESTServerContext.getInstance().catalogWrapperManager();
        if (manager != null && catalogName != null) {
          manager.invalidateCatalogWrapper(catalogName);
        }
      } catch (IllegalStateException e) {
        // IRC already shut down
      }
      return;
    }
    if (localCache != null && catalogName != null) {
      localCache.invalidate(catalogName);
    }
  }

  void close() {
    if (localCache != null) {
      try {
        localCache.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Iceberg catalog wrapper cache", e);
      }
    }
  }

  private IcebergCatalogWrapper resolveWrapper() {
    if (useIrc) {
      CatalogWrapperManager manager =
          IcebergRESTServerContext.getInstance().catalogWrapperManager();
      Preconditions.checkState(
          manager != null,
          "Internal CatalogWrapperManager is not available. "
              + "Aux lakehouse catalog requires Iceberg REST embedded in Gravitino server.");
      return manager.getCatalogWrapper(catalogName, icebergConfig);
    }
    Preconditions.checkState(localCache != null, "Local catalog wrapper cache is not initialized");
    return localCache.get(catalogName, name -> IcebergCatalogWrapper.create(icebergConfig));
  }

  private String unresolvableWarehouseMessage() {
    return String.format(
        "The 'warehouse' value '%s' could not be resolved by the Iceberg REST server. On "
            + "the REST backend 'warehouse' selects a catalog by name on the remote server "
            + "and is not a storage location; remove 'warehouse' to use the server's default "
            + "catalog, or set it to a catalog name/identifier that the server recognizes.",
        icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE));
  }

  private static boolean auxIrcAvailable() {
    try {
      IcebergRESTServerContext serverContext = IcebergRESTServerContext.getInstance();
      return serverContext.isAuxMode() && serverContext.catalogWrapperManager() != null;
    } catch (IllegalStateException e) {
      return false;
    }
  }
}
