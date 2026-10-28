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

import com.google.common.annotations.VisibleForTesting;
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
 */
final class IcebergCatalogWrapperProvider {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperProvider.class);

  private final String catalogName;
  private final IcebergConfig icebergConfig;
  private final boolean useIrc;
  private final IcebergCatalogWrapperCache localCache;

  @VisibleForTesting IcebergCatalogWrapper lastResolved;

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
   * Creates a provider for the given catalog. Non-aux eagerly loads into the local cache; aux
   * defers IRC resolution to avoid re-entering catalog load during initialize.
   *
   * @param catalogName catalog name
   * @param icebergConfig catalog configuration
   * @return provider
   */
  static IcebergCatalogWrapperProvider create(String catalogName, IcebergConfig icebergConfig) {
    boolean useIrc = auxIrcAvailable();
    if (useIrc) {
      return new IcebergCatalogWrapperProvider(catalogName, icebergConfig, true, null);
    }
    IcebergCatalogWrapperProvider provider =
        new IcebergCatalogWrapperProvider(
            catalogName, icebergConfig, false, IcebergCatalogWrapperCache.create(icebergConfig));
    provider.get();
    return provider;
  }

  /**
   * Test helper that always returns {@code wrapper}.
   *
   * @param wrapper fixed wrapper
   * @return provider
   */
  @VisibleForTesting
  static IcebergCatalogWrapperProvider fixed(IcebergCatalogWrapper wrapper) {
    IcebergCatalogWrapperProvider provider =
        new IcebergCatalogWrapperProvider(null, null, false, null);
    provider.lastResolved = wrapper;
    return provider;
  }

  /**
   * Returns the current catalog wrapper (IRC or local cache get-or-create).
   *
   * @return catalog wrapper
   */
  IcebergCatalogWrapper get() {
    if (useIrc) {
      CatalogWrapperManager manager =
          IcebergRESTServerContext.getInstance().catalogWrapperManager();
      Preconditions.checkState(
          manager != null,
          "Internal CatalogWrapperManager is not available. "
              + "Aux lakehouse catalog requires Iceberg REST embedded in Gravitino server.");
      lastResolved = manager.getCatalogWrapper(catalogName, icebergConfig);
      return lastResolved;
    }
    if (localCache != null) {
      lastResolved = loadFromLocalCache();
      return lastResolved;
    }
    return lastResolved;
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
      return;
    }
    if (!useIrc && lastResolved != null) {
      try {
        lastResolved.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Iceberg catalog", e);
      }
    }
  }

  private IcebergCatalogWrapper loadFromLocalCache() {
    try {
      return localCache.get(catalogName, name -> IcebergCatalogWrapper.create(icebergConfig));
    } catch (NoSuchWarehouseException e) {
      throw new IllegalArgumentException(
          String.format(
              "The 'warehouse' value '%s' could not be resolved by the Iceberg REST server. On "
                  + "the REST backend 'warehouse' selects a catalog by name on the remote server "
                  + "and is not a storage location; remove 'warehouse' to use the server's default "
                  + "catalog, or set it to a catalog name/identifier that the server recognizes.",
              icebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE)),
          e);
    } catch (RESTException e) {
      throw IcebergCatalogOperations.handleRestException(e);
    }
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
