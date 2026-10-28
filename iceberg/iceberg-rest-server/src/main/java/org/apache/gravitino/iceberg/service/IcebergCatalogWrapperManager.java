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
package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapperCache;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWrapperManager implements CatalogWrapperManager, AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperManager.class);

  private final IcebergCatalogWrapperCache catalogWrapperCache;

  private final IcebergConfigProvider configProvider;

  public IcebergCatalogWrapperManager(
      Map<String, String> properties,
      IcebergConfigProvider configProvider,
      boolean auxMode,
      String metalakeName) {
    this.configProvider = configProvider;
    long accessEvictionMs =
        new IcebergConfig(properties)
            .get(IcebergConfig.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL);
    this.catalogWrapperCache = new IcebergCatalogWrapperCache(accessEvictionMs);
    if (auxMode) {
      GravitinoEnv.getInstance()
          .catalogManager()
          .addCatalogCacheRemoveListener(
              ident -> {
                if (ident.namespace().level(0).equals(metalakeName)) {
                  catalogWrapperCache.invalidate(ident.name());
                  if (ident.name().equals(configProvider.getDefaultCatalogName())) {
                    catalogWrapperCache.invalidate(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG);
                  }
                }
              });
    }
  }

  /**
   * @param rawPrefix The path parameter is passed by a Jetty handler. The pattern is matching
   *     ([^/]*\/), end with /
   * @return the instance of IcebergCatalogWrapper.
   */
  public CatalogWrapperForREST getOps(String rawPrefix) {
    String catalogName = IcebergRESTUtils.getCatalogName(rawPrefix);
    return getCatalogWrapper(catalogName);
  }

  @Override
  public CatalogWrapperForREST getCatalogWrapper(String catalogName) {
    logCacheAccess(catalogName);
    return catalogWrapperCache.get(catalogName, this::createCatalogWrapperFromProvider);
  }

  @Override
  public IcebergCatalogWrapper getCatalogWrapper(String catalogName, IcebergConfig icebergConfig) {
    logCacheAccess(catalogName);
    return catalogWrapperCache.get(catalogName, name -> createCatalogWrapper(name, icebergConfig));
  }

  private void logCacheAccess(String catalogName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "getCatalogWrapper catalogName={} cacheHit={}",
          catalogName,
          catalogWrapperCache.getIfPresent(catalogName) != null);
    }
  }

  @Override
  public void invalidateCatalogWrapper(String catalogName) {
    catalogWrapperCache.invalidate(catalogName);
  }

  private CatalogWrapperForREST createCatalogWrapperFromProvider(String catalogName) {
    IcebergRESTServerContext serverContext = IcebergRESTServerContext.getInstance();
    if (serverContext.isAuthorizationEnabled()
        && !(configProvider instanceof DynamicIcebergConfigProvider)) {
      throw new IllegalArgumentException(
          "Authorization is enabled. Set `gravitino.iceberg-rest.catalog-config-provider="
              + "dynamic-config-provider` in gravitino.conf for Iceberg REST.");
    }

    Optional<IcebergConfig> icebergConfig = configProvider.getIcebergCatalogConfig(catalogName);
    if (!icebergConfig.isPresent()) {
      throw new NoSuchCatalogException(
          "Couldn't find Iceberg configuration for catalog %s", catalogName);
    }
    return createCatalogWrapper(catalogName, icebergConfig.get());
  }

  // Overriding this method to create a new CatalogWrapperForREST for test;
  @VisibleForTesting
  protected CatalogWrapperForREST createCatalogWrapper(
      String catalogName, IcebergConfig icebergConfig) {
    IcebergConfig enrichedConfig =
        IcebergCatalogUtil.withGcsServiceAccountCredentials(icebergConfig);
    // Federated REST backends use FederatedCatalogWrapper for FileIO / credential vending.
    IcebergCatalogBackend backend =
        IcebergCatalogBackend.valueOf(
            enrichedConfig.get(IcebergConfig.CATALOG_BACKEND).toUpperCase(Locale.ROOT));
    CatalogWrapperForREST rest =
        backend == IcebergCatalogBackend.REST
            ? new FederatedCatalogWrapper(catalogName, enrichedConfig)
            : new CatalogWrapperForREST(catalogName, enrichedConfig);
    return (CatalogWrapperForREST)
        IcebergCatalogWrapper.applyKerberos(rest, catalogName, enrichedConfig);
  }

  @Override
  public void close() throws Exception {
    catalogWrapperCache.close();
  }
}
