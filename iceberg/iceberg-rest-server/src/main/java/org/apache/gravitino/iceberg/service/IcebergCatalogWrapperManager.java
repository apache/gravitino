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
import org.apache.commons.lang3.StringUtils;
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
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWrapperManager implements CatalogWrapperManager, AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperManager.class);

  private final IcebergCatalogWrapperCache catalogWrapperCache;

  private final IcebergConfigProvider configProvider;

  private final boolean auxMode;

  private final String metalakeName;

  public IcebergCatalogWrapperManager(
      Map<String, String> properties,
      IcebergConfigProvider configProvider,
      boolean auxMode,
      String metalakeName) {
    this.configProvider = configProvider;
    this.auxMode = auxMode;
    this.metalakeName = metalakeName;
    this.catalogWrapperCache = IcebergCatalogWrapperCache.create(new IcebergConfig(properties));
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
    LOG.debug("getCatalogWrapper catalogName={}", catalogName);
    rejectStaticProviderWhenAuthorizationEnabled();
    if (!auxMode) {
      return catalogWrapperCache.get(catalogName, this::createCatalogWrapperFromProvider);
    }
    Optional<Long> version = catalogCurrentVersion(catalogName);
    if (!version.isPresent()) {
      catalogWrapperCache.invalidate(catalogName);
      return createCatalogWrapperFromProvider(catalogName);
    }
    return catalogWrapperCache.get(
        catalogName, version.get(), this::createCatalogWrapperFromProvider);
  }

  @Override
  public IcebergCatalogWrapper getOrCreateCatalogWrapper(
      String catalogName, IcebergConfig icebergConfig) {
    LOG.debug("getOrCreateCatalogWrapper catalogName={}", catalogName);
    if (!auxMode) {
      return catalogWrapperCache.get(
          catalogName, name -> createCatalogWrapper(name, icebergConfig));
    }
    Optional<Long> version = catalogCurrentVersion(catalogName);
    if (!version.isPresent()) {
      return catalogWrapperCache.get(
          catalogName, name -> createCatalogWrapper(name, icebergConfig));
    }
    return catalogWrapperCache.get(
        catalogName, version.get(), name -> createCatalogWrapper(name, icebergConfig));
  }

  @Override
  public void invalidateCatalogWrapper(String catalogName) {
    catalogWrapperCache.invalidate(catalogName);
    String defaultCatalogName = configProvider.getDefaultCatalogName();
    if (catalogName != null
        && catalogName.equals(defaultCatalogName)
        && !IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG.equals(catalogName)) {
      catalogWrapperCache.invalidate(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG);
    }
  }

  @Override
  public void acquireCatalogWrapper(IcebergCatalogWrapper wrapper) {
    catalogWrapperCache.acquire(wrapper);
  }

  @Override
  public void releaseCatalogWrapper(IcebergCatalogWrapper wrapper) {
    catalogWrapperCache.release(wrapper);
  }

  /**
   * Reads {@code catalog_meta.current_version} for {@code catalogName}. Empty when the row is gone
   * or this manager has no metalake to query.
   *
   * @param catalogName catalog name, or the Iceberg REST default-catalog alias
   * @return the catalog version stored in the database
   */
  @VisibleForTesting
  protected Optional<Long> catalogCurrentVersion(String catalogName) {
    if (StringUtils.isBlank(metalakeName) || StringUtils.isBlank(catalogName)) {
      return Optional.empty();
    }
    String name = catalogName;
    if (IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG.equals(catalogName)) {
      name = configProvider.getDefaultCatalogName();
      if (StringUtils.isBlank(name) || IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG.equals(name)) {
        return Optional.empty();
      }
    }
    String resolvedName = name;
    Long version =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogCurrentVersionByName(metalakeName, resolvedName));
    return Optional.ofNullable(version);
  }

  private void rejectStaticProviderWhenAuthorizationEnabled() {
    IcebergRESTServerContext serverContext = IcebergRESTServerContext.getInstance();
    if (serverContext.isAuthorizationEnabled()
        && !(configProvider instanceof DynamicIcebergConfigProvider)) {
      throw new IllegalArgumentException(
          "Authorization is enabled. Set `gravitino.iceberg-rest.catalog-config-provider="
              + "dynamic-config-provider` in gravitino.conf for Iceberg REST.");
    }
  }

  private CatalogWrapperForREST createCatalogWrapperFromProvider(String catalogName) {
    rejectStaticProviderWhenAuthorizationEnabled();

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
