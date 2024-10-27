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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.CredentialProviderFactory;
import org.apache.gravitino.credential.CredentialProviderManager;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWrapperManager implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperManager.class);

  private final Cache<String, IcebergCatalogWrapper> icebergCatalogWrapperCache;

  private final IcebergConfigProvider configProvider;

  private CredentialProviderManager credentialProviderManager;

  public IcebergCatalogWrapperManager(
      Map<String, String> properties, IcebergConfigProvider configProvider) {
    this.credentialProviderManager = new CredentialProviderManager();
    this.configProvider = configProvider;
    this.icebergCatalogWrapperCache =
        Caffeine.newBuilder()
            .expireAfterWrite(
                (new IcebergConfig(properties))
                    .get(IcebergConfig.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL),
                TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  String catalogName = (String) k;
                  LOG.info("Remove IcebergCatalogWrapper cache {}.", catalogName);
                  closeIcebergCatalogWrapper((IcebergCatalogWrapper) v);
                  credentialProviderManager.unregisterCredentialProvider(catalogName);
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("iceberg-catalog-wrapper-cleaner-%d")
                            .build())))
            .build();
  }

  /**
   * @param rawPrefix The path parameter is passed by a Jetty handler. The pattern is matching
   *     ([^/]*\/), end with /
   * @return the instance of IcebergCatalogWrapper.
   */
  public IcebergCatalogWrapper getOps(String rawPrefix) {
    String catalogName = IcebergRestUtils.getCatalogName(rawPrefix);
    return getCatalogWrapper(catalogName);
  }

  public IcebergCatalogWrapper getCatalogWrapper(String catalogName) {
    IcebergCatalogWrapper catalogWrapper =
        icebergCatalogWrapperCache.get(catalogName, k -> createCatalogWrapper(catalogName));
    // Reload conf to reset UserGroupInformation or icebergTableOps will always use
    // Simple auth.
    catalogWrapper.reloadHadoopConf();
    return catalogWrapper;
  }

  public CredentialProvider getCredentialProvider(String catalogName) {
    return credentialProviderManager.getCredentialProvider(catalogName);
  }

  @VisibleForTesting
  protected IcebergCatalogWrapper createIcebergCatalogWrapper(IcebergConfig icebergConfig) {
    return new IcebergCatalogWrapper(icebergConfig);
  }

  private IcebergCatalogWrapper createCatalogWrapper(String catalogName) {
    Optional<IcebergConfig> icebergConfig = configProvider.getIcebergCatalogConfig(catalogName);
    if (!icebergConfig.isPresent()) {
      throw new RuntimeException("Couldn't find Iceberg configuration for " + catalogName);
    }

    IcebergConfig config = icebergConfig.get();
    String credentialProviderType = config.get(IcebergConfig.CREDENTIAL_PROVIDER_TYPE);
    if (StringUtils.isNotBlank(credentialProviderType)) {
      CredentialProvider credentialProvider =
          CredentialProviderFactory.create(credentialProviderType, config.getAllConfig());
      credentialProviderManager.registerCredentialProvider(catalogName, credentialProvider);
    }

    return createIcebergCatalogWrapper(icebergConfig.get());
  }

  private void closeIcebergCatalogWrapper(IcebergCatalogWrapper catalogWrapper) {
    try {
      catalogWrapper.close();
    } catch (Exception ex) {
      LOG.warn("Close Iceberg table catalog wrapper fail: {}, {}", catalogWrapper, ex);
    }
  }

  @Override
  public void close() throws Exception {
    icebergCatalogWrapperCache.invalidateAll();
  }
}
