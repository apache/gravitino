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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.CredentialProviderFactory;
import org.apache.gravitino.credential.CredentialProviderManager;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogConfigProvider;
import org.apache.gravitino.iceberg.provider.ConfigBasedIcebergCatalogConfigProvider;
import org.apache.gravitino.iceberg.provider.GravitinoBasedIcebergCatalogConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWrapperManager implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperManager.class);

  private static final ImmutableMap<String, String> ICEBERG_CATALOG_CONFIG_PROVIDER_NAMES =
      ImmutableMap.of(
          ConfigBasedIcebergCatalogConfigProvider.CONFIG_BASE_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
          ConfigBasedIcebergCatalogConfigProvider.class.getCanonicalName(),
          GravitinoBasedIcebergCatalogConfigProvider
              .GRAVITINO_BASE_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
          GravitinoBasedIcebergCatalogConfigProvider.class.getCanonicalName());

  private final Cache<String, IcebergCatalogWrapper> icebergCatalogWrapperCache;

  private final IcebergCatalogConfigProvider provider;

  private CredentialProviderManager credentialProviderManager;

  public IcebergCatalogWrapperManager(Map<String, String> properties) {
    this.credentialProviderManager = new CredentialProviderManager();
    this.provider = createProvider(properties);
    this.provider.initialize(properties);
    this.icebergCatalogWrapperCache =
        Caffeine.newBuilder()
            .expireAfterWrite(
                (new IcebergConfig(properties))
                    .get(IcebergConfig.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL),
                TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  String catalogName = (String)k;
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
    String catalogName = getCatalogName(rawPrefix);
    IcebergCatalogWrapper tableOps =
        icebergCatalogWrapperCache.get(catalogName, k -> createCatalogWrapper(catalogName));
    // Reload conf to reset UserGroupInformation or icebergTableOps will always use
    // Simple auth.
    tableOps.reloadHadoopConf();
    return tableOps;
  }

  public CredentialProvider getCredentialProvider(String prefix) {
    String catalogName = getCatalogName(prefix);
    return credentialProviderManager.getCredentialProvider(catalogName);
  }

  private IcebergCatalogWrapper createCatalogWrapper(String catalogName) {
    Optional<IcebergConfig> icebergConfig = provider.getIcebergCatalogConfig(catalogName);
    if (!icebergConfig.isPresent()) {
      throw new RuntimeException("Couldn't find Iceberg configuration for " + catalogName);
    }

    IcebergConfig config = icebergConfig.get();
    String credentialProviderType = config.get(IcebergConfig.CREDENTIAL_PROVIDER_TYPE);
    if (StringUtils.isNotBlank(credentialProviderType)) {
      CredentialProvider credentialProvider = CredentialProviderFactory.create(credentialProviderType, config.getAllConfig());
      credentialProviderManager.registerCredentialProvider(catalogName, credentialProvider);
    }

    IcebergCatalogWrapper catalogWrapper = new IcebergCatalogWrapper(icebergConfig.get());
    return catalogWrapper;
  }

  private String getCatalogName(String rawPrefix) {
    String prefix = shelling(rawPrefix);
    Preconditions.checkArgument(
        !IcebergConstants.GRAVITINO_DEFAULT_CATALOG.equals(prefix),
        String.format("%s is conflict with reserved key, please replace it", prefix));
    if (StringUtils.isBlank(prefix)) {
      return IcebergConstants.GRAVITINO_DEFAULT_CATALOG;
    }
    return prefix;
  }

  private IcebergCatalogConfigProvider createProvider(Map<String, String> properties) {
    String providerName =
        (new IcebergConfig(properties)).get(IcebergConfig.ICEBERG_REST_CATALOG_PROVIDER);
    String className = ICEBERG_CATALOG_CONFIG_PROVIDER_NAMES.getOrDefault(providerName, providerName);
    LOG.info("Load Iceberg catalog provider: {}.", className);
    try {
      Class<?> providerClz = Class.forName(className);
      return (IcebergCatalogConfigProvider) providerClz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String shelling(String rawPrefix) {
    if (StringUtils.isBlank(rawPrefix)) {
      return rawPrefix;
    } else {
      // rawPrefix is a string matching ([^/]*/) which end with /
      Preconditions.checkArgument(
          rawPrefix.endsWith("/"), String.format("rawPrefix %s format is illegal", rawPrefix));
      return rawPrefix.substring(0, rawPrefix.length() - 1);
    }
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
    if (provider instanceof AutoCloseable) {
      ((AutoCloseable) provider).close();
    }
  }
}
