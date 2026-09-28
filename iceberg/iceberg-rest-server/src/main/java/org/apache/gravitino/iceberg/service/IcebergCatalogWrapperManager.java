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
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.KerberosAwareIcebergCatalogProxy;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogWrapperManager implements AutoCloseable {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogWrapperManager.class);

  /**
   * Evict a cached catalog this long before its minted GCS OAuth2 token expires, so the next
   * request recreates the catalog and refreshes the token.
   */
  @VisibleForTesting static final long GCS_TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);

  private final Cache<String, CatalogWrapperForREST> catalogWrapperCache;

  private final IcebergConfigProvider configProvider;

  public IcebergCatalogWrapperManager(
      Map<String, String> properties,
      IcebergConfigProvider configProvider,
      boolean auxMode,
      String metalakeName) {
    this.configProvider = configProvider;
    long accessEvictionNanos =
        TimeUnit.MILLISECONDS.toNanos(
            new IcebergConfig(properties)
                .get(IcebergConfig.ICEBERG_REST_CATALOG_CACHE_EVICTION_INTERVAL));
    this.catalogWrapperCache =
        Caffeine.newBuilder()
            .expireAfter(new CatalogWrapperExpiry(accessEvictionNanos))
            .removalListener(
                (catalogName, catalogWrapper, cause) -> {
                  LOG.debug(
                      "Removing IcebergCatalogWrapper from cache: catalog={}, cause={}",
                      catalogName,
                      cause);
                  closeIcebergCatalogWrapper(catalogWrapper);
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

  public CatalogWrapperForREST getCatalogWrapper(String catalogName) {
    if (LOG.isDebugEnabled()) {
      boolean cacheHit = catalogWrapperCache.getIfPresent(catalogName) != null;
      LOG.debug("getCatalogWrapper catalogName={} cacheHit={}", catalogName, cacheHit);
    }
    CatalogWrapperForREST catalogWrapperForREST =
        catalogWrapperCache.get(catalogName, k -> createCatalogWrapper(catalogName));
    // Reload conf to reset UserGroupInformation or icebergTableOps will always use
    // Simple auth.
    catalogWrapperForREST.reloadHadoopConf();
    return catalogWrapperForREST;
  }

  private CatalogWrapperForREST createCatalogWrapper(String catalogName) {
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
    // Mint GCS OAuth2 tokens into the config before constructing the wrapper so the IRC catalog
    // cache can expire the entry before gcs.oauth2.token-expires-at.
    IcebergConfig enrichedConfig =
        IcebergCatalogUtil.withGcsServiceAccountCredentials(icebergConfig);
    // When the backend is a federated Iceberg REST catalog, use FederatedCatalogWrapper so
    // federation-aware behavior (FileIO property extraction, remote credential vending, remote
    // /v1/config defaults) is applied through polymorphic dispatch rather than scattered
    // instanceof checks. All other backends use the base CatalogWrapperForREST.
    IcebergCatalogBackend backend =
        IcebergCatalogBackend.valueOf(
            enrichedConfig.get(IcebergConfig.CATALOG_BACKEND).toUpperCase(Locale.ROOT));
    CatalogWrapperForREST rest =
        backend == IcebergCatalogBackend.REST
            ? new FederatedCatalogWrapper(catalogName, enrichedConfig)
            : new CatalogWrapperForREST(catalogName, enrichedConfig);
    AuthenticationConfig authenticationConfig =
        new AuthenticationConfig(enrichedConfig.getAllConfig());
    if (authenticationConfig.isKerberosAuth() && rest.getCatalog() instanceof SupportsKerberos) {
      return (CatalogWrapperForREST)
          new KerberosAwareIcebergCatalogProxy(rest).getProxy(catalogName, enrichedConfig);
    }

    return rest;
  }

  private void closeIcebergCatalogWrapper(IcebergCatalogWrapper catalogWrapper) {
    try {
      catalogWrapper.close();
    } catch (Exception ex) {
      LOG.warn("Close Iceberg table catalog wrapper fail: {}, {}", catalogWrapper, ex);
    }
  }

  /**
   * Computes how long a catalog wrapper may stay in the IRC cache.
   *
   * <p>Uses the configured access-based eviction interval, capped by the time until a minted GCS
   * OAuth2 token should be refreshed ({@code gcs.oauth2.token-expires-at} minus {@link
   * #GCS_TOKEN_REFRESH_BUFFER_MS}). When no token expiry is present, returns {@code
   * accessEvictionNanos}.
   *
   * @param config catalog config that may contain {@code gcs.oauth2.token-expires-at}
   * @param accessEvictionNanos default expire-after-access duration in nanoseconds
   * @param nowEpochMillis current wall-clock time
   * @return cache duration in nanoseconds; {@code 0} means expire immediately
   */
  @VisibleForTesting
  static long computeCacheDurationNanos(
      IcebergConfig config, long accessEvictionNanos, long nowEpochMillis) {
    String expiresAt =
        config.getAllConfig().get(IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT);
    if (StringUtils.isBlank(expiresAt)) {
      return accessEvictionNanos;
    }

    long expiresAtMs;
    try {
      expiresAtMs = Long.parseLong(expiresAt);
    } catch (NumberFormatException e) {
      LOG.warn("Invalid {}: {}", IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT, expiresAt);
      return accessEvictionNanos;
    }

    long remainingMs = expiresAtMs - GCS_TOKEN_REFRESH_BUFFER_MS - nowEpochMillis;
    if (remainingMs <= 0) {
      return 0L;
    }
    return Math.min(accessEvictionNanos, TimeUnit.MILLISECONDS.toNanos(remainingMs));
  }

  @Override
  public void close() throws Exception {
    catalogWrapperCache.invalidateAll();
  }

  private static final class CatalogWrapperExpiry implements Expiry<String, CatalogWrapperForREST> {

    private final long accessEvictionNanos;

    CatalogWrapperExpiry(long accessEvictionNanos) {
      this.accessEvictionNanos = accessEvictionNanos;
    }

    @Override
    public long expireAfterCreate(String key, CatalogWrapperForREST value, long currentTime) {
      return computeCacheDurationNanos(
          value.getIcebergConfig(), accessEvictionNanos, System.currentTimeMillis());
    }

    @Override
    public long expireAfterUpdate(
        String key, CatalogWrapperForREST value, long currentTime, long currentDuration) {
      return expireAfterCreate(key, value, currentTime);
    }

    @Override
    public long expireAfterRead(
        String key, CatalogWrapperForREST value, long currentTime, long currentDuration) {
      // Preserve expire-after-access, but never extend past the GCS token refresh deadline.
      return expireAfterCreate(key, value, currentTime);
    }
  }
}
