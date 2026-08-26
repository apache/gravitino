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
package org.apache.gravitino.spark.connector.catalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoCatalogManager is used to retrieve catalogs from Apache Gravitino server.
 *
 * <p>The manager itself stays a JVM-wide singleton, but the clients inside it are cached per {@link
 * GravitinoIdentity} so that a shared Spark driver does not serve one user's catalog metadata to
 * another. Every auth type other than {@code token} resolves to a single application-wide identity,
 * which keeps those deployments behaving exactly as before.
 */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private final SparkConf sparkConf;
  private final String authType;
  private final List<String> principalFields;
  private final Function<GravitinoIdentity, GravitinoClient> clientBuilder;
  private final Cache<GravitinoIdentity, GravitinoClient> clients;
  private final Cache<String, Catalog> gravitinoCatalogs;
  private volatile Map<String, Catalog> applicationCatalogs = ImmutableMap.of();

  private GravitinoCatalogManager(
      SparkConf sparkConf, Function<GravitinoIdentity, GravitinoClient> clientBuilder) {
    this.sparkConf = sparkConf;
    this.clientBuilder = clientBuilder;
    this.authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    this.principalFields =
        Arrays.stream(
                sparkConf
                    .get(
                        GravitinoSparkConfig.GRAVITINO_TOKEN_PRINCIPAL_FIELDS,
                        GravitinoSparkConfig.GRAVITINO_TOKEN_PRINCIPAL_FIELDS_DEFAULT)
                    .split(ConnectorConstants.COMMA))
            .map(String::trim)
            .filter(field -> !field.isEmpty())
            .collect(ImmutableList.toImmutableList());
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(
                sparkConf.getInt(
                    GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_MAX_SIZE,
                    GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_MAX_SIZE_DEFAULT))
            .expireAfterAccess(
                Duration.ofSeconds(
                    sparkConf.getLong(
                        GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_TTL_SEC,
                        GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_TTL_SEC_DEFAULT)))
            // An evicted client still owns an HTTP connection pool, so it must be closed.
            .<GravitinoIdentity, GravitinoClient>removalListener(
                (identity, client, cause) -> closeClient(identity, client))
            .build();
    this.gravitinoCatalogs =
        Caffeine.newBuilder()
            // A permission revoked in Gravitino must eventually stop being served from cache.
            .expireAfterWrite(
                Duration.ofSeconds(
                    sparkConf.getLong(
                        GravitinoSparkConfig.GRAVITINO_CATALOG_CACHE_TTL_SEC,
                        GravitinoSparkConfig.GRAVITINO_CATALOG_CACHE_TTL_SEC_DEFAULT)))
            .build();
  }

  /**
   * Creates the singleton GravitinoCatalogManager.
   *
   * @param sparkConf the application Spark configuration, used to read the cache settings and, in
   *     {@code token} mode, to resolve the bearer token of the current request
   * @param applicationUser the user the Spark application runs as, recorded for diagnostics only
   *     and never used to derive an identity
   * @param clientBuilder builds a Gravitino client for a given identity
   * @return the created GravitinoCatalogManager
   */
  public static GravitinoCatalogManager create(
      SparkConf sparkConf,
      String applicationUser,
      Function<GravitinoIdentity, GravitinoClient> clientBuilder) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(sparkConf, clientBuilder);
    LOG.info(
        "Created GravitinoCatalogManager for Spark user {} with auth type {}.",
        applicationUser,
        gravitinoCatalogManager.authType);
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed, "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  public void close() {
    Preconditions.checkState(!isClosed, "Gravitino Catalog is already closed");
    isClosed = true;
    // Caffeine dispatches the removal listener asynchronously, so shutdown closes explicitly.
    clients.asMap().forEach(GravitinoCatalogManager::closeClient);
    clients.invalidateAll();
    gravitinoCatalogs.invalidateAll();
    applicationCatalogs = ImmutableMap.of();
    gravitinoCatalogManager = null;
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    GravitinoIdentity identity = currentIdentity();
    try {
      return gravitinoCatalogs.get(cacheKey(identity, name), key -> loadCatalog(identity, name));
    } catch (Exception e) {
      LOG.error(String.format("Load catalog %s failed", name), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the relational catalogs visible to the application identity. This runs at driver init,
   * before any Spark session exists, so it never consults session state.
   */
  public void loadRelationalCatalogs() {
    GravitinoIdentity identity = applicationIdentity();
    Catalog[] catalogs = getClient(identity).listCatalogsInfo();
    Map<String, Catalog> relationalCatalogs =
        Arrays.stream(catalogs)
            .filter(catalog -> Catalog.Type.RELATIONAL.equals(catalog.type()))
            .collect(
                Collectors.toMap(Catalog::name, catalog -> catalog, (first, second) -> second));
    relationalCatalogs.forEach(
        (name, catalog) -> gravitinoCatalogs.put(cacheKey(identity, name), catalog));
    this.applicationCatalogs = ImmutableMap.copyOf(relationalCatalogs);
  }

  /**
   * Returns the catalogs loaded by {@link #loadRelationalCatalogs()}, that is, the catalogs the
   * application identity can see.
   *
   * @return the catalogs registered at driver startup, keyed by catalog name
   */
  public Map<String, Catalog> getCatalogs() {
    return applicationCatalogs;
  }

  /**
   * Resolves the identity of the caller. Outside {@code token} mode this is the application
   * identity and no session state is consulted at all. In {@code token} mode the bearer token is
   * read exactly as the request-time provider reads it, active session configuration first and
   * application configuration second, and the identity is derived from that token alone.
   *
   * @return the identity of the current caller
   */
  @VisibleForTesting
  GravitinoIdentity currentIdentity() {
    if (!AuthProperties.isToken(authType)) {
      return applicationIdentity();
    }
    return GravitinoIdentity.fromToken(
        GravitinoDriverPlugin.resolveToken(sparkConf), principalFields);
  }

  @VisibleForTesting
  GravitinoClient getClient(GravitinoIdentity identity) {
    return clients.get(identity, clientBuilder);
  }

  private GravitinoIdentity applicationIdentity() {
    return GravitinoIdentity.application(authType);
  }

  private Catalog loadCatalog(GravitinoIdentity identity, String catalogName) {
    Catalog catalog = getClient(identity).loadCatalog(catalogName);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully for {}.", catalogName, identity);
    return catalog;
  }

  private static String cacheKey(GravitinoIdentity identity, String catalogName) {
    return identity.key() + ":" + catalogName;
  }

  private static void closeClient(GravitinoIdentity identity, GravitinoClient client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception e) {
      LOG.warn("Failed to close the Gravitino client of {}.", identity, e);
    }
  }
}
