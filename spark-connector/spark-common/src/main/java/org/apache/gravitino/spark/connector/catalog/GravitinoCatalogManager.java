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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.util.Arrays;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
>>>>>>> 5b666b322 ([#12709] improvement(spark-connector): Route lakehouse-iceberg catalogs through the Iceberg REST server (#12710))
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Apache Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private final Cache<String, Catalog> gravitinoCatalogs;
  private final GravitinoClient gravitinoClient;

<<<<<<< HEAD
  private GravitinoCatalogManager(Supplier<GravitinoClient> clientBuilder) {
    this.gravitinoClient = clientBuilder.get();
    // Will not evict catalog by default
    this.gravitinoCatalogs = Caffeine.newBuilder().build();
=======
  // Resolved lazily on first access and cached for the life of this manager; Spark catalogs are
  // initialized once, so there is no refresh path if the Iceberg REST endpoint changes later.
  private volatile Optional<String> icebergRestUri;

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
>>>>>>> 5b666b322 ([#12709] improvement(spark-connector): Route lakehouse-iceberg catalogs through the Iceberg REST server (#12710))
  }

  public static GravitinoCatalogManager create(Supplier<GravitinoClient> clientBuilder) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(clientBuilder);
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
    gravitinoClient.close();
    gravitinoCatalogManager = null;
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    try {
      return gravitinoCatalogs.get(name, catalogName -> loadCatalog(catalogName));
    } catch (Exception e) {
      LOG.error(String.format("Load catalog %s failed", name), e);
      throw new RuntimeException(e);
    }
  }

  public void loadRelationalCatalogs() {
    Catalog[] catalogs = gravitinoClient.listCatalogsInfo();
    Arrays.stream(catalogs)
        .filter(catalog -> Catalog.Type.RELATIONAL.equals(catalog.type()))
        .forEach(catalog -> gravitinoCatalogs.put(catalog.name(), catalog));
  }

  public Map<String, Catalog> getCatalogs() {
    return gravitinoCatalogs.asMap();
  }

<<<<<<< HEAD
  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = gravitinoClient.loadCatalog(catalogName);
=======
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

  /**
   * Resolves the Gravitino Iceberg REST server endpoint for this manager's metalake, if the server
   * exposes one. The lookup is performed once and a successful response, including a response with
   * no endpoint, is cached for the lifetime of this manager. Request failures are propagated.
   *
   * @return the discovered Iceberg REST endpoint, or empty if none is available
   */
  public Optional<String> getIcebergRestUri() {
    if (icebergRestUri == null) {
      synchronized (this) {
        if (icebergRestUri == null) {
          icebergRestUri = resolveIcebergRestUri();
        }
      }
    }
    return icebergRestUri;
  }

  private Optional<String> resolveIcebergRestUri() {
    String metalakeName = sparkConf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    return getClient(applicationIdentity()).icebergRestServiceUri(metalakeName);
  }

  private Catalog loadCatalog(GravitinoIdentity identity, String catalogName) {
    Catalog catalog = getClient(identity).loadCatalog(catalogName);
>>>>>>> 5b666b322 ([#12709] improvement(spark-connector): Route lakehouse-iceberg catalogs through the Iceberg REST server (#12710))
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
