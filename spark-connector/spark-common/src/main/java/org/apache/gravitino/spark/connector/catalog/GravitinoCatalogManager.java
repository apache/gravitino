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
import java.util.Optional;
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
  private final String metalakeName;

  // Resolved lazily on first access and cached for the life of this manager; Spark catalogs are
  // initialized once, so there is no refresh path if the Iceberg REST endpoint changes later.
  private volatile Optional<String> icebergRestUri;

  private GravitinoCatalogManager(String metalakeName, Supplier<GravitinoClient> clientBuilder) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = clientBuilder.get();
    // Will not evict catalog by default
    this.gravitinoCatalogs = Caffeine.newBuilder().build();
  }

  public static GravitinoCatalogManager create(
      String metalakeName, Supplier<GravitinoClient> clientBuilder) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(metalakeName, clientBuilder);
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

  /**
   * Resolves the Gravitino Iceberg REST server endpoint for this manager's metalake, if the server
   * exposes one. The lookup is performed once and the result, including a lookup failure, is cached
   * for the lifetime of this manager.
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
    try {
      return gravitinoClient.icebergRestServiceUri(metalakeName);
    } catch (Exception e) {
      LOG.debug(
          "No Iceberg REST server endpoint is available for metalake {}, "
              + "falling back to native catalog backend routing.",
          metalakeName,
          e);
      return Optional.empty();
    }
  }

  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = gravitinoClient.loadCatalog(catalogName);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
