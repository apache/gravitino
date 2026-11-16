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

package org.apache.gravitino.flink.connector.store;

import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_CLIENT_CONFIG;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_EXCLUDED_CATALOGS;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_METALAKE;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Factory for creating {@link GravitinoCatalogStore}. */
public class GravitinoCatalogStoreFactory implements CatalogStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogStoreFactory.class);

  private GravitinoCatalogManager catalogManager;
  private boolean enableSessionCatalogSupport;
  private GravitinoCatalogStore gravitinoCatalogStore;
  private GenericInMemoryCatalogStore memoryCatalogStore;
  private CatalogStore catalogStore;

  @Override
  public CatalogStore createCatalogStore() {
    return catalogStore;
  }

  @Override
  public void open(Context context) throws CatalogException {
    FactoryUtil.FactoryHelper<CatalogStoreFactory> factoryHelper =
        createCatalogStoreFactoryHelper(this, context);
    factoryHelper.validate();

    ReadableConfig options = factoryHelper.getOptions();

    String gravitinoUri = options.get(GRAVITINO_URI);
    String gravitinoName = options.get(GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        gravitinoUri != null && gravitinoName != null,
        "Both %s and %s must be set",
        GRAVITINO_URI.key(),
        GRAVITINO_METALAKE.key());

    boolean sessionCatalogSupport = options.get(GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT);
    // Validate before creating the Gravitino client so that a misconfiguration does not leave an
    // unused client behind.
    Set<String> excludedCatalogs =
        parseExcludedCatalogs(options.get(GRAVITINO_EXCLUDED_CATALOGS), sessionCatalogSupport);

    this.catalogManager =
        GravitinoCatalogManager.create(gravitinoUri, gravitinoName, extractClientConfig(options));
    this.enableSessionCatalogSupport = sessionCatalogSupport;
    this.gravitinoCatalogStore = new GravitinoCatalogStore(catalogManager);
    if (enableSessionCatalogSupport) {
      this.memoryCatalogStore = new GenericInMemoryCatalogStore();
      this.memoryCatalogStore.open();
      if (!excludedCatalogs.isEmpty()) {
        LOG.info(
            "Catalogs {} are excluded from Gravitino in this session; only session catalogs "
                + "can use these names.",
            excludedCatalogs);
      }
      this.catalogStore =
          new GravitinoSessionCatalogStore(
              gravitinoCatalogStore, memoryCatalogStore, excludedCatalogs);
    } else {
      this.catalogStore = gravitinoCatalogStore;
    }
  }

  @Override
  public void close() throws CatalogException {
    if (catalogManager != null) {
      catalogManager.close();
    }
    if (memoryCatalogStore != null) {
      memoryCatalogStore.close();
    }
  }

  @Override
  public String factoryIdentifier() {
    return GRAVITINO;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(GRAVITINO_METALAKE, GRAVITINO_URI);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        GRAVITINO_CLIENT_CONFIG,
        GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT,
        GRAVITINO_EXCLUDED_CATALOGS);
  }

  /**
   * Validates and normalizes the configured excluded catalog names.
   *
   * @param catalogNames the configured catalog names
   * @param sessionCatalogSupport whether session catalog support is enabled
   * @return the trimmed, de-duplicated catalog names
   * @throws IllegalArgumentException if a name is blank, or names are configured while session
   *     catalog support is disabled
   */
  @VisibleForTesting
  static Set<String> parseExcludedCatalogs(
      List<String> catalogNames, boolean sessionCatalogSupport) {
    if (catalogNames == null || catalogNames.isEmpty()) {
      return ImmutableSet.of();
    }
    Preconditions.checkArgument(
        sessionCatalogSupport,
        "%s can only be set when %s is true",
        GRAVITINO_EXCLUDED_CATALOGS.key(),
        GRAVITINO_ENABLE_SESSION_CATALOG_SUPPORT.key());
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String catalogName : catalogNames) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(catalogName),
          "%s must not contain blank catalog names, but got %s",
          GRAVITINO_EXCLUDED_CATALOGS.key(),
          catalogNames);
      builder.add(catalogName.trim());
    }
    return builder.build();
  }

  @VisibleForTesting
  static Map<String, String> extractClientConfig(ReadableConfig options) {
    return options.get(GRAVITINO_CLIENT_CONFIG).entrySet().stream()
        .collect(
            Collectors.toMap(
                entry ->
                    GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + entry.getKey(),
                Map.Entry::getValue,
                (oldVal, newVal) -> newVal));
  }
}
