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
package org.apache.gravitino.flink.connector.utils;

import static org.apache.flink.table.factories.FactoryUtil.validateFactoryOptions;
import static org.apache.flink.table.factories.FactoryUtil.validateWatermarkOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.jdbc.GravitinoJdbcCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.paimon.GravitinoPaimonCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactoryUtils {

  private FactoryUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(FactoryUtils.class);

  /**
   * {@link GravitinoCatalogStoreFactoryOptions#GRAVITINO_EXCLUDED_CATALOGS} as seen in the session
   * configuration, where catalog store options carry the {@code table.catalog-store.gravitino.}
   * prefix.
   */
  private static final ConfigOption<List<String>> SESSION_EXCLUDED_CATALOGS =
      ConfigOptions.key(
              CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX
                  + GravitinoCatalogStoreFactoryOptions.GRAVITINO
                  + "."
                  + GravitinoCatalogStoreFactoryOptions.GRAVITINO_EXCLUDED_CATALOGS.key())
          .stringType()
          .asList()
          .noDefaultValue();

  /** The set of catalog type identifiers managed by Gravitino. */
  private static final ImmutableSet<String> GRAVITINO_CATALOG_TYPES =
      ImmutableSet.of(
          GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
          GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
          GravitinoJdbcCatalogFactoryOptions.MYSQL_IDENTIFIER,
          GravitinoJdbcCatalogFactoryOptions.POSTGRESQL_IDENTIFIER,
          GravitinoPaimonCatalogFactoryOptions.IDENTIFIER);

  public static boolean isGravitinoManagedCatalogType(String type) {
    return GRAVITINO_CATALOG_TYPES.contains(type) || isProvidedByCatalogFactory(type);
  }

  /**
   * Whether a {@link BaseCatalogFactory} on the classpath declares the type. This lets a jar
   * outside the connector contribute catalog types without registering them here.
   */
  private static boolean isProvidedByCatalogFactory(String type) {
    if (type == null) {
      return false;
    }
    return isProvidedByCatalogFactory(ServiceLoader.load(Factory.class).iterator(), type);
  }

  @VisibleForTesting
  static boolean isProvidedByCatalogFactory(Iterator<Factory> factories, String type) {
    while (true) {
      try {
        if (!factories.hasNext()) {
          return false;
        }
        Factory factory = factories.next();
        if (factory instanceof BaseCatalogFactory && type.equals(factory.factoryIdentifier())) {
          return true;
        }
      } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
        // A factory that cannot be loaded or fails to report its identifier cannot be the one
        // asked for.
        LOG.debug(
            "Skip a {} entry that cannot be loaded while resolving catalog type '{}'.",
            Factory.class.getName(),
            type,
            e);
      }
    }
  }

  /**
   * Utility for working with {@link Factory}s. The {@link GravitinoCatalogFactoryHelper} override
   * the {@link FactoryUtil.CatalogFactoryHelper#validate()} method to validate the options. For the
   * unconsumed option keys, it logs a warning instead of throwing an exception.
   */
  public static class GravitinoCatalogFactoryHelper extends FactoryUtil.CatalogFactoryHelper {

    private GravitinoCatalogFactoryHelper(
        CatalogFactory catalogFactory, CatalogFactory.Context context) {
      super(catalogFactory, context);
    }

    @Override
    public void validate() {
      validateFactoryOptions(factory, allOptions);
      ignoreUnconsumedKeys(
          factory.factoryIdentifier(),
          allOptions.keySet(),
          consumedOptionKeys,
          deprecatedOptionKeys);
      validateWatermarkOptions(factory.factoryIdentifier(), allOptions);
    }
  }

  /**
   * Validates unconsumed option keys. Logs a warning for each unconsumed option key instead of
   * throwing an exception.
   */
  private static void ignoreUnconsumedKeys(
      String factoryIdentifier,
      Set<String> allOptionKeys,
      Set<String> consumedOptionKeys,
      Set<String> deprecatedOptionKeys) {
    final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
    remainingOptionKeys.removeAll(consumedOptionKeys);
    if (!remainingOptionKeys.isEmpty()) {
      LOG.warn(
          "Unsupported options found for '{}'.\n\n"
              + "Unsupported options that will be ignored:\n\n"
              + "{}\n\n"
              + "Supported options:\n\n"
              + "{}",
          factoryIdentifier,
          remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
          consumedOptionKeys.stream()
              .map(
                  k -> {
                    if (deprecatedOptionKeys.contains(k)) {
                      return String.format("%s (deprecated)", k);
                    }
                    return k;
                  })
              .sorted()
              .collect(Collectors.joining("\n")));
    }
  }

  /**
   * Creates a utility that helps to validate options for a {@link CatalogFactory}.
   *
   * <p>Gravitino catalog factories must call this method before creating a catalog. It rejects
   * catalog names excluded from Gravitino by {@link
   * GravitinoCatalogStoreFactoryOptions#GRAVITINO_EXCLUDED_CATALOGS} in the current session.
   *
   * <p>Note: This utility checks for left-over options in the final step.
   *
   * @param factory The catalog factory to create the helper for
   * @param context The context containing the options to validate
   * @return A new CatalogFactoryHelper instance
   */
  public static FactoryUtil.CatalogFactoryHelper createCatalogFactoryHelper(
      CatalogFactory factory, CatalogFactory.Context context) {
    checkCatalogNameNotExcluded(factory, context);
    return new FactoryUtils.GravitinoCatalogFactoryHelper(factory, context);
  }

  /**
   * Rejects creating a Gravitino-managed catalog whose name is excluded from Gravitino in the
   * current session. Flink instantiates and registers a catalog before storing it in the catalog
   * store, so the check must happen here: a Gravitino-managed catalog resolves the Gravitino
   * catalog with the same name, which the exclusion must hide from the session.
   *
   * @param factory the Gravitino catalog factory
   * @param context the catalog factory context, whose configuration is the session configuration
   * @throws CatalogException if the catalog name is excluded from Gravitino
   */
  @VisibleForTesting
  static void checkCatalogNameNotExcluded(CatalogFactory factory, CatalogFactory.Context context) {
    ReadableConfig sessionConfiguration = context.getConfiguration();
    if (sessionConfiguration == null) {
      return;
    }
    List<String> excludedCatalogs =
        sessionConfiguration.getOptional(SESSION_EXCLUDED_CATALOGS).orElse(Collections.emptyList());
    String catalogName = context.getName();
    boolean excluded =
        excludedCatalogs.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .anyMatch(name -> name.equals(catalogName));
    if (excluded) {
      throw new CatalogException(
          String.format(
              "Cannot create Gravitino-managed catalog '%s' of type '%s': the catalog name is "
                  + "excluded from Gravitino by '%s', so only session catalogs can use it.",
              catalogName,
              factory.factoryIdentifier(),
              GravitinoCatalogStoreFactoryOptions.GRAVITINO_EXCLUDED_CATALOGS.key()));
    }
  }
}
