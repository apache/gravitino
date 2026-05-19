/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.provider;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads per-catalog {@link IcebergConfig} entries from Iceberg REST server properties.
 *
 * <p>Input is the flat property map loaded from {@code gravitino.conf} (for example {@code
 * gravitino.iceberg-rest.*}). Keys prefixed with {@code catalog.<name>.} become named catalog
 * entries. The full property map is also stored under {@link
 * IcebergConstants#ICEBERG_REST_DEFAULT_CATALOG} for server-wide defaults such as {@code
 * table-metadata-cache-*}.
 */
final class ServerCatalogConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ServerCatalogConfigLoader.class);
  private static final String CATALOG_PREFIX = "catalog.";

  private final Map<String, IcebergConfig> serverCatalogConfigs;

  private ServerCatalogConfigLoader(Map<String, IcebergConfig> serverCatalogConfigs) {
    this.serverCatalogConfigs = serverCatalogConfigs;
  }

  static ServerCatalogConfigLoader fromServerProperties(Map<String, String> properties) {
    return new ServerCatalogConfigLoader(parseServerCatalogConfigs(properties));
  }

  static Map<String, IcebergConfig> loadFromServerProperties(Map<String, String> properties) {
    return fromServerProperties(properties).serverCatalogConfigs;
  }

  /**
   * Merges Gravitino catalog properties with server-side defaults for the given catalog key.
   *
   * <p>Catalog properties take precedence; missing keys are filled from the server config entry
   * identified by {@code serverCatalogKey} (for example {@link
   * IcebergConstants#ICEBERG_REST_DEFAULT_CATALOG}).
   */
  IcebergConfig mergeWithServerCatalogConfig(
      Map<String, String> catalogProperties, String serverCatalogKey) {
    Map<String, String> merged =
        new HashMap<>(fromGravitinoCatalogProperties(catalogProperties).getAllConfig());
    IcebergConfig serverConfig = serverCatalogConfigs.get(serverCatalogKey);
    if (serverConfig != null) {
      mergeAbsentProperties(merged, serverConfig.getAllConfig());
    }
    return new IcebergConfig(merged);
  }

  @VisibleForTesting
  static IcebergConfig fromGravitinoCatalogProperties(Map<String, String> catalogProperties) {
    Map<String, String> properties = new HashMap<>();
    properties.putAll(MapUtils.getPrefixMap(catalogProperties, CATALOG_BYPASS_PREFIX));
    properties.putAll(catalogProperties);
    return new IcebergConfig(properties);
  }

  private static Map<String, IcebergConfig> parseServerCatalogConfigs(
      Map<String, String> properties) {
    Map<String, IcebergConfig> configs =
        properties.keySet().stream()
            .map(ServerCatalogConfigLoader::parseCatalogName)
            .flatMap(Optional::stream)
            .distinct()
            .collect(
                Collectors.toMap(
                    catalogName -> catalogName,
                    catalogName ->
                        new IcebergConfig(
                            MapUtils.getPrefixMap(
                                properties, String.format("%s%s.", CATALOG_PREFIX, catalogName)))));
    configs.put(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG, new IcebergConfig(properties));
    return configs;
  }

  static void mergeAbsentProperties(Map<String, String> target, Map<String, String> defaults) {
    defaults.forEach(target::putIfAbsent);
  }

  @VisibleForTesting
  static Optional<String> parseCatalogName(String catalogConfigKey) {
    if (!catalogConfigKey.startsWith(CATALOG_PREFIX)) {
      return Optional.empty();
    }
    int catalogNameStart = CATALOG_PREFIX.length();
    int catalogNameEnd = catalogConfigKey.indexOf('.', catalogNameStart);
    if (catalogNameEnd < 0) {
      LOG.warn("{} format is illegal", catalogConfigKey);
      return Optional.empty();
    }
    return Optional.of(catalogConfigKey.substring(catalogNameStart, catalogNameEnd));
  }
}
