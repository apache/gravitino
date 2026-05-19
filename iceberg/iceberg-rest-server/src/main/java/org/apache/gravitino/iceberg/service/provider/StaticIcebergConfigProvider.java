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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provider use configs to support multiple catalogs.
 *
 * <p>For example, there are two different catalogs: jdbc_proxy, hive_proxy The config is like:
 *
 * <p>gravitino.iceberg-rest.catalog.jdbc_proxy.catalog-backend = jdbc
 * gravitino.iceberg-rest.catalog.jdbc_proxy.uri = jdbc:mysql://{host}:{port}/{db} ...
 * gravitino.iceberg-rest.catalog.hive_proxy.catalog-backend = hive
 * gravitino.iceberg-rest.catalog.hive_proxy.uri = thrift://{host}:{port} ...
 *
 * <p>The full property map is also stored under {@link
 * IcebergConstants#ICEBERG_REST_DEFAULT_CATALOG} for server-wide defaults such as {@code
 * table-metadata-cache-*}.
 */
public class StaticIcebergConfigProvider implements IcebergConfigProvider {

  private static final Logger LOG = LoggerFactory.getLogger(StaticIcebergConfigProvider.class);
  private static final String CATALOG_PREFIX = "catalog.";

  @VisibleForTesting Map<String, IcebergConfig> catalogConfigs;

  @Override
  public void initialize(Map<String, String> properties) {
    this.catalogConfigs = initCatalogConfigs(properties);
  }

  @Override
  public Optional<IcebergConfig> getIcebergCatalogConfig(String catalogName) {
    return Optional.ofNullable(catalogConfigs.get(catalogName));
  }

  @Override
  public void close() {}

  /**
   * Loads per-catalog {@link IcebergConfig} entries from Iceberg REST server properties.
   *
   * <p>Keys prefixed with {@code catalog.<name>.} become named catalog entries.
   */
  @VisibleForTesting
  static Map<String, IcebergConfig> initCatalogConfigs(Map<String, String> properties) {
    Map<String, IcebergConfig> configs =
        properties.keySet().stream()
            .map(StaticIcebergConfigProvider::parseCatalogName)
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
