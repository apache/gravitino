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
package org.apache.gravitino.iceberg.common.ops;

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
 */
public class ConfigBasedIcebergTableOpsProvider implements IcebergTableOpsProvider {
  public static final Logger LOG =
      LoggerFactory.getLogger(ConfigBasedIcebergTableOpsProvider.class);

  public static final String CONFIG_BASE_ICEBERG_TABLE_OPS_PROVIDER_NAME = "config-based-provider";

  @VisibleForTesting Map<String, IcebergConfig> catalogConfigs;

  @Override
  public void initialize(Map<String, String> properties) {
    this.catalogConfigs =
        properties.keySet().stream()
            .map(this::getCatalogName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .distinct()
            .collect(
                Collectors.toMap(
                    catalogName -> catalogName,
                    catalogName ->
                        new IcebergConfig(
                            MapUtils.getPrefixMap(
                                properties, String.format("catalog.%s.", catalogName)))));
    this.catalogConfigs.put(
        IcebergConstants.GRAVITINO_DEFAULT_CATALOG, new IcebergConfig(properties));
  }

  @Override
  public IcebergTableOps getIcebergTableOps(String catalogName) {
    IcebergConfig icebergConfig = this.catalogConfigs.get(catalogName);
    if (icebergConfig == null) {
      String errorMsg = String.format("%s can not match any catalog", catalogName);
      LOG.warn(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return new IcebergTableOps(icebergConfig);
  }

  private Optional<String> getCatalogName(String catalogConfigKey) {
    if (!catalogConfigKey.startsWith("catalog.")) {
      return Optional.empty();
    }
    // The catalogConfigKey's format is catalog.<catalog_name>.<param_name>
    if (catalogConfigKey.split("\\.").length < 3) {
      LOG.warn("{} format is illegal", catalogConfigKey);
      return Optional.empty();
    }
    return Optional.of(catalogConfigKey.split("\\.")[1]);
  }
}
