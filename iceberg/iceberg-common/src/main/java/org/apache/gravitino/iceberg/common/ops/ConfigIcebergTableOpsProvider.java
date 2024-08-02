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

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
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
public class ConfigIcebergTableOpsProvider implements IcebergTableOpsProvider {
  public static final Logger LOG = LoggerFactory.getLogger(ConfigIcebergTableOpsProvider.class);

  private Map<String, String> properties;

  private List<String> catalogNames;

  @Override
  public void initialize(Map<String, String> properties) {
    Map<String, Boolean> catalogs = Maps.newHashMap();
    for (String key : properties.keySet()) {
      if (!key.startsWith("catalog.")) {
        continue;
      }
      if (key.split("\\.").length < 3) {
        throw new RuntimeException(String.format("%s format is illegal", key));
      }
      catalogs.put(key.split("\\.")[1], true);
    }
    this.catalogNames = catalogs.keySet().stream().sorted().collect(Collectors.toList());
    this.properties = properties;
  }

  @Override
  public IcebergTableOps getIcebergTableOps(String prefix) {
    if (StringUtils.isBlank(prefix)) {
      return new IcebergTableOps(new IcebergConfig(properties));
    }
    if (!catalogNames.contains(prefix)) {
      String errorMsg = String.format("%s can not match any catalog", prefix);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return new IcebergTableOps(getCatalogConfig(prefix));
  }

  private IcebergConfig getCatalogConfig(String catalog) {
    Map<String, String> base = Maps.newHashMap(this.properties);
    Map<String, String> merge =
        MapUtils.getPrefixMap(this.properties, String.format("catalog.%s.", catalog));
    for (String key : merge.keySet()) {
      base.put(key, merge.get(key));
    }
    return new IcebergConfig(base);
  }
}
