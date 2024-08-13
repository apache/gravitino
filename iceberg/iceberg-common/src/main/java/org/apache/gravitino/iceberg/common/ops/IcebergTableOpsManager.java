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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOpsManager implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergTableOpsManager.class);

  private static final ImmutableMap<String, String> ICEBERG_TABLE_OPS_PROVIDER_NAMES =
      ImmutableMap.of(
          ConfigBasedIcebergTableOpsProvider.CONFIG_BASE_ICEBERG_TABLE_OPS_PROVIDER_NAME,
          ConfigBasedIcebergTableOpsProvider.class.getCanonicalName());

  private final Cache<String, IcebergTableOps> icebergTableOpsCache;

  private final IcebergTableOpsProvider provider;

  public IcebergTableOpsManager(Map<String, String> properties) {
    this.icebergTableOpsCache = Caffeine.newBuilder().build();
    this.provider = createProvider(properties);
    this.provider.initialize(properties);
  }

  /**
   * @param rawPrefix The path parameter is passed by a Jetty handler. The pattern is matching
   *     ([^/]*\/), end with /
   * @return the instance of IcebergTableOps.
   */
  public IcebergTableOps getOps(String rawPrefix) {
    String catalogName = getCatalogName(rawPrefix);
    return icebergTableOpsCache.get(catalogName, k -> provider.getIcebergTableOps(catalogName));
  }

  private String getCatalogName(String rawPrefix) {
    String prefix = shelling(rawPrefix);
    Preconditions.checkArgument(
        !IcebergConstants.GRAVITINO_DEFAULT_CATALOG.equals(prefix),
        String.format("%s is conflict with reserved key, please replace it", prefix));
    if (StringUtils.isBlank(prefix)) {
      return IcebergConstants.GRAVITINO_DEFAULT_CATALOG;
    }
    return prefix;
  }

  private IcebergTableOpsProvider createProvider(Map<String, String> properties) {
    String providerName =
        (new IcebergConfig(properties)).get(IcebergConfig.ICEBERG_REST_CATALOG_PROVIDER);
    String className = ICEBERG_TABLE_OPS_PROVIDER_NAMES.getOrDefault(providerName, providerName);
    LOG.info("Load Iceberg catalog provider: {}.", className);
    try {
      Class<?> providerClz = Class.forName(className);
      return (IcebergTableOpsProvider) providerClz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String shelling(String rawPrefix) {
    if (StringUtils.isBlank(rawPrefix)) {
      return rawPrefix;
    } else {
      // rawPrefix is a string matching ([^/]*/) which end with /
      Preconditions.checkArgument(
          rawPrefix.endsWith("/"), String.format("rawPrefix %s format is illegal", rawPrefix));
      return rawPrefix.substring(0, rawPrefix.length() - 1);
    }
  }

  @Override
  public void close() throws Exception {
    icebergTableOpsCache.invalidateAll();
  }
}
