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
package org.apache.gravitino.iceberg.service;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache wrapper that validates cached entries against the current configuration. When the config
 * provider can check config changes efficiently, it compares the cached wrapper's config with the
 * current config. If the config has changed, the stale entry is invalidated before returning.
 */
public class ValidatingCache {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatingCache.class);

  private final Cache<String, CatalogWrapperForREST> cache;
  private final IcebergConfigProvider configProvider;

  public ValidatingCache(
      Cache<String, CatalogWrapperForREST> cache, IcebergConfigProvider configProvider) {
    this.cache = cache;
    this.configProvider = configProvider;
  }

  /**
   * Gets the value from the cache, validating it first if the provider can check config changes. If
   * the cached entry is stale (config has changed), it will be invalidated and a new entry will be
   * created.
   *
   * @param key The cache key (catalog name).
   * @param mappingFunction The function to create a new value if not present or invalidated.
   * @return The cached or newly created CatalogWrapperForREST.
   */
  public CatalogWrapperForREST get(
      String key, Function<String, CatalogWrapperForREST> mappingFunction) {
    validateCachedEntry(key);
    return cache.get(key, mappingFunction);
  }

  /** Invalidates all entries in the cache. */
  public void invalidateAll() {
    cache.invalidateAll();
  }

  /**
   * Validates if the cached entry is still valid. When the provider can check config changes
   * efficiently, checks if the cached config matches the current config. If not, invalidates the
   * cache entry.
   */
  private void validateCachedEntry(String catalogName) {
    if (!configProvider.canCheckConfigChange()) {
      return;
    }

    CatalogWrapperForREST cachedWrapper = cache.getIfPresent(catalogName);
    if (cachedWrapper == null) {
      return;
    }

    Optional<IcebergConfig> currentConfig = configProvider.getIcebergCatalogConfig(catalogName);
    if (currentConfig.isEmpty()) {
      return;
    }

    if (!isSameConfig(cachedWrapper.getIcebergConfig(), currentConfig.get())) {
      LOG.info(
          "Catalog {} config has changed, invalidating cache and creating new wrapper.",
          catalogName);
      cache.invalidate(catalogName);
    }
  }

  private boolean isSameConfig(IcebergConfig cachedConfig, IcebergConfig currentConfig) {
    return cachedConfig.getAllConfig().equals(currentConfig.getAllConfig());
  }
}
