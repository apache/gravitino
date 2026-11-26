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
package org.apache.gravitino.paimon.service.provider;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager;

/**
 * Static configuration provider. The default catalog ({@value
 * PaimonCatalogWrapperManager#DEFAULT_CATALOG_NAME}) is built from the top-level service
 * configuration keys, and additional catalogs can be declared with {@code catalog.<name>.<key>}
 * entries.
 */
public class StaticPaimonConfigProvider implements PaimonConfigProvider {

  private static final String CATALOG_CONFIG_PREFIX = "catalog.";

  private final Map<String, Map<String, String>> catalogConfigs = Maps.newHashMap();

  @Override
  public void initialize(Map<String, String> properties) {
    Map<String, String> defaultConfig = Maps.newHashMap(properties);
    Map<String, Map<String, String>> grouped = Maps.newHashMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(CATALOG_CONFIG_PREFIX)) {
        String remaining = key.substring(CATALOG_CONFIG_PREFIX.length());
        int dot = remaining.indexOf('.');
        if (dot > 0) {
          String catalogName = remaining.substring(0, dot);
          grouped
              .computeIfAbsent(catalogName, name -> Maps.newHashMap())
              .put(remaining.substring(dot + 1), entry.getValue());
          // Per-catalog keys are not part of the default catalog's own configuration.
          defaultConfig.remove(key);
        }
      }
    }
    catalogConfigs.put(PaimonCatalogWrapperManager.DEFAULT_CATALOG_NAME, defaultConfig);
    catalogConfigs.putAll(grouped);
  }

  @Override
  public Optional<CatalogConfig> getCatalogConfig(String catalogName) {
    return Optional.ofNullable(catalogConfigs.get(catalogName))
        .map(properties -> new CatalogConfig(properties));
  }

  @Override
  public Set<String> catalogNames() {
    return Sets.newHashSet(catalogConfigs.keySet());
  }

  @Override
  public void close() {}
}
