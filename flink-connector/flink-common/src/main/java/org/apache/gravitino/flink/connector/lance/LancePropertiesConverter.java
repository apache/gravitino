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

package org.apache.gravitino.flink.connector.lance;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;

/**
 * Converter between Gravitino Lance catalog and table properties and Apache Flink properties.
 */
public class LancePropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {

  /** Singleton instance of {@link LancePropertiesConverter}. */
  public static final LancePropertiesConverter INSTANCE = new LancePropertiesConverter();

  private static final Set<String> PASSTHROUGH_CATALOG_KEYS =
      ImmutableSet.of(
          LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_URI,
          LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_BACKEND,
          LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_IMPL,
          LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_WAREHOUSE,
          LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_LOCATION);

  private LancePropertiesConverter() {}

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    if (PASSTHROUGH_CATALOG_KEYS.contains(configKey)
        || configKey.startsWith(LancePropertiesConstants.LANCE_STORAGE_PREFIX)) {
      return configKey;
    }
    return null;
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    if (PASSTHROUGH_CATALOG_KEYS.contains(configKey)
        || configKey.startsWith(LancePropertiesConstants.LANCE_STORAGE_PREFIX)) {
      return configKey;
    }
    return null;
  }

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkProperties =
        CatalogPropertiesConverter.super.toFlinkCatalogProperties(gravitinoProperties);
    flinkProperties.putIfAbsent(
        LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_BACKEND,
        LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST);
    flinkProperties.putIfAbsent(
        LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_IMPL,
        LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST);
    return flinkProperties;
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    Map<String, String> tableProperties =
        new HashMap<>(
            gravitinoTableProperties != null ? gravitinoTableProperties : Collections.emptyMap());

    String location = tableProperties.get(LancePropertiesConstants.LANCE_TABLE_LOCATION);
    if (StringUtils.isBlank(location)) {
      location = tableProperties.get(LancePropertiesConstants.LANCE_TABLE_PATH);
    }
    if (StringUtils.isBlank(location) && flinkCatalogProperties != null) {
      String baseLocation =
          flinkCatalogProperties.get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_WAREHOUSE);
      if (StringUtils.isBlank(baseLocation)) {
        baseLocation =
            flinkCatalogProperties.get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_LOCATION);
      }
      if (StringUtils.isNotBlank(baseLocation) && tablePath != null) {
        location =
            StringUtils.stripEnd(baseLocation, "/")
                + "/"
                + tablePath.getDatabaseName()
                + "/"
                + tablePath.getObjectName();
      }
    }

    if (StringUtils.isNotBlank(location)) {
      tableProperties.put(LancePropertiesConstants.LANCE_TABLE_PATH, location);
      tableProperties.put(LancePropertiesConstants.LANCE_TABLE_LOCATION, location);
    }

    tableProperties.put(
        LancePropertiesConstants.FLINK_CONNECTOR, LancePropertiesConstants.LANCE_CONNECTOR_TYPE);

    if (flinkCatalogProperties != null) {
      for (Map.Entry<String, String> entry : flinkCatalogProperties.entrySet()) {
        if (entry.getKey().startsWith(LancePropertiesConstants.LANCE_STORAGE_PREFIX)) {
          tableProperties.putIfAbsent(entry.getKey(), entry.getValue());
        }
      }
    }

    return tableProperties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    Map<String, String> properties =
        new HashMap<>(flinkProperties != null ? flinkProperties : Collections.emptyMap());
    String path = properties.get(LancePropertiesConstants.LANCE_TABLE_PATH);
    if (StringUtils.isNotBlank(path)
        && !properties.containsKey(LancePropertiesConstants.LANCE_TABLE_LOCATION)) {
      properties.put(LancePropertiesConstants.LANCE_TABLE_LOCATION, path);
    }
    properties.putIfAbsent(
        LancePropertiesConstants.LANCE_TABLE_FORMAT, LancePropertiesConstants.LANCE_TABLE_FORMAT);
    return properties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoLanceCatalogFactoryOptions.IDENTIFIER;
  }
}
