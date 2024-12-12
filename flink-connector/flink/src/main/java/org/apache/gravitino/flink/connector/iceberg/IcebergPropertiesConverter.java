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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.flink.connector.PropertiesConverter;

public class IcebergPropertiesConverter implements PropertiesConverter {
  public static IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();

  private IcebergPropertiesConverter() {}

  private static final Map<String, String> GRAVITINO_CONFIG_TO_FLINK_ICEBERG =
      ImmutableMap.of(
          IcebergConstants.CATALOG_BACKEND, IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE);

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Preconditions.checkArgument(
        gravitinoProperties != null, "Iceberg Catalog properties should not be null.");

    Map<String, String> all = new HashMap<>();
    if (gravitinoProperties != null) {
      gravitinoProperties.forEach(
          (k, v) -> {
            if (k.startsWith(FLINK_PROPERTY_PREFIX)) {
              String newKey = k.substring(FLINK_PROPERTY_PREFIX.length());
              all.put(newKey, v);
            }
          });
    }
    Map<String, String> transformedProperties =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProperties);

    if (transformedProperties != null) {
      all.putAll(transformedProperties);
    }
    all.put(
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoIcebergCatalogFactoryOptions.IDENTIFIER);
    // Map "catalog-backend" to "catalog-type".
    GRAVITINO_CONFIG_TO_FLINK_ICEBERG.forEach(
        (key, value) -> {
          if (all.containsKey(key)) {
            String config = all.remove(key);
            all.put(value, config);
          }
        });
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
