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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

public class IcebergPropertiesUtils {

  // Map that maintains the mapping of keys in Gravitino to that in Iceberg, for example, users
  // will only need to set the configuration 'catalog-backend' in Gravitino and Gravitino will
  // change it to `catalogType` automatically and pass it to Iceberg.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_ICEBERG;
  public static final Map<String, String> ICEBERG_CATALOG_CONFIG_TO_GRAVITINO;

  static {
    Map<String, String> map = new HashMap();
    map.put(IcebergConstants.CATALOG_BACKEND, IcebergConstants.CATALOG_BACKEND);
    map.put(IcebergConstants.CATALOG_BACKEND_IMPL, IcebergConstants.CATALOG_BACKEND_IMPL);
    map.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, IcebergConstants.GRAVITINO_JDBC_DRIVER);
    map.put(IcebergConstants.GRAVITINO_JDBC_USER, IcebergConstants.ICEBERG_JDBC_USER);
    map.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, IcebergConstants.ICEBERG_JDBC_PASSWORD);
    map.put(IcebergConstants.URI, IcebergConstants.URI);
    map.put(IcebergConstants.WAREHOUSE, IcebergConstants.WAREHOUSE);
    map.put(IcebergConstants.CATALOG_BACKEND_NAME, IcebergConstants.CATALOG_BACKEND_NAME);
    map.put(IcebergConstants.IO_IMPL, IcebergConstants.IO_IMPL);
    // S3
    map.put(S3Properties.GRAVITINO_S3_ENDPOINT, IcebergConstants.ICEBERG_S3_ENDPOINT);
    map.put(S3Properties.GRAVITINO_S3_REGION, IcebergConstants.AWS_S3_REGION);
    map.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID);
    map.put(
        S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY);
    map.put(
        S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS);
    // OSS
    map.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, IcebergConstants.ICEBERG_OSS_ENDPOINT);
    map.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, IcebergConstants.ICEBERG_OSS_ACCESS_KEY_ID);
    map.put(
        OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
        IcebergConstants.ICEBERG_OSS_ACCESS_KEY_SECRET);
    // ADLS
    map.put(
        AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
        IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_NAME);
    map.put(
        AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
        IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_KEY);
    // Table metadata cache
    map.put(IcebergConstants.TABLE_METADATA_CACHE_IMPL, IcebergConstants.TABLE_METADATA_CACHE_IMPL);
    map.put(
        IcebergConstants.TABLE_METADATA_CACHE_CAPACITY,
        IcebergConstants.TABLE_METADATA_CACHE_CAPACITY);
    map.put(
        IcebergConstants.TABLE_METADATA_CACHE_EXPIRE_MINUTES,
        IcebergConstants.TABLE_METADATA_CACHE_EXPIRE_MINUTES);

    GRAVITINO_CONFIG_TO_ICEBERG = Collections.unmodifiableMap(map);

    Map<String, String> icebergCatalogConfigToGravitino = new HashMap<>();
    map.forEach(
        (key, value) -> {
          icebergCatalogConfigToGravitino.put(value, key);
        });
    ICEBERG_CATALOG_CONFIG_TO_GRAVITINO =
        Collections.unmodifiableMap(icebergCatalogConfigToGravitino);
  }

  /**
   * Converts Gravitino properties to Iceberg catalog properties, the common transform logic shared
   * by Spark connector, Iceberg REST server, Gravitino Iceberg catalog.
   *
   * @param gravitinoProperties a map of Gravitino configuration properties.
   * @return a map containing Iceberg catalog properties.
   */
  public static Map<String, String> toIcebergCatalogProperties(
      Map<String, String> gravitinoProperties) {
    Map<String, String> icebergProperties = new HashMap<>();
    gravitinoProperties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_ICEBERG.containsKey(key)) {
            icebergProperties.put(GRAVITINO_CONFIG_TO_ICEBERG.get(key), value);
          }
        });
    return icebergProperties;
  }

  /**
   * Get catalog backend name from Gravitino catalog properties.
   *
   * @param catalogProperties a map of Gravitino catalog properties.
   * @return catalog backend name.
   */
  public static String getCatalogBackendName(Map<String, String> catalogProperties) {
    String backendName = catalogProperties.get(IcebergConstants.CATALOG_BACKEND_NAME);
    if (backendName != null) {
      return backendName;
    }

    String catalogBackend = catalogProperties.get(IcebergConstants.CATALOG_BACKEND);
    return Optional.ofNullable(catalogBackend)
        .map(s -> s.toLowerCase(Locale.ROOT))
        .orElse("memory");
  }
}
