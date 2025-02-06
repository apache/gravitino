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
package org.apache.gravitino.catalog.lakehouse.paimon;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

public class PaimonPropertiesUtils {

  // Map that maintains the mapping of keys in Gravitino to that in Paimon, for example, users
  // will only need to set the configuration 'catalog-backend' in Gravitino and Gravitino will
  // change it to `catalogType` automatically and pass it to Paimon.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_PAIMON;
  public static final Map<String, String> PAIMON_CATALOG_CONFIG_TO_GRAVITINO;

  static {
    Map<String, String> gravitinoConfigToPaimon = new HashMap<>();
    Map<String, String> paimonCatalogConfigToGravitino = new HashMap<>();
    gravitinoConfigToPaimon.put(PaimonConstants.CATALOG_BACKEND, PaimonConstants.CATALOG_BACKEND);
    gravitinoConfigToPaimon.put(
        PaimonConstants.GRAVITINO_JDBC_DRIVER, PaimonConstants.GRAVITINO_JDBC_DRIVER);
    gravitinoConfigToPaimon.put(
        PaimonConstants.GRAVITINO_JDBC_USER, PaimonConstants.PAIMON_JDBC_USER);
    gravitinoConfigToPaimon.put(
        PaimonConstants.GRAVITINO_JDBC_PASSWORD, PaimonConstants.PAIMON_JDBC_PASSWORD);
    gravitinoConfigToPaimon.put(PaimonConstants.URI, PaimonConstants.URI);
    gravitinoConfigToPaimon.put(PaimonConstants.WAREHOUSE, PaimonConstants.WAREHOUSE);
    gravitinoConfigToPaimon.put(
        PaimonConstants.CATALOG_BACKEND_NAME, PaimonConstants.CATALOG_BACKEND_NAME);
    // S3
    gravitinoConfigToPaimon.put(S3Properties.GRAVITINO_S3_ENDPOINT, PaimonConstants.S3_ENDPOINT);
    gravitinoConfigToPaimon.put(
        S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, PaimonConstants.S3_ACCESS_KEY);
    gravitinoConfigToPaimon.put(
        S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, PaimonConstants.S3_SECRET_KEY);
    // OSS
    gravitinoConfigToPaimon.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, PaimonConstants.OSS_ENDPOINT);
    gravitinoConfigToPaimon.put(
        OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, PaimonConstants.OSS_ACCESS_KEY);
    gravitinoConfigToPaimon.put(
        OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, PaimonConstants.OSS_SECRET_KEY);
    GRAVITINO_CONFIG_TO_PAIMON = Collections.unmodifiableMap(gravitinoConfigToPaimon);
    gravitinoConfigToPaimon.forEach(
        (key, value) -> {
          paimonCatalogConfigToGravitino.put(value, key);
        });
    PAIMON_CATALOG_CONFIG_TO_GRAVITINO =
        Collections.unmodifiableMap(paimonCatalogConfigToGravitino);
  }

  /**
   * Converts Gravitino properties to Paimon catalog properties, the common transform logic shared
   * by Spark connector, Gravitino Paimon catalog.
   *
   * @param gravitinoProperties a map of Gravitino configuration properties.
   * @return a map containing Paimon catalog properties.
   */
  public static Map<String, String> toPaimonCatalogProperties(
      Map<String, String> gravitinoProperties) {
    Map<String, String> paimonProperties = new HashMap<>();
    gravitinoProperties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_PAIMON.containsKey(key)) {
            paimonProperties.put(GRAVITINO_CONFIG_TO_PAIMON.get(key), value);
          }
        });
    return paimonProperties;
  }

  /**
   * Get catalog backend name from Gravitino catalog properties.
   *
   * @param catalogProperties a map of Gravitino catalog properties.
   * @return catalog backend name.
   */
  public static String getCatalogBackendName(Map<String, String> catalogProperties) {
    String backendName = catalogProperties.get(PaimonConstants.CATALOG_BACKEND_NAME);
    if (backendName != null) {
      return backendName;
    }

    String catalogBackend = catalogProperties.get(PaimonConstants.CATALOG_BACKEND);
    return Optional.ofNullable(catalogBackend)
        .map(s -> s.toLowerCase(Locale.ROOT))
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format("Unsupported catalog backend: %s", catalogBackend)));
  }
}
