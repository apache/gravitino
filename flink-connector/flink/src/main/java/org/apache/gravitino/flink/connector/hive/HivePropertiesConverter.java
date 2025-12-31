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

package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Properties converter for Hive catalogs used by the Gravitino Flink connector.
 *
 * <p>This converter is exclusively used by the Flink connector when creating or accessing Hive
 * tables through Gravitino. It is not used by other connectors (e.g., Spark, Trino) or by direct
 * Gravitino Hive catalog operations.
 *
 * <p>The default SerDe behavior in {@link #toGravitinoTableProperties(Map)} applies only to Hive
 * tables created through the Flink connector path, ensuring Flink ↔ Hive interoperability. Other
 * Hive usage paths (Spark connector, Trino connector, direct Gravitino API) are unaffected.
 */
public class HivePropertiesConverter implements PropertiesConverter {

  private HivePropertiesConverter() {}

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter();
  private static final Map<String, String> HIVE_CATALOG_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, HiveConstants.METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(HiveConstants.METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return HIVE_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return GRAVITINO_CONFIG_TO_HIVE.get(configKey);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    Map<String, String> properties =
        gravitinoTableProperties.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> {
                      String key = entry.getKey();
                      if (key.startsWith(HiveConstants.SERDE_PARAMETER_PREFIX)) {
                        return key.substring(HiveConstants.SERDE_PARAMETER_PREFIX.length());
                      } else {
                        return key;
                      }
                    },
                    Map.Entry::getValue,
                    (existingValue, newValue) -> newValue));
    properties.put("connector", "hive");
    return properties;
  }

  /**
   * Converts Flink table properties to Gravitino table properties for Hive tables.
   *
   * <p><b>Scope:</b> This method is only invoked when creating Hive tables through the Flink
   * connector. It does not affect:
   *
   * <ul>
   *   <li>Hive tables created via other connectors (Spark, Trino, etc.)
   *   <li>Hive tables created directly through Gravitino API
   *   <li>Non-Hive catalogs (Iceberg, Paimon, JDBC, etc.)
   * </ul>
   *
   * <p><b>Default SerDe behavior:</b> When {@code serde-lib} is not explicitly provided in the
   * Flink table properties, this method derives the default SerDe from {@link HiveConf}, consistent
   * with Flink Hive connector behavior. This ensures tables created via the Gravitino Flink
   * connector can be read by the native Flink Hive client.
   *
   * @param flinkProperties The Flink table properties to convert
   * @return Gravitino table properties with default {@code serde-lib} set if missing
   */
  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    Map<String, String> gravitinoProperties = new HashMap<>(flinkProperties);

    // Set default serde-lib for Flink-created Hive tables only.
    // This ensures Flink ↔ Hive interoperability and does not affect other usage
    // paths.
    if (!gravitinoProperties.containsKey(HiveConstants.SERDE_LIB)) {
      HiveConf hiveConf = new HiveConf(false);
      String defaultSerDe = hiveConf.get("hive.default.serde");

      if (defaultSerDe != null) {
        gravitinoProperties.put(HiveConstants.SERDE_LIB, defaultSerDe);
      }
    }

    return gravitinoProperties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoHiveCatalogFactoryOptions.IDENTIFIER;
  }
}
