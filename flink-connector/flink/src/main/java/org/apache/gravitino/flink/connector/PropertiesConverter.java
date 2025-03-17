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

package org.apache.gravitino.flink.connector;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * PropertiesConverter is used to convert properties between Flink properties and Apache Gravitino
 * properties
 */
public interface PropertiesConverter {

  String FLINK_PROPERTY_PREFIX = "flink.bypass.";

  /**
   * Converts properties from application provided properties and Flink connector properties to
   * Gravitino properties.This method processes the Flink configuration and transforms it into a
   * format suitable for the Gravitino catalog.
   *
   * @param flinkConf The Flink configuration containing connector properties. This includes both
   *     Flink-specific properties and any user-provided properties.
   * @return A map of properties converted for use in the Gravitino catalog. The returned map
   *     includes both directly transformed properties and bypass properties prefixed with {@link
   *     #FLINK_PROPERTY_PREFIX}.
   */
  default Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoProperties = Maps.newHashMap();
    for (Map.Entry<String, String> entry : flinkConf.toMap().entrySet()) {
      String gravitinoKey = transformPropertyToGravitinoCatalog(entry.getKey());
      if (gravitinoKey != null) {
        gravitinoProperties.put(gravitinoKey, entry.getValue());
      } else if (!entry.getKey().startsWith(FLINK_PROPERTY_PREFIX)) {
        gravitinoProperties.put(FLINK_PROPERTY_PREFIX + entry.getKey(), entry.getValue());
      } else {
        gravitinoProperties.put(entry.getKey(), entry.getValue());
      }
    }
    return gravitinoProperties;
  }

  /**
   * Converts properties from Gravitino catalog properties to Flink connector properties. This
   * method processes the Gravitino properties and transforms them into a format suitable for the
   * Flink connector.
   *
   * @param gravitinoProperties The properties provided by the Gravitino catalog. This includes both
   *     Gravitino-specific properties and any bypass properties prefixed with {@link
   *     #FLINK_PROPERTY_PREFIX}.
   * @return A map of properties converted for use in the Flink connector. The returned map includes
   *     both transformed properties and the Flink catalog type.
   */
  default Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> allProperties = Maps.newHashMap();
    gravitinoProperties.forEach(
        (key, value) -> {
          String flinkConfigKey = key;
          if (key.startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX)) {
            flinkConfigKey = key.substring(PropertiesConverter.FLINK_PROPERTY_PREFIX.length());
            allProperties.put(flinkConfigKey, value);
          } else {
            String convertedKey = transformPropertyToFlinkCatalog(flinkConfigKey);
            if (convertedKey != null) {
              allProperties.put(convertedKey, value);
            }
          }
        });
    allProperties.put(CommonCatalogOptions.CATALOG_TYPE.key(), getFlinkCatalogType());
    return allProperties;
  }

  /**
   * Transforms a Flink configuration key to a corresponding Gravitino catalog property key. This
   * method is used to map Flink-specific configuration keys to Gravitino catalog properties.
   *
   * @param configKey The Flink configuration key to be transformed.
   * @return The corresponding Gravitino catalog property key, or {@code null} if no transformation
   *     is needed.
   */
  String transformPropertyToGravitinoCatalog(String configKey);

  /**
   * Transforms a specific configuration key from Gravitino catalog properties to Flink connector
   * properties. This method is used to convert a property key that is specific to Gravitino into a
   * format that can be understood by the Flink connector.
   *
   * @param configKey The configuration key from Gravitino catalog properties to be transformed.
   * @return The transformed configuration key that is compatible with the Flink connector.
   * @throws IllegalArgumentException If the provided configuration key cannot be transformed or is
   *     invalid.
   */
  String transformPropertyToFlinkCatalog(String configKey);

  /**
   * Converts properties from Flink connector schema properties to Gravitino schema properties.
   *
   * @param flinkProperties The schema properties provided by Flink.
   * @return The schema properties for the Gravitino.
   */
  default Map<String, String> toGravitinoSchemaProperties(Map<String, String> flinkProperties) {
    return flinkProperties;
  }

  /**
   * Converts properties from Gravitino database properties to Flink connector schema properties.
   *
   * @param gravitinoProperties The schema properties provided by Gravitino.
   * @return The database properties for the Flink connector.
   */
  default Map<String, String> toFlinkDatabaseProperties(Map<String, String> gravitinoProperties) {
    return gravitinoProperties;
  }

  /**
   * Converts properties from Gravitino table properties to Flink connector table properties.
   *
   * @param flinkCatalogProperties The flinkCatalogProperties are either the converted properties
   *     obtained through the toFlinkCatalogProperties method in GravitinoCatalogStore, or the
   *     options passed when writing a CREATE CATALOG statement in Flink SQL.
   * @param gravitinoTableProperties The table properties provided by Gravitino.
   * @param tablePath The tablePath provides the database and table for some catalogs, such as the
   *     {@link org.apache.gravitino.flink.connector.jdbc.GravitinoJdbcCatalog}.
   * @return The table properties for the Flink connector.
   */
  default Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    return gravitinoTableProperties;
  }

  /**
   * Converts properties from Flink connector table properties to Gravitino table properties.
   *
   * @param flinkProperties The table properties provided by Flink.
   * @return The table properties for the Gravitino.
   */
  default Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    return flinkProperties;
  }

  /**
   * Retrieves the Flink catalog type associated with this converter. This method is used to
   * determine the type of Flink catalog that this converter is designed for.
   *
   * @return The Flink catalog type.
   */
  String getFlinkCatalogType();
}
