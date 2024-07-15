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

package com.datastrato.gravitino.catalog.property;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforming between Apache Gravitino schema/table/column property and engine property. */
public abstract class PropertyConverter {

  protected static final String TRINO_PROPERTIES_PREFIX = "trino.bypass.";

  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);
  /**
   * Mapping that maps engine properties to Gravitino properties. It will return a map that holds
   * the mapping between engine and Gravitino properties.
   *
   * @return a map that holds the mapping from engine to Gravitino properties.
   */
  public abstract Map<String, String> engineToGravitinoMapping();

  /**
   * Get the property metadata for the catalog. for more please see {@link
   * HiveTablePropertiesMetadata#propertyEntries()} or {@link
   * com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata#propertyEntries()}
   */
  public abstract Map<String, PropertyEntry<?>> gravitinoPropertyMeta();

  Map<String, String> revsereMap(Map<String, String> map) {
    Map<String, String> res = new HashMap<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      res.put(entry.getValue(), entry.getKey());
    }

    return res;
  }

  /** Convert Gravitino properties to engine properties. */
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> engineProperties = new HashMap<>();
    Map<String, String> gravitinoToEngineMapping = revsereMap(engineToGravitinoMapping());
    for (Map.Entry<String, String> entry : gravitinoProperties.entrySet()) {
      String engineKey = gravitinoToEngineMapping.get(entry.getKey());
      if (engineKey != null) {
        engineProperties.put(engineKey, entry.getValue());
      } else {
        LOG.info("Property {} is not supported by engine", entry.getKey());
      }
    }
    return engineProperties;
  }

  /**
   * Convert engine properties to Gravitino properties.
   *
   * <p>If different engine has different behavior about error handling, you can override this
   * method.
   */
  public Map<String, Object> engineToGravitinoProperties(Map<String, Object> engineProperties) {
    Map<String, Object> gravitinoProperties = new HashMap<>();
    Map<String, String> engineToGravitinoMapping = engineToGravitinoMapping();

    Map<String, PropertyEntry<?>> propertyEntryMap = gravitinoPropertyMeta();
    for (Map.Entry<String, Object> entry : engineProperties.entrySet()) {
      String gravitinoKey = engineToGravitinoMapping.get(entry.getKey());
      if (gravitinoKey != null) {
        PropertyEntry<?> propertyEntry = propertyEntryMap.get(gravitinoKey);
        if (propertyEntry != null) {
          // Check value is valid.
          propertyEntry.decode(entry.getValue().toString());
        }
        gravitinoProperties.put(gravitinoKey, entry.getValue());
      } else {
        LOG.info("Property {} is not supported by Gravitino", entry.getKey());
      }
    }

    // Check the required properties.
    for (Map.Entry<String, PropertyEntry<?>> propertyEntry : propertyEntryMap.entrySet()) {
      if (propertyEntry.getValue().isRequired()
          && !gravitinoProperties.containsKey(propertyEntry.getKey())) {
        throw new IllegalArgumentException(
            "Property " + propertyEntry.getKey() + " is required, you should ");
      }
    }

    return gravitinoProperties;
  }
}
