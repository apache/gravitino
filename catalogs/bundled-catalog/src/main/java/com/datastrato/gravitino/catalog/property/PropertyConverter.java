/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.property;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforming between gravitino schema/table/column property and engine property. */
public abstract class PropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);
  /**
   * Mapping that maps engine properties to Gravitino properties. It will return a map that holds
   * the mapping between engine and gravitino properties.
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
