/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.catalog.property;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import java.util.HashMap;
import java.util.Map;

/** Transforming between gravitino schema/table/column property and engine property. */
public abstract class PropertyConverter {

  /**
   * Convert engine properties to Gravitino properties. It will return a map that holds the mapping
   * between engine and gravitino properties.
   *
   * @return a map that holds the mapping from engine to Gravitino properties.
   */
  public abstract Map<String, String> engineToGravitino();

  /**
   * Get the property metadata for the catalog. for more please see {@link
   * HiveTablePropertiesMetadata#propertyEntries()} or {@link
   * com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata#propertyEntries()}
   *
   * @return
   */
  public abstract Map<String, PropertyEntry<?>> gravitinoPropertyMeta();

  Map<String, String> revsereMap(Map<String, String> map) {
    Map<String, String> res = new HashMap<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      res.put(entry.getValue(), entry.getKey());
    }

    return res;
  }

  /** Convert engine properties to Gravitino properties. */
  public Map<String, String> fromGravitinoProperties(Map<String, String> properties) {
    Map<String, String> engineProperties = new HashMap<>();
    Map<String, String> gravitinoToTrinoMapping = revsereMap(engineToGravitino());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String engineKey = gravitinoToTrinoMapping.get(entry.getKey());
      if (engineKey != null) {
        engineProperties.put(engineKey, entry.getValue());
      }
    }
    return engineProperties;
  }

  /** Convert Gravitino properties to engine properties. */
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> gravitinoProperties = new HashMap<>();
    Map<String, String> trinoToGravitinoMapping = engineToGravitino();

    Map<String, PropertyEntry<?>> propertyEntryMap = gravitinoPropertyMeta();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = trinoToGravitinoMapping.get(entry.getKey());
      if (gravitinoKey != null) {
        PropertyEntry<?> propertyEntry = propertyEntryMap.get(gravitinoKey);
        if (propertyEntry != null) {
          // Check value is valid.
          propertyEntry.decode(entry.getValue().toString());
        }
        gravitinoProperties.put(gravitinoKey, entry.getValue());
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
