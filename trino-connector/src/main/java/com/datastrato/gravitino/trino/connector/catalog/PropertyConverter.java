/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforming between gravitino schema/table/column property and Trino property. */
public abstract class PropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);

  /**
   * Convert Trino properties to Gravitino properties. It will return a map that holds the mapping
   * between Trino and gravitino properties.
   *
   * @return a map that holds the mapping from Trino to Gravitino properties.
   */
  public abstract TreeBidiMap<String, String> trinoPropertyKeyToGravitino();

  /** Convert Trino properties to Gravitino properties. */
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> trinoProperties = new HashMap<>();
    Map<String, String> gravitinoToTrinoMapping = trinoPropertyKeyToGravitino().inverseBidiMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String trinoKey = gravitinoToTrinoMapping.get(entry.getKey());
      if (trinoKey != null) {
        trinoProperties.put(trinoKey, entry.getValue());
      } else {
        LOG.warn("Property {} is not supported in Trino", trinoKey);
      }
    }
    return trinoProperties;
  }

  /** Convert Gravitino properties to Trino properties. */
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> gravitinoProperties = new HashMap<>();
    Map<String, String> trinoToGravitinoMapping = trinoPropertyKeyToGravitino();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = trinoToGravitinoMapping.get(entry.getKey());
      if (gravitinoKey != null) {
        gravitinoProperties.put(gravitinoKey, entry.getValue());
      } else {
        LOG.warn("Property {} is not supported in Gravitino", gravitinoKey);
      }
    }
    return gravitinoProperties;
  }
}
