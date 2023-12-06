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

/** Transforming between gravitino schema/table/column property and trino property. */
public abstract class PropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);
  /**
   * Convert trino properties to gravitino properties. It will return a map that holds the mapping
   * between trino and gravitino properties.
   *
   * @return a map that holds the mapping from trino to gravitino properties.
   */
  public abstract TreeBidiMap<String, String> trinoPropertyKeyToGravitino();
  /** Convert trino properties to gravitino properties. */
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> trinoProperties = new HashMap<>();
    Map<String, String> gravitinoToTrinoMapping = trinoPropertyKeyToGravitino().inverseBidiMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String trinoKey = gravitinoToTrinoMapping.get(entry.getKey());
      if (trinoKey != null) {
        trinoProperties.put(trinoKey, entry.getValue());
      } else {
        LOG.warn("Property {} is not supported in trino", trinoKey);
      }
    }
    return trinoProperties;
  }

  /** Convert gravitino properties to trino properties. */
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> gravitinoProperties = new HashMap<>();
    Map<String, String> trinoToGravitinoMapping = trinoPropertyKeyToGravitino();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = trinoToGravitinoMapping.get(entry.getKey());
      if (gravitinoKey != null) {
        gravitinoProperties.put(gravitinoKey, entry.getValue());
      } else {
        LOG.warn("Property {} is not supported in gravitino", gravitinoKey);
      }
    }
    return gravitinoProperties;
  }
}
