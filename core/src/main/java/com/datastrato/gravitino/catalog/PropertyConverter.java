/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PropertyConverter {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);

  /**
   * Key mapping between Gravitino and query engines that Gravitino is integrated with. The key is
   * the Gravitino property key, and the value is the query engines property key.
   *
   * @return
   */
  public abstract TreeBidiMap<PropertyEntry<?>, PropertyEntry<?>> gravitinoToEngineProperty();

  static class CompoundPropertyEntry {
    PropertyEntry<?> from;
    PropertyEntry<?> to;
  }

  public Map<String, CompoundPropertyEntry> getCompoundPropertyMap(
      Map<PropertyEntry<?>, PropertyEntry<?>> propertyEntryMap) {
    Map<String, CompoundPropertyEntry> map = new HashMap<>();
    for (Map.Entry<PropertyEntry<?>, PropertyEntry<?>> entry : propertyEntryMap.entrySet()) {
      CompoundPropertyEntry compoundEntry = new CompoundPropertyEntry();
      compoundEntry.from = entry.getKey();
      compoundEntry.to = entry.getValue();
      map.put(entry.getKey().getName(), compoundEntry);
    }
    return map;
  }

  public Map<String, String> fromGravitino(Map<String, String> properties) {
    Map<String, String> engineProperties = new HashMap<>();
    Map<String, CompoundPropertyEntry> gravitinoToEngineMapping =
        getCompoundPropertyMap(gravitinoToEngineProperty());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      CompoundPropertyEntry compoundPropertyEntry = gravitinoToEngineMapping.get(entry.getKey());
      if (compoundPropertyEntry != null) {
        // Check value format is valid
        compoundPropertyEntry.to.decode(entry.getValue());

        engineProperties.put(compoundPropertyEntry.to.getName(), entry.getValue());
      } else {
        // Ignore unrecognized properties
        LOG.warn("Property {} is not supported by query engine", entry.getKey());
      }
    }

    // Check required properties
    for (Map.Entry<String, CompoundPropertyEntry> entry : gravitinoToEngineMapping.entrySet()) {
      if (entry.getValue().to.isRequired()
          && !engineProperties.containsKey(entry.getValue().to.getName())) {
        throw new IllegalArgumentException(
            String.format("Missing required property %s", entry.getKey()));
      }
    }

    return engineProperties;
  }

  public Map<String, String> toGravitino(Map<String, String> properties) {
    Map<String, String> gravitinoProperties = new HashMap<>();
    Map<String, CompoundPropertyEntry> engineToGravitinoMapping =
        getCompoundPropertyMap(gravitinoToEngineProperty().inverseBidiMap());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      CompoundPropertyEntry compoundPropertyEntry = engineToGravitinoMapping.get(entry.getKey());
      if (compoundPropertyEntry != null) {
        // Check value format
        compoundPropertyEntry.to.decode(entry.getValue());
        gravitinoProperties.put(compoundPropertyEntry.to.getName(), entry.getValue());
      } else {
        // Ignore unrecognized properties
        LOG.warn("Property {} is not supported by Gravitino", entry.getKey());
      }
    }

    // Check required properties
    for (Map.Entry<String, CompoundPropertyEntry> entry : engineToGravitinoMapping.entrySet()) {
      if (entry.getValue().to.isRequired()
          && !gravitinoProperties.containsKey(entry.getValue().to.getName())) {
        throw new IllegalArgumentException(
            String.format("Missing required property %s", entry.getKey()));
      }
    }

    return gravitinoProperties;
  }
}
