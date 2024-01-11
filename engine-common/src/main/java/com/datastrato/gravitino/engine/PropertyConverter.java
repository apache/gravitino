/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.engine;

import com.datastrato.gravitino.catalog.PropertyEntry;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PropertyConverter {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);

  /**
   * Key mapping between Gravitino and query engines that Gravitino is integrated with. The key is
   * the Gravitino property entry, and the value is the query engines property key.
   */
  public abstract TreeBidiMap<PropertyEntry<?>, String> gravitinoToEngineProperty();

  private TreeBidiMap<String, String> getMapping(
      TreeBidiMap<PropertyEntry<?>, String> propertyEntryMap) {
    TreeBidiMap<String, String> map = new TreeBidiMap<>();
    for (Map.Entry<PropertyEntry<?>, String> entry : propertyEntryMap.entrySet()) {
      map.put(entry.getKey().getName(), entry.getValue());
    }
    return map;
  }

  private Map<String, PropertyEntry<?>> stringToEntry(
      TreeBidiMap<PropertyEntry<?>, String> propertyEntryMap) {
    Map<String, PropertyEntry<?>> map = new HashMap<>();
    for (Map.Entry<PropertyEntry<?>, String> entry : propertyEntryMap.entrySet()) {
      map.put(entry.getKey().getName(), entry.getKey());
    }
    return map;
  }

  public Map<String, String> fromGravitino(Map<String, String> properties) {
    Map<String, String> engineProperties = new HashMap<>();
    TreeBidiMap<String, String> gravitinoKeyToEngineKey = getMapping(gravitinoToEngineProperty());
    Map<String, PropertyEntry<?>> gravitinoKeyToEntry = stringToEntry(gravitinoToEngineProperty());

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      PropertyEntry<?> propertyEntry = gravitinoKeyToEntry.get(entry.getKey());
      if (propertyEntry == null) {
        // Ignore unrecognized properties
        LOG.warn("Property {} is not supported by query engine", entry.getKey());
      } else {
        // Check value format is valid
        propertyEntry.decode(entry.getValue());
        engineProperties.put(gravitinoKeyToEngineKey.get(entry.getKey()), entry.getValue());
      }
    }

    return engineProperties;
  }

  public Map<String, String> toGravitino(Map<String, Object> properties) {
    Map<String, String> gravitinoProperties = new HashMap<>();

    BidiMap<String, String> engineKeyToGravitinoKey =
        getMapping(gravitinoToEngineProperty()).inverseBidiMap();
    Map<String, PropertyEntry<?>> gravitinoKeyToEntry = stringToEntry(gravitinoToEngineProperty());

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = engineKeyToGravitinoKey.get(entry.getKey());
      if (gravitinoKey == null) {
        // Ignore unrecognized properties
        LOG.warn("Property {} is not supported by Gravitino", entry.getKey());
      } else {
        PropertyEntry<?> propertyEntry = gravitinoKeyToEntry.get(gravitinoKey);
        propertyEntry.decode(entry.getValue().toString());
        gravitinoProperties.put(gravitinoKey, entry.getValue().toString());
      }
    }

    return gravitinoProperties;
  }
}
