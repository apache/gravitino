/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSchemaPropertyConverter implements PropertyConverter {
  public static final Logger LOG = LoggerFactory.getLogger(HiveSchemaPropertyConverter.class);
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>().put("location", "location").build());

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();

    // TODO(yuqi) merge logic here and in HiveTablePropertyConverter
    Map<String, String> gravitinoToTrino = TRINO_KEY_TO_GRAVITINO_KEY.inverseBidiMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String trinoKey = gravitinoToTrino.get(entry.getKey());
      if (trinoKey != null) {
        hiveProperties.put(trinoKey, entry.getValue());
      } else {
        LOG.warn("No mapping for property {} in Hive schema", trinoKey);
      }
    }
    return hiveProperties;
  }

  @Override
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> hiveProperties = new HashMap<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = TRINO_KEY_TO_GRAVITINO_KEY.get(entry.getKey());
      if (gravitinoKey != null) {
        hiveProperties.put(gravitinoKey, entry.getValue());
      } else {
        LOG.warn("No mapping for property {} in Hive schema", gravitinoKey);
      }
    }
    return hiveProperties;
  }
}
