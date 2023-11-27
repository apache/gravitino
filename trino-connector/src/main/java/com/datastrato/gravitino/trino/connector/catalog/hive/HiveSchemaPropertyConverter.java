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

public class HiveSchemaPropertyConverter implements PropertyConverter {

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>().put("location", "location").build());

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      hiveProperties.put(
          TRINO_KEY_TO_GRAVITINO_KEY.inverseBidiMap().get(entry.getKey()), entry.getValue());
    }
    return hiveProperties;
  }

  @Override
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> hiveProperties = new HashMap<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      hiveProperties.put(TRINO_KEY_TO_GRAVITINO_KEY.get(entry.getKey()), entry.getValue());
    }
    return hiveProperties;
  }
}
