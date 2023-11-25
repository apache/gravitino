/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.HasProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveSchemaProperties implements HasProperties {

  public static final HiveSchemaProperties INSTANCE = new HiveSchemaProperties();

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>().put("location", "location").build());

  // TODO yuqi Need to improve schema properties
  private static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      ImmutableList.of(
          stringProperty("location", "Hive storage location for the schema", null, false));

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return SCHEMA_PROPERTY_META;
  }

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
  public Map<String, String> toGravitinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      hiveProperties.put(TRINO_KEY_TO_GRAVITINO_KEY.get(entry.getKey()), entry.getValue());
    }
    return hiveProperties;
  }

  @Override
  public List<PropertyMetadata<?>> getPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }
}
