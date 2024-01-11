/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.engine.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveSchemaPropertyConverterV2 extends PropertyConverter {

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put("location", HiveSchemaPropertiesMetadata.LOCATION)
              .build());

  static final TreeBidiMap<PropertyEntry<?>, String> GRAVITINO_PROPERTY_TO_TRINO_PROPERTY =
      new TreeBidiMap<>(
          HiveSchemaPropertiesMetadata.propertiesMetadata.values().stream()
              .filter(
                  entry -> TRINO_KEY_TO_GRAVITINO_KEY.inverseBidiMap().containsKey(entry.getName()))
              .collect(
                  ImmutableMap.toImmutableMap(
                      propertyEntry -> propertyEntry,
                      propertyEntry ->
                          TRINO_KEY_TO_GRAVITINO_KEY
                              .inverseBidiMap()
                              .get(propertyEntry.getName()))));

  @Override
  public TreeBidiMap<PropertyEntry<?>, String> gravitinoToEngineProperty() {
    return GRAVITINO_PROPERTY_TO_TRINO_PROPERTY;
  }
}
