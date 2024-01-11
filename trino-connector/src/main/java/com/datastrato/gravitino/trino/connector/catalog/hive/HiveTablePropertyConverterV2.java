/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.engine.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import java.util.Map.Entry;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveTablePropertyConverterV2 extends PropertyConverter {

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put("format", HiveTablePropertiesMetadata.FORMAT)
              .put("total_size", HiveTablePropertiesMetadata.TOTAL_SIZE)
              .put("num_files", HiveTablePropertiesMetadata.NUM_FILES)
              .put("external", HiveTablePropertiesMetadata.EXTERNAL)
              .put("location", HiveTablePropertiesMetadata.LOCATION)
              .put("table_type", HiveTablePropertiesMetadata.TABLE_TYPE)
              .put("input_format", HiveTablePropertiesMetadata.INPUT_FORMAT)
              .put("output_format", HiveTablePropertiesMetadata.INPUT_FORMAT)
              .put("serde_lib", HiveTablePropertiesMetadata.SERDE_LIB)
              .put("serde_name", HiveTablePropertiesMetadata.SERDE_NAME)
              .build());

  static final TreeBidiMap<PropertyEntry<?>, String> GRAVITINO_PROPERTY_TO_TRINO_PROPERTY =
      new TreeBidiMap<>(
          HiveTablePropertiesMetadata.propertiesMetadata.entrySet().stream()
              .map(Entry::getValue)
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
