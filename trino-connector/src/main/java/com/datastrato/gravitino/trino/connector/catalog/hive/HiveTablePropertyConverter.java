/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.catalog.common.property.PropertyConverter;
import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HiveTablePropertyConverter extends PropertyConverter {
  private final BasePropertiesMetadata hiveTablePropertiesMetadata =
      new HiveTablePropertiesMetadata();
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
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

  @Override
  public TreeBidiMap<String, String> engineToGravitino() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return hiveTablePropertiesMetadata.propertyEntries();
  }
}
