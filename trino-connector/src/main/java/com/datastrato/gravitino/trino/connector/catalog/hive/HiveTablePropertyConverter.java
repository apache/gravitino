/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_EXTERNAL;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_INPUT_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_LOCATION;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_NUM_FILES;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_OUTPUT_FORMAT;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_SERDE_LIB;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_SERDE_NAME;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_TOTAL_SIZE;
import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_TABLE_TYPE;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveTablePropertyConverter extends PropertyConverter {
  private final BasePropertiesMetadata hiveTablePropertiesMetadata =
      new HiveTablePropertiesMetadata();
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(HIVE_TABLE_FORMAT, HiveTablePropertiesMetadata.FORMAT)
              .put(HIVE_TABLE_TOTAL_SIZE, HiveTablePropertiesMetadata.TOTAL_SIZE)
              .put(HIVE_TABLE_NUM_FILES, HiveTablePropertiesMetadata.NUM_FILES)
              .put(HIVE_TABLE_EXTERNAL, HiveTablePropertiesMetadata.EXTERNAL)
              .put(HIVE_TABLE_LOCATION, HiveTablePropertiesMetadata.LOCATION)
              .put(HIVE_TABLE_TYPE, HiveTablePropertiesMetadata.TABLE_TYPE)
              .put(HIVE_TABLE_INPUT_FORMAT, HiveTablePropertiesMetadata.INPUT_FORMAT)
              .put(HIVE_TABLE_OUTPUT_FORMAT, HiveTablePropertiesMetadata.OUTPUT_FORMAT)
              .put(HIVE_TABLE_SERDE_LIB, HiveTablePropertiesMetadata.SERDE_LIB)
              .put(HIVE_TABLE_SERDE_NAME, HiveTablePropertiesMetadata.SERDE_NAME)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return hiveTablePropertiesMetadata.propertyEntries();
  }
}
