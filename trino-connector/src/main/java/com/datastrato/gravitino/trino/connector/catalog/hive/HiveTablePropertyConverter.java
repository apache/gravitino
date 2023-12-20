/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalogs.hive.HiveTablePropertyKeys;
import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;

public class HiveTablePropertyConverter extends PropertyConverter {

  static final String TRINO_HIVE_TABLE_FORMAT = "format";
  static final String TRINO_HIVE_TABLE_TOTAL_SIZE = "total_size";
  static final String TRINO_HIVE_TABLE_NUM_FILES = "num_files";
  static final String TRINO_HIVE_TABLE_EXTERNAL = "external";
  static final String TRINO_HIVE_TABLE_LOCATION = "location";
  static final String TRINO_HIVE_TABLE_TABLE_TYPE = "table_type";
  static final String TRINO_HIVE_TABLE_INPUT_FORMAT = "input_format";
  static final String TRINO_HIVE_TABLE_OUTPUT_FORMAT = "output_format";
  static final String TRINO_HIVE_TABLE_SERDE_LIB = "serde_lib";
  static final String TRINO_HIVE_TABLE_SERDE_NAME = "serde_name";

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(TRINO_HIVE_TABLE_FORMAT, HiveTablePropertyKeys.FORMAT)
              .put(TRINO_HIVE_TABLE_TOTAL_SIZE, HiveTablePropertyKeys.TOTAL_SIZE)
              .put(TRINO_HIVE_TABLE_NUM_FILES, HiveTablePropertyKeys.NUM_FILES)
              .put(TRINO_HIVE_TABLE_EXTERNAL, HiveTablePropertyKeys.EXTERNAL)
              .put(TRINO_HIVE_TABLE_LOCATION, HiveTablePropertyKeys.LOCATION)
              .put(TRINO_HIVE_TABLE_TABLE_TYPE, HiveTablePropertyKeys.TABLE_TYPE)
              .put(TRINO_HIVE_TABLE_INPUT_FORMAT, HiveTablePropertyKeys.INPUT_FORMAT)
              .put(TRINO_HIVE_TABLE_OUTPUT_FORMAT, HiveTablePropertyKeys.OUTPUT_FORMAT)
              .put(TRINO_HIVE_TABLE_SERDE_LIB, HiveTablePropertyKeys.SERDE_LIB)
              .put(TRINO_HIVE_TABLE_SERDE_NAME, HiveTablePropertyKeys.SERDE_NAME)
              .build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
