/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTablePropertyConverter extends PropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTablePropertyConverter.class);
  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_HIVE_TO_GRAVITINO_HIVE =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put("format", "format")
              .put("total_size", "totalSize")
              .put("num_files", "numFiles")
              .put("external", "external")
              .put("location", "location")
              .put("table_type", "table-type")
              .put("input_format", "input-format")
              .put("output_format", "output-format")
              .put("serde_lib", "serde-lib")
              .put("serde_name", "serde-name")
              .build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_HIVE_TO_GRAVITINO_HIVE;
  }
}
