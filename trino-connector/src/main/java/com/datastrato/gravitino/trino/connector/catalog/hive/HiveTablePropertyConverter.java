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

public class HiveTablePropertyConverter implements PropertyConverter {

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
              .put("transient_last_ddl_time", "transient_lastDdlTime")
              .put("serde_lib", "serde-lib")
              .put("serde_name", "serde-name")
              .build());

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    Map<String, String> gravitinoToTrino = TRINO_HIVE_TO_GRAVITINO_HIVE.inverseBidiMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String trinoKey = gravitinoToTrino.get(entry.getKey());
      if (trinoKey != null) {
        hiveProperties.put(trinoKey, entry.getValue());
      } else {
        LOG.warn("No mapping for property {} in Hive table", trinoKey);
      }
    }
    return hiveProperties;
  }

  @Override
  public Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    Map<String, Object> hiveProperties = new HashMap<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String gravitinoKey = TRINO_HIVE_TO_GRAVITINO_HIVE.get(entry.getKey());
      if (gravitinoKey != null) {
        hiveProperties.put(gravitinoKey, entry.getValue());
      } else {
        LOG.warn("No mapping for property {} in Hive table", gravitinoKey);
      }
    }
    return hiveProperties;
  }
}
