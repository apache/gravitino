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

public class HiveTableProperties implements HasProperties {

  public static final HiveTableProperties INSTANCE = new HiveTableProperties();

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

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty("format", "Hive storage format for the table", "TEXTFILE", false),
          stringProperty("total_size", "total size of the table", null, false),
          stringProperty("num_files", "number of files", null, false),
          stringProperty("external", "Indicate whether it is an external table", null, true),
          stringProperty("location", "HDFS location for table storage", null, false),
          stringProperty("table_type", "The type of Hive table", null, false),
          stringProperty(
              "input_format",
              "The input format class for the table",
              "org.apache.hadoop.mapred.TextInputFormat",
              false),
          stringProperty(
              "output_format",
              "The output format class for the table",
              "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
              false),
          stringProperty("transient_last_ddl_time", "Last DDL time", null, false),
          stringProperty(
              "serde_lib",
              "The serde library class for the table",
              "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
              false),
          stringProperty("serde_name", "Name of the serde, table name by default", null, false));

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      hiveProperties.put(
          TRINO_HIVE_TO_GRAVITINO_HIVE.inverseBidiMap().get(entry.getKey()), entry.getValue());
    }
    return hiveProperties;
  }

  @Override
  public Map<String, String> toGravitinoProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      hiveProperties.put(TRINO_HIVE_TO_GRAVITINO_HIVE.get(entry.getKey()), entry.getValue());
    }
    return hiveProperties;
  }

  @Override
  public List<PropertyMetadata<?>> getPropertyMetadata() {
    return TABLE_PROPERTY_META;
  }
}
