/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Implementation of {@link HasPropertyMeta} for Hive catalog. */
public class HivePropertyMeta implements HasPropertyMeta {

  private static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      ImmutableList.of(
          stringProperty("location", "Hive storage location for the schema", null, false));

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
  public List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return HasPropertyMeta.super.getColumnPropertyMetadata();
  }
}
