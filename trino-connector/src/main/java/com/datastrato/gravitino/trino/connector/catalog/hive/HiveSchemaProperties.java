/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasProperties;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

public class HiveSchemaProperties implements HasProperties {

  public static final HiveSchemaProperties INSTANCE = new HiveSchemaProperties();

  // TODO yuqi Need to improve schema properties
  private static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      ImmutableList.of(
          stringProperty("location", "Hive storage location for the schema", null, false));

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return SCHEMA_PROPERTY_META;
  }

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    return HasProperties.super.toTrinoProperties(properties);
  }

  @Override
  public Map<String, String> toGravitinoProperties(Map<String, String> properties) {
    return HasProperties.super.toGravitinoProperties(properties);
  }

  @Override
  public List<PropertyMetadata<?>> getPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }
}
