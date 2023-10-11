/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class HiveSchemaProperties {

  private final List<PropertyMetadata<?>> schemaProperties;

  // TODO yuhui Need to add table properties
  HiveSchemaProperties() {
    schemaProperties =
        ImmutableList.of(
            stringProperty("location", "Hive storage location for the schema", "", false));
  }

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return schemaProperties;
  }
}
