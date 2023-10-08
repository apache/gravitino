/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class HiveTableProperties {

  private final List<PropertyMetadata<?>> tableProperties;

  HiveTableProperties() {
    tableProperties =
        ImmutableList.of(
            stringProperty("format", "Hive storage format for the table", "TEXTFILE", false));
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return tableProperties;
  }
}
