/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.memory;

import static io.trino.spi.session.PropertyMetadata.integerProperty;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class MemoryTableProperties {

  private final List<PropertyMetadata<?>> tablePropertyMetadata;

  // TODO yuhui Need to add table properties
  MemoryTableProperties() {
    tablePropertyMetadata =
        ImmutableList.of(integerProperty("max_ttl", "Max ttl days for the table.", 10, false));
  }

  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return tablePropertyMetadata;
  }
}
