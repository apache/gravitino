/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.memory;

import static io.trino.spi.session.PropertyMetadata.integerProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

public class MemoryPropertyMeta implements HasPropertyMeta {

  public static final List<PropertyMetadata<?>> MEMORY_TABLE_PROPERTY =
      ImmutableList.of(integerProperty("max_ttl", "Max ttl days for the table.", 10, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return MEMORY_TABLE_PROPERTY;
  }
}
