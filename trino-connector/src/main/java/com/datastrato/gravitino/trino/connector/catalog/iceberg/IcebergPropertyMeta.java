/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

public class IcebergPropertyMeta implements HasPropertyMeta {

  // Value is whether this property is reserved and cannot be used by users
  // TODO (yuqi) add more properties
  public static final Map<PropertyMetadata<?>, Boolean> TABLE_PROPERTY_TO_RESERVED_MAP =
      new ImmutableMap.Builder().build();

  public static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      TABLE_PROPERTY_TO_RESERVED_MAP.entrySet().stream()
          .map(Map.Entry::getKey)
          .collect(ImmutableList.toImmutableList());

  // TODO (yuqi) add more properties
  public static final Map<PropertyMetadata<?>, Boolean> SCHEMA_PROPERTY_TO_RESERVED_MAP =
      new ImmutableMap.Builder().build();

  public static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      SCHEMA_PROPERTY_TO_RESERVED_MAP.entrySet().stream()
          .map(Map.Entry::getKey)
          .collect(ImmutableList.toImmutableList());

  @Override
  public List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }
}
