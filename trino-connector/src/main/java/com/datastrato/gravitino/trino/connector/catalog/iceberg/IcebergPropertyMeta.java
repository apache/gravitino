/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import static io.trino.spi.type.VarcharType.VARCHAR;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.List;
import java.util.Map;

public class IcebergPropertyMeta implements HasPropertyMeta {

  public static final String ICEBERG_PARTITIONING_PROPERTY = "partitioning";
  public static final String ICEBERG_SORTED_BY_PROPERTY = "sorted_by";
  public static final String ICEBERG_LOCATION_PROPERTY = "location";

  // Value is whether this property is reserved and cannot be used by users
  // TODO (yuqi) add more properties
  public static final Map<PropertyMetadata<?>, Boolean> TABLE_PROPERTY_TO_RESERVED_MAP =
      new ImmutableMap.Builder().build();

  public static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          new PropertyMetadata<>(
              ICEBERG_PARTITIONING_PROPERTY,
              "Partition transforms",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value),
          new PropertyMetadata<>(
              ICEBERG_SORTED_BY_PROPERTY,
              "Sorted columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value -> (List<?>) value,
              value -> value),
          PropertyMetadata.stringProperty(
              ICEBERG_LOCATION_PROPERTY, "Location for table storage", null, false));

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
