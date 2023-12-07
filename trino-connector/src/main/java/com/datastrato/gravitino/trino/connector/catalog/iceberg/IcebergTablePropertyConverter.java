/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;

public class IcebergTablePropertyConverter extends PropertyConverter {
  // TODO (yuqi) add more properties
  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITON_ICEBERG =
      new TreeBidiMap<>(new ImmutableMap.Builder<String, String>().build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    // Note: As the properties for Iceberg table loaded from Gravitino are always empty currently,
    // no matter what the mapping is, the properties will be empty.
    return TRINO_ICEBERG_TO_GRAVITON_ICEBERG;
  }
}
