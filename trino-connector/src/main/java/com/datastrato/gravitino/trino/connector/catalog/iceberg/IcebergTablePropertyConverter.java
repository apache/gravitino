/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;

public class IcebergTablePropertyConverter extends PropertyConverter {
  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITON_ICEBERG =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put("location", "location")
              .put("comment", "comment")
              .put("creator", "creator")
              .put("current-snapshot-id", "current-snapshot-id")
              .put("cherry-pick-snapshot-id", "cherry-pick-snapshot-id")
              .put("sort-order", "sort-order")
              .put("identifier-fields", "identifier-fields")
              .build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_ICEBERG_TO_GRAVITON_ICEBERG;
  }
}
