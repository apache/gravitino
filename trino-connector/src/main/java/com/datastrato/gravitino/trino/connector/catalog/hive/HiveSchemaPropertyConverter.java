/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;

public class HiveSchemaPropertyConverter extends PropertyConverter {

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>().put("location", "location").build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
