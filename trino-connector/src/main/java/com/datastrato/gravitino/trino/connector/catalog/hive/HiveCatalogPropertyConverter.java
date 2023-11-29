/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Convert hive properties between trino and gravitino. */
public class HiveCatalogPropertyConverter implements PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_HIVE_TO_GRAVITINO_HIVE =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the trino property, value is the gravitino property
              .put("hive.storage-format", "hive.storage-format")
              .put("hive.compression-codec", "hive.compression-codec")
              .build());

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (TRINO_HIVE_TO_GRAVITINO_HIVE.inverseBidiMap().containsKey(entry.getKey())) {
        builder.put(
            TRINO_HIVE_TO_GRAVITINO_HIVE.inverseBidiMap().get(entry.getKey()), entry.getValue());
      } else {
        // Let other properties pass through
        builder.put(entry.getKey(), entry.getValue());
      }
    }

    return builder.build();
  }
}
