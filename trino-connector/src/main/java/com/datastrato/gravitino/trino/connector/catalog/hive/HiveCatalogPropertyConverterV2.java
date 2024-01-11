/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.engine.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import java.util.Map.Entry;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveCatalogPropertyConverterV2 extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the Trino property, value is the Gravitino property
              .put("hive.storage-format", "hive.storage-format")
              .put("hive.compression-codec", "hive.compression-codec")
              .put("hive.config.resources", "hive.config.resources")
              .put("hive.recursive-directories", "hive.recursive-directories")
              .put("hive.ignore-absent-partitions", "hive.ignore-absent-partitions")
              .put("hive.force-local-scheduling", "hive.force-local-scheduling")
              .put("hive.respect-table-format", "hive.respect-table-format")
              .put("hive.immutable-partitions", "hive.immutable-partitions")
              .put(
                  "hive.insert-existing-partitions-behavior",
                  "hive.insert-existing-partitions-behavior")
              .put("hive.target-max-file-size", "hive.target-max-file-size")
              .put("hive.create-empty-bucket-files", "hive.create-empty-bucket-files")
              .put("hive.validate-bucketing", "hive.validate-bucketing")
              .put("hive.partition-statistics-sample-size", "hive.partition-statistics-sample-size")
              .put("hive.max-partitions-per-writers", "hive.max-partitions-per-writers")
              .put("hive.max-partitions-for-eager-load", "hive.max-partitions-for-eager-load")
              .put("hive.max-partitions-per-scan", "hive.max-partitions-per-scan")
              .build());

  public static final TreeBidiMap<PropertyEntry<?>, String> GRAVITINO_PROPERTY_TO_TRINO_PROPERTY =
      new TreeBidiMap<>(
          TRINO_KEY_TO_GRAVITINO_KEY.entrySet().stream()
              .map(Entry::getValue)
              .collect(
                  ImmutableMap.toImmutableMap(
                      key -> PropertyEntry.stringOptionalPropertyEntry(key, key, true, null, false),
                      key -> key)));

  @Override
  public TreeBidiMap<PropertyEntry<?>, String> gravitinoToEngineProperty() {
    return GRAVITINO_PROPERTY_TO_TRINO_PROPERTY;
  }
}
