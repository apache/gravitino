/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convert hive properties between trino and gravitino. */
public class HiveCatalogPropertyConverter implements PropertyConverter {

  public static final Logger LOG = LoggerFactory.getLogger(HiveCatalogPropertyConverter.class);

  private static final TreeBidiMap<String, String> GRAVITINO_HIVE_TO_TRINO_HIVE =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the trino property, value is the gravitino property
              .put("hive.storage-format", "hive.storage-format")
              .put("hive.compression-codec", "hive.compression-codec")
              .put("hive.config.resources", "hive.config.resources")
              .put("hive.recursive-directories", "hive.ignore-absent-partitions")
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

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (GRAVITINO_HIVE_TO_TRINO_HIVE.containsKey(entry.getKey())) {
        builder.put(GRAVITINO_HIVE_TO_TRINO_HIVE.get(entry.getKey()), entry.getValue());
      } else {
        // Let other properties pass through
        LOG.warn("No mapping for property {} in Hive catalog", entry.getKey());
      }
    }

    return builder.build();
  }
}
