/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveCatalogPropertyConverter {

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveCatalogPropertyConverter.class);

  @Test
  public void testConverter() {
    // You can refer to testHiveCatalogCreatedByGravitino
    HiveCatalogPropertyConverter hiveCatalogPropertyConverter = new HiveCatalogPropertyConverter();
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("hive.immutable-partitions", "true")
            .put("hive.compression-codec", "ZSTD")
            .put("hive.unknown-key", "1")
            .build();

    Map<String, String> re = hiveCatalogPropertyConverter.toTrinoProperties(map);
    Assert.assertEquals(re.get("hive.immutable-partitions"), "true");
    Assert.assertEquals(re.get("hive.compression-codec"), "ZSTD");
    Assert.assertEquals(re.get("hive.unknown-key"), null);
  }

  // To test whether we load jar `bundled-catalog` successfully.
  @Test
  public void testPropertyMetadata() {
    for (Map.Entry<String, PropertyEntry<?>> entryEntry :
        new HiveTablePropertiesMetadata().propertyEntries().entrySet()) {
      LOG.info(entryEntry.getKey() + " " + entryEntry.getValue());
    }
  }
}
