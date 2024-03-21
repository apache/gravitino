/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveCatalogPropertyConverter {

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

    Map<String, String> re = hiveCatalogPropertyConverter.gravitinoToEngineProperties(map);
    Assert.assertEquals(re.get("hive.immutable-partitions"), "true");
    Assert.assertEquals(re.get("hive.compression-codec"), "ZSTD");
    Assert.assertEquals(re.get("hive.unknown-key"), null);
  }

  // To test whether we load jar `bundled-catalog` successfully.
  @Test
  public void testPropertyMetadata() {
    Set<String> gravitinoHiveKeys =
        Sets.newHashSet(HiveTablePropertyConverter.TRINO_KEY_TO_GRAVITINO_KEY.values());
    Set<String> actualGravitinoKeys =
        Sets.newHashSet(new HiveTablePropertiesMetadata().propertyEntries().keySet());

    // Needs to confirm whether external should be a property key for Trino.
    gravitinoHiveKeys.remove("external");
    Assert.assertTrue(actualGravitinoKeys.containsAll(gravitinoHiveKeys));
  }
}
