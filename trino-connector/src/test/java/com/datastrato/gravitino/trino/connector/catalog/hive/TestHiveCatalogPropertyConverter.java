/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import java.util.Map;
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

    Map<String, String> re = hiveCatalogPropertyConverter.toTrinoProperties(map);
    Assert.assertEquals(re.get("hive.immutable-partitions"), "true");
    Assert.assertEquals(re.get("hive.compression-codec"), "ZSTD");
    Assert.assertEquals(re.get("hive.unknown-key"), null);
  }
}
