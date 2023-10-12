/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHiveCatalogOperations {
  @Test
  void testInitialize() {
    Map<String, String> properties = Maps.newHashMap();
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations(null);
    hiveCatalogOperations.initialize(properties);
    String v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("10", v);

    // Test If we can override the value in hive-site.xml
    properties.put(CATALOG_BYPASS_PREFIX + "mapreduce.job.reduces", "20");
    hiveCatalogOperations.initialize(properties);
    v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("20", v);

    // Test If user properties can override the value in hive-site.xml
    properties.clear();
    properties.put("mapreduce.job.reduces", "30");
    hiveCatalogOperations.initialize(properties);
    v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("30", v);
  }
}
