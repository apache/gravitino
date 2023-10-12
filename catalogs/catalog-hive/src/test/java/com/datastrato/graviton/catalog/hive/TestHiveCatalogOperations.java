/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.CATALOG_CLIENT_POOL_MAXSIZE;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHiveCatalogOperations {

  @Test
  void testGetCatalogClientPoolMaxsize() {
    Map<String, String> maps = Maps.newHashMap();
    maps.put(CATALOG_CLIENT_POOL_MAXSIZE, "10");
    HiveCatalogOperations op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertEquals(10, op.getCatalogClientPoolMaxsize(maps));

    maps.clear();
    maps.put(CATALOG_CLIENT_POOL_MAXSIZE + "_wrong_mark", "10");
    op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertNotEquals(10, op.getCatalogClientPoolMaxsize(maps));

    maps.put(CATALOG_CLIENT_POOL_MAXSIZE, "1");
    op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertEquals(1, op.getCatalogClientPoolMaxsize(maps));
  }

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
