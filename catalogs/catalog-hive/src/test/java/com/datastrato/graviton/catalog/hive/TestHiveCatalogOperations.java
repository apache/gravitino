/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.CATALOG_CLIENT_POOL_MAXSIZE;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogOperations {

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
}
