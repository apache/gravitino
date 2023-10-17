/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_SIZE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHiveCatalogOperations {

  @Test
  void testGetClientPoolSize() {
    Map<String, String> maps = Maps.newHashMap();
    maps.put(CLIENT_POOL_SIZE, "10");
    HiveCatalogOperations op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertEquals(10, op.getClientPoolSize(maps));

    maps.clear();
    maps.put(CLIENT_POOL_SIZE + "_wrong_mark", "10");
    op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertNotEquals(10, op.getClientPoolSize(maps));

    maps.put(CLIENT_POOL_SIZE, "1");
    op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertEquals(1, op.getClientPoolSize(maps));
  }

  @Test
  void testInitialize() throws NoSuchFieldException, IllegalAccessException {
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
  }

  @Test
  void testPropertyMeta() {
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations(null);
    hiveCatalogOperations.initialize(Maps.newHashMap());

    Map<String, PropertyEntry<?>> propertyEntryMap =
        hiveCatalogOperations.catalogPropertiesMetadata().propertyEntries();
    Assertions.assertEquals(4, propertyEntryMap.size());
    Assertions.assertTrue(propertyEntryMap.containsKey(METASTORE_URIS));
    Assertions.assertTrue(propertyEntryMap.containsKey(Catalog.PROPERTY_PACKAGE));
    Assertions.assertTrue(propertyEntryMap.containsKey(CLIENT_POOL_SIZE));

    Assertions.assertTrue(propertyEntryMap.get(METASTORE_URIS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(Catalog.PROPERTY_PACKAGE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLIENT_POOL_SIZE).isRequired());
  }

  @Test
  void testPropertyOverwrite() {
    Map<String, String> maps = Maps.newHashMap();
    maps.put("a.b", "v1");
    maps.put(CATALOG_BYPASS_PREFIX + "a.b", "v2");

    maps.put("c.d", "v3");
    maps.put(CATALOG_BYPASS_PREFIX + "c.d", "v4");
    maps.put("e.f", "v5");

    maps.put(METASTORE_URIS, "url1");
    maps.put(ConfVars.METASTOREURIS.varname, "url2");
    maps.put(CATALOG_BYPASS_PREFIX + ConfVars.METASTOREURIS.varname, "url3");
    HiveCatalogOperations op = new HiveCatalogOperations(null);
    op.initialize(maps);

    Assertions.assertEquals("v2", op.hiveConf.get("a.b"));
    Assertions.assertEquals("v4", op.hiveConf.get("c.d"));
  }
}
