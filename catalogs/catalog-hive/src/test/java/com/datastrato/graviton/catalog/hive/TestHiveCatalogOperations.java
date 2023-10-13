/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_SIZE;
import static com.datastrato.graviton.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URL;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHiveCatalogOperations {

  @Test
  void testGetCatalogClientPoolMaxsize() {
    Map<String, String> maps = Maps.newHashMap();
    maps.put(CLIENT_POOL_SIZE, "10");
    HiveCatalogOperations op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertEquals(10, op.getCatalogClientPoolMaxsize(maps));

    maps.clear();
    maps.put(CLIENT_POOL_SIZE + "_wrong_mark", "10");
    op = new HiveCatalogOperations(null);
    op.initialize(maps);
    Assertions.assertNotEquals(10, op.getCatalogClientPoolMaxsize(maps));

    maps.put(CLIENT_POOL_SIZE, "1");
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

    // Test If user properties can overwrite the value in hive-site.xml
    properties.clear();
    properties.put("mapreduce.job.reduces", "30");
    hiveCatalogOperations.initialize(properties);
    v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("30", v);

    // Test If Graviton properties can overwrite bypass configuration
    properties.clear();
    properties.put(METASTORE_URL, "hive_url1");
    hiveCatalogOperations.initialize(properties);
    v = hiveCatalogOperations.hiveConf.get(ConfVars.METASTOREURIS.varname);
    Assertions.assertEquals("hive_url1", v);
  }

  @Test
  void testPropertyMeta() {
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations(null);
    hiveCatalogOperations.initialize(Maps.newHashMap());

    Map<String, PropertyEntry<?>> propertyEntryMap =
        hiveCatalogOperations.catalogPropertiesMetadata().propertyEntries();
    Assertions.assertEquals(4, propertyEntryMap.size());
    Assertions.assertTrue(propertyEntryMap.containsKey(METASTORE_URL));
    Assertions.assertTrue(propertyEntryMap.containsKey(Catalog.PROPERTY_PACKAGE));
    Assertions.assertTrue(propertyEntryMap.containsKey(CLIENT_POOL_SIZE));

    Assertions.assertTrue(propertyEntryMap.get(METASTORE_URL).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(Catalog.PROPERTY_PACKAGE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLIENT_POOL_SIZE).isRequired());
  }
}
