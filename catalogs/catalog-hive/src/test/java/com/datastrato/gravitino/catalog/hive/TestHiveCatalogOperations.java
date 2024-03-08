/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CHECK_INTERVAL_SEC;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.CLIENT_POOL_SIZE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.FETCH_TIMEOUT_SEC;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.KET_TAB_URI;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.PRINCIPAL;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.PropertyEntry;
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
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(maps, null);
    Assertions.assertEquals(10, op.getClientPoolSize(maps));

    maps.clear();
    maps.put(CLIENT_POOL_SIZE + "_wrong_mark", "10");
    op = new HiveCatalogOperations();
    op.initialize(maps, null);
    Assertions.assertNotEquals(10, op.getClientPoolSize(maps));

    maps.put(CLIENT_POOL_SIZE, "1");
    op = new HiveCatalogOperations();
    op.initialize(maps, null);
    Assertions.assertEquals(1, op.getClientPoolSize(maps));
  }

  @Test
  void testInitialize() throws NoSuchFieldException, IllegalAccessException {
    Map<String, String> properties = Maps.newHashMap();
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations();
    hiveCatalogOperations.initialize(properties, null);
    String v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("10", v);

    // Test If we can override the value in hive-site.xml
    properties.put(CATALOG_BYPASS_PREFIX + "mapreduce.job.reduces", "20");
    hiveCatalogOperations.initialize(properties, null);
    v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("20", v);
  }

  @Test
  void testPropertyMeta() {
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations();
    hiveCatalogOperations.initialize(Maps.newHashMap(), null);

    Map<String, PropertyEntry<?>> propertyEntryMap =
        hiveCatalogOperations.catalogPropertiesMetadata().propertyEntries();
    Assertions.assertEquals(12, propertyEntryMap.size());
    Assertions.assertTrue(propertyEntryMap.containsKey(METASTORE_URIS));
    Assertions.assertTrue(propertyEntryMap.containsKey(Catalog.PROPERTY_PACKAGE));
    Assertions.assertTrue(propertyEntryMap.containsKey(BaseCatalog.CATALOG_OPERATION_IMPL));
    Assertions.assertTrue(propertyEntryMap.containsKey(CLIENT_POOL_SIZE));
    Assertions.assertTrue(propertyEntryMap.containsKey(IMPERSONATION_ENABLE));

    Assertions.assertTrue(propertyEntryMap.get(METASTORE_URIS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(Catalog.PROPERTY_PACKAGE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLIENT_POOL_SIZE).isRequired());
    Assertions.assertFalse(
        propertyEntryMap.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(IMPERSONATION_ENABLE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(KET_TAB_URI).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(PRINCIPAL).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CHECK_INTERVAL_SEC).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(FETCH_TIMEOUT_SEC).isRequired());
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
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(maps, null);

    Assertions.assertEquals("v2", op.hiveConf.get("a.b"));
    Assertions.assertEquals("v4", op.hiveConf.get("c.d"));
  }
}
