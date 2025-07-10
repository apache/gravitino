/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.Catalog.CLOUD_NAME;
import static org.apache.gravitino.Catalog.CLOUD_REGION_CODE;
import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CHECK_INTERVAL_SEC;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CLIENT_POOL_SIZE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.FETCH_TIMEOUT_SEC;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.IMPERSONATION_ENABLE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.KEY_TAB_URI;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.LIST_ALL_TABLES;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.PRINCIPAL;
import static org.apache.gravitino.catalog.hive.TestHiveCatalog.HIVE_PROPERTIES_METADATA;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHiveCatalogOperations {
  @Test
  void testInitialize() {
    Map<String, String> properties = Maps.newHashMap();
    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations();
    hiveCatalogOperations.initialize(properties, null, HIVE_PROPERTIES_METADATA);
    String v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("10", v);

    // Test If we can override the value in hive-site.xml
    properties.put(CATALOG_BYPASS_PREFIX + "mapreduce.job.reduces", "20");
    hiveCatalogOperations.initialize(properties, null, HIVE_PROPERTIES_METADATA);
    v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("20", v);
  }

  @Test
  void testPropertyMeta() {
    Map<String, PropertyEntry<?>> propertyEntryMap =
        HIVE_PROPERTIES_METADATA.catalogPropertiesMetadata().propertyEntries();

    Assertions.assertEquals(16, propertyEntryMap.size());
    Assertions.assertTrue(propertyEntryMap.containsKey(METASTORE_URIS));
    Assertions.assertTrue(propertyEntryMap.containsKey(Catalog.PROPERTY_PACKAGE));
    Assertions.assertTrue(propertyEntryMap.containsKey(BaseCatalog.CATALOG_OPERATION_IMPL));
    Assertions.assertTrue(propertyEntryMap.containsKey(PROPERTY_IN_USE));
    Assertions.assertTrue(propertyEntryMap.containsKey(AUTHORIZATION_PROVIDER));
    Assertions.assertTrue(propertyEntryMap.containsKey(CLIENT_POOL_SIZE));
    Assertions.assertTrue(propertyEntryMap.containsKey(IMPERSONATION_ENABLE));
    Assertions.assertTrue(propertyEntryMap.containsKey(LIST_ALL_TABLES));
    Assertions.assertTrue(propertyEntryMap.get(METASTORE_URIS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(Catalog.PROPERTY_PACKAGE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLIENT_POOL_SIZE).isRequired());
    Assertions.assertFalse(
        propertyEntryMap.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(IMPERSONATION_ENABLE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(KEY_TAB_URI).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(PRINCIPAL).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CHECK_INTERVAL_SEC).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(FETCH_TIMEOUT_SEC).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_NAME).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_NAME).isImmutable());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_REGION_CODE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_REGION_CODE).isImmutable());
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
    op.initialize(maps, null, HIVE_PROPERTIES_METADATA);

    Assertions.assertEquals("v2", op.hiveConf.get("a.b"));
    Assertions.assertEquals("v4", op.hiveConf.get("c.d"));
  }

  @Test
  void testEmptyBypassKey() {
    Map<String, String> properties = Maps.newHashMap();
    // Add a normal bypass configuration
    properties.put(CATALOG_BYPASS_PREFIX + "mapreduce.job.reduces", "20");
    // Add an empty bypass configuration
    properties.put(CATALOG_BYPASS_PREFIX, "some-value");

    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations();
    hiveCatalogOperations.initialize(properties, null, HIVE_PROPERTIES_METADATA);

    // Verify that the normal bypass configuration is correctly applied
    String v = hiveCatalogOperations.hiveConf.get("mapreduce.job.reduces");
    Assertions.assertEquals("20", v);

    // Verify that the empty bypass configuration is not applied
    // This will fail if the empty key is incorrectly added
    Assertions.assertNull(hiveCatalogOperations.hiveConf.get(""));
  }

  @Test
  void testTestConnection() throws TException, InterruptedException {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.clientPool = mock(CachedClientPool.class);
    when(op.clientPool.run(any())).thenThrow(new TException("mock connection exception"));

    ConnectionFailedException exception =
        Assertions.assertThrows(
            ConnectionFailedException.class,
            () ->
                op.testConnection(
                    NameIdentifier.of("metalake", "catalog"),
                    Catalog.Type.RELATIONAL,
                    "hive",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertEquals(
        "Failed to run getAllDatabases in Hive Metastore: mock connection exception",
        exception.getMessage());
  }
}
