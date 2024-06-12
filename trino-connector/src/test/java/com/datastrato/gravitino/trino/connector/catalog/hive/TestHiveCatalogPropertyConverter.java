/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.datastrato.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogPropertyConverter {

  @Test
  public void testConverter() {
    // You can refer to testHiveCatalogCreatedByGravitino
    HiveCatalogPropertyConverter hiveCatalogPropertyConverter = new HiveCatalogPropertyConverter();
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("trino.bypass.hive.immutable-partitions", "true")
            .put("trino.bypass.hive.compression-codec", "ZSTD")
            .put("trino.bypass.hive.unknown-key", "1")
            .build();

    Map<String, String> re = hiveCatalogPropertyConverter.gravitinoToEngineProperties(map);
    Assertions.assertEquals(re.get("hive.immutable-partitions"), "true");
    Assertions.assertEquals(re.get("hive.compression-codec"), "ZSTD");
    Assertions.assertNull(re.get("hive.unknown-key"));
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
    Assertions.assertTrue(actualGravitinoKeys.containsAll(gravitinoHiveKeys));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildConnectorProperties() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("metastore.uris", "thrift://localhost:9083")
            .put("hive.unknown-key", "1")
            .put("trino.bypass.unknown-key", "1")
            .put("trino.bypass.hive.config.resources", "/tmp/hive-site.xml, /tmp/core-site.xml")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "hive", "test catalog", Catalog.Type.RELATIONAL, properties);
    HiveConnectorAdapter adapter = new HiveConnectorAdapter();
    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(config.get("hive.metastore.uri"), "thrift://localhost:9083");

    // test fixed properties
    Assertions.assertEquals(config.get("hive.security"), "allow-all");

    // test trino passing properties
    Assertions.assertEquals(
        config.get("hive.config.resources"), "/tmp/hive-site.xml, /tmp/core-site.xml");

    // test unknown properties
    Assertions.assertNull(config.get("hive.unknown-key"));
    Assertions.assertNull(config.get("trino.bypass.unknown-key"));
  }
}
