/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHivePropertiesConverter {

  private static final HivePropertiesConverter CONVERTER = HivePropertiesConverter.INSTANCE;

  @Test
  public void testToGravitinoCatalogProperties() {
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                "hive-conf-dir",
                "src/test/resources/flink-tests",
                "flink.bypass.key",
                "value",
                HiveConf.ConfVars.METASTOREURIS.varname,
                "thrift://127.0.0.1:9084"));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(3, properties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        properties.get("flink.bypass.hive-conf-dir"),
        "This will add the prefix");
    Assertions.assertEquals(
        "value", properties.get("flink.bypass.key"), "The prefix have already existed");
    Assertions.assertEquals(
        "thrift://127.0.0.1:9084",
        properties.get(METASTORE_URIS),
        "The key is converted to Gravitino Config");
  }

  @Test
  public void testToFlinkCatalogProperties() {
    Map<String, String> catalogProperties =
        ImmutableMap.of("flink.bypass.key", "value", "metastore.uris", "thrift://xxx");
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(3, flinkCatalogProperties.size());
    Assertions.assertEquals("value", flinkCatalogProperties.get("key"));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals("thrift://xxx", flinkCatalogProperties.get("hive.metastore.uris"));
  }
}
