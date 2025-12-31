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
package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.gravitino.catalog.hive.HiveConstants;
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
        properties.get(HiveConstants.METASTORE_URIS),
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

  @Test
  public void testToGravitinoTablePropertiesWithoutSerdeLib() {
    // Test that serde-lib is set to LazySimpleSerDe when not explicitly provided.
    // This default applies ONLY to Hive tables created through the Flink connector
    // (issue #9508). Other Hive usage paths (Spark, Trino, direct API) are
    // unaffected.
    Map<String, String> flinkProperties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Map<String, String> gravitinoProperties = CONVERTER.toGravitinoTableProperties(flinkProperties);

    Assertions.assertEquals(3, gravitinoProperties.size());
    Assertions.assertEquals("value1", gravitinoProperties.get("key1"));
    Assertions.assertEquals("value2", gravitinoProperties.get("key2"));
    HiveConf hiveConf = new HiveConf(false);
    String expectedSerde = hiveConf.get("hive.default.serde");

    Assertions.assertEquals(
        expectedSerde,
        gravitinoProperties.get(HiveConstants.SERDE_LIB),
        "serde-lib should be set to Hive default SerDe for Flink-Hive interoperability");
  }

  @Test
  public void testToGravitinoTablePropertiesWithSerdeLib() {
    // Test that explicitly provided serde-lib is preserved
    String customSerde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
    Map<String, String> flinkProperties =
        ImmutableMap.of("key1", "value1", HiveConstants.SERDE_LIB, customSerde);
    Map<String, String> gravitinoProperties = CONVERTER.toGravitinoTableProperties(flinkProperties);

    Assertions.assertEquals(2, gravitinoProperties.size());
    Assertions.assertEquals("value1", gravitinoProperties.get("key1"));
    Assertions.assertEquals(
        customSerde,
        gravitinoProperties.get(HiveConstants.SERDE_LIB),
        "Explicitly provided serde-lib should be preserved");
  }
}
