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

package org.apache.gravitino.trino.connector.catalog.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogPropertyConverter {

  @Test
  public void testConverter() {
    // You can refer to testHiveCatalogCreatedByGravitino
    CatalogPropertyConverter hiveCatalogPropertyConverter = new CatalogPropertyConverter();
    Map<String, String> map =
        ImmutableMap.<String, String>builder()
            .put("trino.bypass.hive.immutable-partitions", "true")
            .put("trino.bypass.hive.compression-codec", "ZSTD")
            .put("trino.bypass.hive.unknown-key", "1")
            .build();

    Map<String, String> re = hiveCatalogPropertyConverter.gravitinoToEngineProperties(map);
    Assertions.assertEquals(re.get("hive.immutable-partitions"), "true");
    Assertions.assertEquals(re.get("hive.compression-codec"), "ZSTD");
    Assertions.assertEquals(re.get("hive.unknown-key"), "1");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildConnectorProperties() throws Exception {
    String name = "test_catalog";
    // trino.bypass properties will be skipped when the catalog properties is defined by Gravitino
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("trino.bypass.hive.security", "skip_value")
            .put("metastore.uris", "thrift://localhost:9083")
            .put("unknown-key", "1")
            .put("trino.bypass.hive.unknown-key", "1")
            .put("trino.bypass.hive.config.resources", "/tmp/hive-site.xml, /tmp/core-site.xml")
            .put("trino.bypass.hive.metastore.uri", "skip_value")
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
    Assertions.assertNull(config.get("unknown-key"));
    Assertions.assertEquals(config.get("hive.unknown-key"), "1");
  }
}
