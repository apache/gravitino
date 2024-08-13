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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestConfigBasedIcebergTableOpsProvider {
  private static final String STORE_PATH =
      "/tmp/gravitino_test_iceberg_jdbc_backend_" + UUID.randomUUID().toString().replace("-", "");

  @Test
  public void testValidIcebergTableOps() {
    String hiveCatalogName = "hive_backend";
    String jdbcCatalogName = "jdbc_backend";
    String defaultCatalogName = IcebergConstants.GRAVITINO_DEFAULT_CATALOG;

    Map<String, String> config = Maps.newHashMap();
    // hive backend catalog
    config.put("catalog.hive_backend.catalog-backend-name", hiveCatalogName);
    config.put("catalog.hive_backend.catalog-backend", "hive");
    config.put("catalog.hive_backend.uri", "thrift://127.0.0.1:9083");
    config.put("catalog.hive_backend.warehouse", "/tmp/usr/hive/warehouse");
    // jdbc backend catalog
    config.put("catalog.jdbc_backend.catalog-backend-name", jdbcCatalogName);
    config.put("catalog.jdbc_backend.catalog-backend", "jdbc");
    config.put(
        "catalog.jdbc_backend.uri",
        String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
    config.put("catalog.jdbc_backend.warehouse", "/tmp/usr/jdbc/warehouse");
    config.put("catalog.jdbc_backend.jdbc.password", "gravitino");
    config.put("catalog.jdbc_backend.jdbc.user", "gravitino");
    config.put("catalog.jdbc_backend.jdbc-driver", "org.h2.Driver");
    config.put("catalog.jdbc_backend.jdbc-initialize", "true");
    // default catalog
    config.put("catalog-backend-name", defaultCatalogName);
    config.put("catalog-backend", "memory");
    config.put("warehouse", "/tmp/");

    ConfigBasedIcebergTableOpsProvider provider = new ConfigBasedIcebergTableOpsProvider();
    provider.initialize(config);

    IcebergConfig hiveIcebergConfig = provider.catalogConfigs.get(hiveCatalogName);
    IcebergConfig jdbcIcebergConfig = provider.catalogConfigs.get(jdbcCatalogName);
    IcebergConfig defaultIcebergConfig = provider.catalogConfigs.get(defaultCatalogName);
    IcebergTableOps hiveOps = provider.getIcebergTableOps(hiveCatalogName);
    IcebergTableOps jdbcOps = provider.getIcebergTableOps(jdbcCatalogName);
    IcebergTableOps defaultOps = provider.getIcebergTableOps(defaultCatalogName);

    Assertions.assertEquals(
        hiveCatalogName, hiveIcebergConfig.get(IcebergConfig.CATALOG_BACKEND_NAME));
    Assertions.assertEquals("hive", hiveIcebergConfig.get(IcebergConfig.CATALOG_BACKEND));
    Assertions.assertEquals(
        "thrift://127.0.0.1:9083", hiveIcebergConfig.get(IcebergConfig.CATALOG_URI));
    Assertions.assertEquals(
        "/tmp/usr/hive/warehouse", hiveIcebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE));

    Assertions.assertEquals(
        jdbcCatalogName, jdbcIcebergConfig.get(IcebergConfig.CATALOG_BACKEND_NAME));
    Assertions.assertEquals("jdbc", jdbcIcebergConfig.get(IcebergConfig.CATALOG_BACKEND));
    Assertions.assertEquals(
        "/tmp/usr/jdbc/warehouse", jdbcIcebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE));
    Assertions.assertEquals("org.h2.Driver", jdbcIcebergConfig.get(IcebergConfig.JDBC_DRIVER));
    Assertions.assertEquals(true, jdbcIcebergConfig.get(IcebergConfig.JDBC_INIT_TABLES));

    Assertions.assertEquals(
        defaultCatalogName, defaultIcebergConfig.get(IcebergConfig.CATALOG_BACKEND_NAME));
    Assertions.assertEquals("memory", defaultIcebergConfig.get(IcebergConfig.CATALOG_BACKEND));
    Assertions.assertEquals("/tmp/", defaultIcebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE));

    Assertions.assertEquals(hiveCatalogName, hiveOps.catalog.name());
    Assertions.assertEquals(jdbcCatalogName, jdbcOps.catalog.name());
    Assertions.assertEquals(defaultCatalogName, defaultOps.catalog.name());

    Assertions.assertTrue(hiveOps.catalog instanceof HiveCatalog);
    Assertions.assertTrue(jdbcOps.catalog instanceof JdbcCatalog);
    Assertions.assertTrue(defaultOps.catalog instanceof InMemoryCatalog);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not_match"})
  public void testInvalidIcebergTableOps(String catalogName) {
    ConfigBasedIcebergTableOpsProvider provider = new ConfigBasedIcebergTableOpsProvider();
    provider.initialize(Maps.newHashMap());

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> provider.getIcebergTableOps(catalogName));
  }
}
