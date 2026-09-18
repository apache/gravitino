/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.provider;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestStaticIcebergConfigProvider {

  @Test
  public void testValidIcebergTableOps() {
    String hiveCatalogName = "hive_backend";
    String jdbcCatalogName = "jdbc_backend";
    String defaultCatalogName = IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG;

    Map<String, String> config = Maps.newHashMap();
    // hive backend catalog
    config.put("catalog.hive_backend.catalog-backend-name", hiveCatalogName);
    config.put("catalog.hive_backend.catalog-backend", "hive");
    config.put("catalog.hive_backend.uri", "thrift://127.0.0.1:9083");
    config.put("catalog.hive_backend.warehouse", "/tmp/usr/hive/warehouse");
    // jdbc backend catalog
    config.put("catalog.jdbc_backend.catalog-backend-name", jdbcCatalogName);
    config.put("catalog.jdbc_backend.catalog-backend", "jdbc");
    config.put("catalog.jdbc_backend.uri", "jdbc:sqlite::memory:");
    config.put("catalog.jdbc_backend.warehouse", "/tmp/usr/jdbc/warehouse");
    config.put("catalog.jdbc_backend.jdbc.password", "gravitino");
    config.put("catalog.jdbc_backend.jdbc.user", "gravitino");
    config.put("catalog.jdbc_backend.jdbc-driver", "org.sqlite.JDBC");
    config.put("catalog.jdbc_backend.jdbc-initialize", "true");
    // default catalog
    config.put("catalog-backend-name", defaultCatalogName);
    config.put("catalog-backend", "memory");
    config.put("warehouse", "/tmp/");

    StaticIcebergConfigProvider provider = new StaticIcebergConfigProvider();
    provider.initialize(config);

    IcebergConfig hiveIcebergConfig = provider.catalogConfigs.get(hiveCatalogName);
    IcebergConfig jdbcIcebergConfig = provider.catalogConfigs.get(jdbcCatalogName);
    IcebergConfig defaultIcebergConfig = provider.catalogConfigs.get(defaultCatalogName);
    IcebergCatalogWrapper hiveOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(hiveCatalogName).get());
    IcebergCatalogWrapper jdbcOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(jdbcCatalogName).get());
    IcebergCatalogWrapper defaultOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(defaultCatalogName).get());

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
    Assertions.assertEquals("org.sqlite.JDBC", jdbcIcebergConfig.get(IcebergConfig.JDBC_DRIVER));
    Assertions.assertEquals(true, jdbcIcebergConfig.get(IcebergConfig.JDBC_INIT_TABLES));

    Assertions.assertEquals(
        defaultCatalogName, defaultIcebergConfig.get(IcebergConfig.CATALOG_BACKEND_NAME));
    Assertions.assertEquals("memory", defaultIcebergConfig.get(IcebergConfig.CATALOG_BACKEND));
    Assertions.assertEquals("/tmp/", defaultIcebergConfig.get(IcebergConfig.CATALOG_WAREHOUSE));

    Assertions.assertEquals(hiveCatalogName, hiveOps.getCatalog().name());
    Assertions.assertEquals(jdbcCatalogName, jdbcOps.getCatalog().name());
    Assertions.assertEquals(defaultCatalogName, defaultOps.getCatalog().name());

    Assertions.assertTrue(hiveOps.getCatalog() instanceof HiveCatalog);
    Assertions.assertTrue(jdbcOps.getCatalog() instanceof JdbcCatalog);
    Assertions.assertTrue(defaultOps.getCatalog() instanceof InMemoryCatalog);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not_match"})
  public void testInvalidIcebergTableOps(String catalogName) {
    StaticIcebergConfigProvider provider = new StaticIcebergConfigProvider();
    provider.initialize(Maps.newHashMap());

    Optional<IcebergConfig> config = provider.getIcebergCatalogConfig(catalogName);
    Assertions.assertEquals(Optional.empty(), config);
  }

  @Test
  public void testTableFormatVersionConfigPerCatalog() {
    Map<String, String> config = Maps.newHashMap();
    // gravitino.iceberg-rest.table-format-version.max, with the prefix already stripped
    config.put(IcebergConstants.TABLE_FORMAT_VERSION_MAX, "3");
    config.put("catalog.v1_catalog." + IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, "1");

    StaticIcebergConfigProvider provider = new StaticIcebergConfigProvider();
    provider.initialize(config);

    IcebergConfig defaultConfig =
        provider.getIcebergCatalogConfig(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG).get();
    Assertions.assertEquals(3, defaultConfig.getMaxTableFormatVersion());
    Assertions.assertEquals(2, defaultConfig.getDefaultTableFormatVersion());

    IcebergConfig v1Config = provider.getIcebergCatalogConfig("v1_catalog").get();
    Assertions.assertEquals(1, v1Config.getDefaultTableFormatVersion());
    Assertions.assertEquals(
        IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION, v1Config.getMaxTableFormatVersion());
  }

  @Test
  public void testInvalidTableFormatVersionConfigFailsAtLoad() {
    Map<String, String> config = Maps.newHashMap();
    config.put("catalog.bad." + IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, "4");
    config.put("catalog.bad." + IcebergConstants.TABLE_FORMAT_VERSION_MAX, "3");

    StaticIcebergConfigProvider provider = new StaticIcebergConfigProvider();
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, () -> provider.initialize(config));
    Assertions.assertTrue(e.getMessage().contains("must not exceed"), e.getMessage());
  }

  /** A catalog with the rest backend forwards requests unchanged, so its bounds are not checked. */
  @Test
  public void testFederatedCatalogIsNotValidatedAtLoad() {
    Map<String, String> config = Maps.newHashMap();
    config.put("catalog.federated." + IcebergConstants.CATALOG_BACKEND, "rest");
    config.put("catalog.federated." + IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT, "4");
    config.put("catalog.federated." + IcebergConstants.TABLE_FORMAT_VERSION_MAX, "3");

    StaticIcebergConfigProvider provider = new StaticIcebergConfigProvider();
    Assertions.assertDoesNotThrow(() -> provider.initialize(config));
    Assertions.assertTrue(provider.getIcebergCatalogConfig("federated").isPresent());
  }
}
