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

package org.apache.gravitino.iceberg.common.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogUtil {

  @Test
  void testLoadCatalog() {
    Catalog catalog;

    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toLowerCase());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toUpperCase());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("hive");
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("HIVE");
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("jdbc");
        });

    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "test");
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "false");
    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));
    Assertions.assertTrue(catalog instanceof JdbcCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("other");
        });
  }
}
