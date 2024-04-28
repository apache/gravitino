/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.utils;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import java.util.HashMap;
import java.util.Map;
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
    properties.put(CatalogProperties.URI, "jdbc://0.0.0.0:3306");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "test");
    properties.put(IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_PASSWORD, "test");
    properties.put(IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_INITIALIZE, "false");
    catalog = IcebergCatalogUtil.loadCatalogBackend("jdbc", properties);
    Assertions.assertTrue(catalog instanceof JdbcCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("other");
        });
  }
}
