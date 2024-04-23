/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.utils;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.RESTException;
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
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toLowerCase(), Collections.emptyMap());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toUpperCase(), Collections.emptyMap());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("hive", Collections.emptyMap());
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("HIVE", Collections.emptyMap());
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("jdbc", Collections.emptyMap());
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

    Assertions.assertThrows(
        NullPointerException.class,
        () -> IcebergCatalogUtil.loadCatalogBackend("rest", Collections.emptyMap()));
    properties.put(IcebergConfig.CATALOG_URI.getKey(), "http://localhost:9001/iceberg/");
    properties.put(IcebergConfig.REAL_CATALOG_BACKEND.getKey(), "jdbc");
    properties.put(IcebergConfig.CATALOG_BACKEND_URI.getKey(), "jdbc://0.0.0.0:3306");
    Assertions.assertThrows(
        RESTException.class, () -> IcebergCatalogUtil.loadCatalogBackend("rest", properties));

    catalog = IcebergCatalogUtil.loadCatalogBackend("rest", properties, true);
    Assertions.assertInstanceOf(JdbcCatalog.class, catalog);

    properties.clear();
    properties.put(IcebergConfig.CATALOG_URI.getKey(), "http://localhost:9001/iceberg/");
    properties.put(IcebergConfig.REAL_CATALOG_BACKEND.getKey(), "hive");
    properties.put(IcebergConfig.CATALOG_BACKEND_URI.getKey(), "thrift://localhost:9083");
    properties.put(
        CatalogProperties.WAREHOUSE_LOCATION, "hdfs://%localhost:9083/user/hive/warehouse-hive");
    catalog = IcebergCatalogUtil.loadCatalogBackend("rest", properties, true);
    Assertions.assertInstanceOf(HiveCatalog.class, catalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("other", Collections.emptyMap());
        });
  }
}
