/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.utils;

import static com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergConfig.INITIALIZE_JDBC_CATALOG_TABLES;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  private static InMemoryCatalog loadMemoryCatalog(Map<String, String> properties) {
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> resultProperties = new HashMap<>(properties);
    resultProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "/tmp");
    memoryCatalog.initialize("memory", resultProperties);
    return memoryCatalog;
  }

  private static HiveCatalog loadHiveCatalog(Map<String, String> properties) {
    HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  private static JdbcCatalog loadJdbcCatalog(Map<String, String> properties) {
    JdbcCatalog jdbcCatalog =
        new JdbcCatalog(
            null,
            null,
            Boolean.parseBoolean(
                properties.getOrDefault(
                    INITIALIZE_JDBC_CATALOG_TABLES.getKey(),
                    String.valueOf(INITIALIZE_JDBC_CATALOG_TABLES.getDefaultValue()))));
    jdbcCatalog.setConf(new HdfsConfiguration());
    jdbcCatalog.initialize("jdbc", properties);
    return jdbcCatalog;
  }

  public static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(catalogType, Collections.emptyMap());
  }

  public static Catalog loadCatalogBackend(String catalogType, Map<String, String> properties) {
    // TODO Organize the configuration properties and adapt them to the lower layer, and map some
    // graviton configuration keys.
    LOG.info("Load catalog backend of {}", catalogType);
    switch (catalogType.toLowerCase(Locale.ENGLISH)) {
      case "memory":
        return loadMemoryCatalog(properties);
      case "hive":
        return loadHiveCatalog(properties);
      case "jdbc":
        return loadJdbcCatalog(properties);
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }
}
