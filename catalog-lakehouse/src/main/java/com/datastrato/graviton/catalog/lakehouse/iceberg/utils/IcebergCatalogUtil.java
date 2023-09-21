/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.utils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;

public class IcebergCatalogUtil {

  private static InMemoryCatalog loadMemoryCatalog() {
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> properties = new HashMap<>();

    memoryCatalog.initialize("memory", properties);
    return memoryCatalog;
  }

  private static HiveCatalog loadHiveCatalog() {
    HiveCatalog hiveCatalog = new HiveCatalog();
    Map<String, String> properties = new HashMap<>();
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  private static JdbcCatalog loadJdbcCatalog() {
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    Map<String, String> properties = new HashMap<>();
    jdbcCatalog.initialize("jdbc", properties);
    return jdbcCatalog;
  }

  public static Catalog loadIcebergCatalog(String catalogType) {
    switch (catalogType.toLowerCase(Locale.ENGLISH)) {
      case "memory":
        return loadMemoryCatalog();
      case "hive":
        return loadHiveCatalog();
      case "jdbc":
        return loadJdbcCatalog();
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }
}
