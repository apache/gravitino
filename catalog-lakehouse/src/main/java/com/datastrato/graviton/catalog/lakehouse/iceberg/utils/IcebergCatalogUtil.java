/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.utils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;

public class IcebergCatalogUtil {

  private static InMemoryCatalog loadMemoryCatalog() {
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> properties = new HashMap<>();

    memoryCatalog.initialize("memory", properties);
    return memoryCatalog;
  }

  public static Catalog loadIcebergCatalog(String catalogType) {
    switch (catalogType.toLowerCase(Locale.ENGLISH)) {
      case "memory":
        return loadMemoryCatalog();
        // todo: add hive, jdbc catalog
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }
}
