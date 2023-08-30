/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.lakehouse.iceberg.utils;

import com.datastrato.graviton.catalog.lakehouse.iceberg.iceberg.utils.IcebergCatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogUtil {

  @Test
  void testLoadCatalog() {
    Catalog catalog;

    catalog = IcebergCatalogUtil.loadIcebergCatalog("memory");
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog = IcebergCatalogUtil.loadIcebergCatalog("MEMORY");
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    Assertions.assertThrowsExactly(
        RuntimeException.class,
        () -> {
          IcebergCatalogUtil.loadIcebergCatalog("other");
        });
  }
}
