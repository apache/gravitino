/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.function.Consumer;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.factories.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for {@link CatalogUtils}. */
public class TestCatalogUtils {

  @Test
  void testLoadCatalogBackend() throws Exception {
    // Test load FileSystemCatalog for filesystem metastore.
    assertCatalog(PaimonCatalogBackend.FILESYSTEM.name(), FileSystemCatalog.class);
    // Test load catalog exception for other metastore.
    assertThrowsExactly(FactoryException.class, () -> assertCatalog("other", catalog -> {}));
  }

  private void assertCatalog(String metastore, Class<?> expected) throws Exception {
    assertCatalog(
        metastore.toLowerCase(Locale.ROOT), catalog -> assertEquals(expected, catalog.getClass()));
  }

  private void assertCatalog(String metastore, Consumer<Catalog> consumer) throws Exception {
    try (Catalog catalog =
        loadCatalogBackend(
            new PaimonConfig(
                ImmutableMap.of(
                    PaimonConfig.CATALOG_BACKEND.getKey(),
                    metastore,
                    PaimonConfig.CATALOG_WAREHOUSE.getKey(),
                    "/tmp/paimon_catalog_warehouse",
                    PaimonConfig.CATALOG_URI.getKey(),
                    "uri")))) {
      consumer.accept(catalog);
    }
  }
}
