/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_BACKEND;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_URI;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.CATALOG_WAREHOUSE;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

/** Utilities of {@link Catalog} to support catalog management. */
public class CatalogUtils {

  private CatalogUtils() {}

  /**
   * Loads {@link Catalog} instance with given {@link PaimonConfig}.
   *
   * @param paimonConfig The Paimon configuration.
   * @return The {@link Catalog} instance of catalog backend.
   */
  public static Catalog loadCatalogBackend(PaimonConfig paimonConfig) {
    String metastore = paimonConfig.get(CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastore), "Paimon Catalog metastore can not be null or empty.");
    String warehouse = paimonConfig.get(CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(warehouse), "Paimon Catalog warehouse can not be null or empty.");
    if (!PaimonCatalogBackend.FILESYSTEM.name().equalsIgnoreCase(metastore)) {
      String uri = paimonConfig.get(CATALOG_URI);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "Paimon Catalog uri can not be null or empty.");
    }
    CatalogContext catalogContext =
        CatalogContext.create(Options.fromMap(paimonConfig.getAllConfig()));
    return CatalogFactory.createCatalog(catalogContext);
  }
}
