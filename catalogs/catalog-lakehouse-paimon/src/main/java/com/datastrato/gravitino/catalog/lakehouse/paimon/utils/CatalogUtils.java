/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.METASTORE;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import java.util.Locale;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

/** Utilities of {@link Catalog} to support catalog management. */
public class CatalogUtils {

  /**
   * Loads {@link Catalog} instance with given {@link PaimonConfig}.
   *
   * @param paimonConfig The Paimon configuration.
   * @return The {@link Catalog} instance of catalog backend.
   */
  public static Catalog loadCatalogBackend(PaimonConfig paimonConfig) {
    String metastore = paimonConfig.get(METASTORE).toLowerCase(Locale.ROOT);
    paimonConfig.set(METASTORE, metastore);
    CatalogContext catalogContext =
        CatalogContext.create(Options.fromMap(paimonConfig.getAllConfig()));
    return CatalogFactory.createCatalog(catalogContext);
  }
}
