/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import io.trino.plugin.graviton.metadata.GravitonCatalog;
import org.apache.commons.lang3.NotImplementedException;

public class CatalogConnectorFactory {

  private final CatalogInjector catalogInjector;

  public CatalogConnectorFactory(CatalogInjector catalogInjector) {
    this.catalogInjector = catalogInjector;
  }

  public CatalogConnector loadCatalogConnector(
      NameIdentifier nameIdentifier, GravitonMetaLake metalake, GravitonCatalog catalog) {
    throw new NotImplementedException();
  }
}
