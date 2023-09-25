/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.metadata.GravitonCatalog;
import org.apache.commons.lang3.NotImplementedException;

public class CatalogConnectorFactory {

  private final CatalogInjector catalogInjector;

  public CatalogConnectorFactory(CatalogInjector catalogInjector) {
    this.catalogInjector = catalogInjector;
  }

  public CatalogConnectorContext loadCatalogConnector(
      NameIdentifier nameIdentifier, GravitonMetaLake metalake, GravitonCatalog catalog) {
    throw new NotImplementedException();
  }
}
