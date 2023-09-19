/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

import io.trino.plugin.graviton.catalog.CatalogConnector;
import io.trino.plugin.graviton.catalog.CatalogConnectorFactory;
import io.trino.plugin.graviton.catalog.CatalogConnectorManager;
import io.trino.plugin.graviton.catalog.CatalogInjector;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class GravitonConnectorFactory implements ConnectorFactory {
  private static final String DEFAULT_CONNECTOR_NAME = "graviton";
  private CatalogConnectorManager catalogConnectorManager;

  @Override
  public String getName() {
    return DEFAULT_CONNECTOR_NAME;
  }

  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    requireNonNull(requiredConfig, "requiredConfig is null");
    checkStrictSpiVersionMatch(context, this);

    synchronized (this) {
      if (catalogConnectorManager == null) {
        try {
          CatalogInjector catalogInjector = new CatalogInjector();
          catalogInjector.bindCatalogManager(context);
          CatalogConnectorFactory catalogConnectorFactory =
              new CatalogConnectorFactory(catalogInjector);

          GravitonConfig config = new GravitonConfig(requiredConfig);
          catalogConnectorManager =
              new CatalogConnectorManager(catalogInjector, catalogConnectorFactory);
          catalogConnectorManager.config(config);
          catalogConnectorManager.start();

        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Default GravitonConnector named "graviton" is just using to load CatalogConnectorManager,
        // that's dynamically loading catalogs from graviton server.
        return new DummyGravitonConnector();
      }
    }

    requireNonNull(catalogConnectorManager, "catalogConnectorManager is null");

    CatalogConnector catalogConnector = catalogConnectorManager.getCatalogConnector(catalogName);
    requireNonNull(catalogConnector, "catalogConnector is null");
    requireNonNull(catalogConnector.getConnector(), "catalogConnector is null");
    requireNonNull(catalogConnector.getInternalConnector(), "catalogConnector is null");

    return catalogConnector.getConnector();
  }
}
