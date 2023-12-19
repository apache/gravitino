/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.catalog.CatalogInjector;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoConnectorFactory implements ConnectorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoConnectorFactory.class);
  private static final String DEFAULT_CONNECTOR_NAME = "gravitino";

  private CatalogConnectorManager catalogConnectorManager;

  @Override
  public String getName() {
    return DEFAULT_CONNECTOR_NAME;
  }

  /**
   * This function call by trino creates a connector. It creates DummyGravitinoConnector at first.
   * Another time's it get GravitinoConnector by CatalogConnectorManger
   *
   * @param catalogName the connector name of catalog
   * @param requiredConfig the config of connector
   * @param context trino connector context
   * @return trino connector
   */
  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    Preconditions.checkArgument(requiredConfig != null, "requiredConfig is not null");
    GravitinoConfig config = new GravitinoConfig(requiredConfig);

    synchronized (this) {
      if (catalogConnectorManager == null) {
        try {
          CatalogInjector catalogInjector = new CatalogInjector();
          catalogInjector.init(context);
          CatalogConnectorFactory catalogConnectorFactory =
              new CatalogConnectorFactory(catalogInjector);

          catalogConnectorManager =
              new CatalogConnectorManager(catalogInjector, catalogConnectorFactory);
          catalogConnectorManager.config(config);

          // For testing
          if (GravitinoPlugin.gravitinoClient != null) {
            catalogConnectorManager.setGravitinoClient(GravitinoPlugin.gravitinoClient);
            GravitinoPlugin.catalogConnectorManager = catalogConnectorManager;
          }
          catalogConnectorManager.start();

        } catch (Exception e) {
          LOG.error("Initialization of the GravitinoConnector failed.", e);
          throw e;
        }
      }
    }

    if (config.isStatic()) {
      // Default GravitinoConnector named "gravitino" is just using to load
      // CatalogConnectorManager,
      // that's dynamically loading catalogs from gravitino server.

      String metalake = config.getMetalake();
      if (Strings.isNullOrEmpty(metalake)) {
        throw new TrinoException(GRAVITINO_METALAKE_NOT_EXISTS, "No gravitino metalake selected");
      }
      catalogConnectorManager.addMetalake(metalake);

      return new DummyGravitinoConnector(catalogConnectorManager);
    } else {
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectorManager.getCatalogConnector(catalogName);
      Preconditions.checkNotNull(catalogConnectorContext, "catalogConnector is not null");
      return catalogConnectorContext.getConnector();
    }
  }
}
