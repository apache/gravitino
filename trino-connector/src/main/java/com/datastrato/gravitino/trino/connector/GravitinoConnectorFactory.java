/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.catalog.CatalogInjector;
import com.datastrato.gravitino.trino.connector.system.GravitinoSystemConnector;
import com.datastrato.gravitino.trino.connector.system.storedprocdure.GravitinoStoredProcedureFactory;
import com.datastrato.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;
import java.util.function.Supplier;
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

  @VisibleForTesting
  public CatalogConnectorManager getCatalogConnectorManager() {
    return catalogConnectorManager;
  }

  /**
   * This function call by trino creates a connector. It creates GravitinoSystemConnector at first.
   * Another time's it get GravitinoConnector by CatalogConnectorManager
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
          CatalogConnectorFactory catalogConnectorFactory = new CatalogConnectorFactory();

          catalogConnectorManager =
              new CatalogConnectorManager(catalogInjector, catalogConnectorFactory);
          catalogConnectorManager.config(config);
          catalogConnectorManager.start(clientProvider().get());

          new GravitinoSystemTableFactory(catalogConnectorManager);

        } catch (Exception e) {
          LOG.error("Initialization of the GravitinoConnector failed.", e);
          throw e;
        }
      }
    }

    if (config.isDynamicConnector()) {
      // The dynamic connector is an instance of GravitinoConnector. It is loaded from Gravitino
      // server.
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectorManager.getCatalogConnector(catalogName);
      Preconditions.checkNotNull(catalogConnectorContext, "catalogConnector is not null");
      return catalogConnectorContext.getConnector();
    } else {
      // The static connector is an instance of GravitinoSystemConnector. It is loaded by Trino
      // using the connector configuration.
      String metalake = config.getMetalake();
      if (Strings.isNullOrEmpty(metalake)) {
        throw new TrinoException(GRAVITINO_METALAKE_NOT_EXISTS, "No gravitino metalake selected");
      }
      if (config.simplifyCatalogNames() && !catalogConnectorManager.getUsedMetalakes().isEmpty()) {
        throw new TrinoException(
            GRAVITINO_MISSING_CONFIG,
            "Multiple metalakes are not supported when setting gravitino.simplify-catalog-names = true");
      }
      catalogConnectorManager.addMetalake(metalake);
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory =
          new GravitinoStoredProcedureFactory(catalogConnectorManager, metalake);

      catalogConnectorManager.loadMetalakeSync();
      return new GravitinoSystemConnector(gravitinoStoredProcedureFactory);
    }
  }

  @VisibleForTesting
  Supplier<GravitinoAdminClient> clientProvider() {
    return () -> null;
  }
}
