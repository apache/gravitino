/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.gravitino.trino.connector;

import com.apache.gravitino.client.GravitinoAdminClient;
import com.apache.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import com.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.apache.gravitino.trino.connector.catalog.CatalogRegister;
import com.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import com.apache.gravitino.trino.connector.system.storedprocdure.GravitinoStoredProcedureFactory;
import com.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
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
  public static final String DEFAULT_CONNECTOR_NAME = "gravitino";

  @SuppressWarnings("UnusedVariable")
  private GravitinoSystemTableFactory gravitinoSystemTableFactory;

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
   * This function call by Trino creates a connector. It creates GravitinoSystemConnector at first.
   * Another time's it get GravitinoConnector by CatalogConnectorManager
   *
   * @param catalogName the connector name of catalog
   * @param requiredConfig the config of connector
   * @param context Trino connector context
   * @return Trino connector
   */
  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    Preconditions.checkArgument(requiredConfig != null, "requiredConfig is not null");
    GravitinoConfig config = new GravitinoConfig(requiredConfig);

    synchronized (this) {
      if (catalogConnectorManager == null) {
        try {
          CatalogRegister catalogRegister = new CatalogRegister();
          CatalogConnectorFactory catalogConnectorFactory = new CatalogConnectorFactory();

          catalogConnectorManager =
              new CatalogConnectorManager(catalogRegister, catalogConnectorFactory);
          catalogConnectorManager.config(config, clientProvider().get());
          catalogConnectorManager.start(context);

          gravitinoSystemTableFactory = new GravitinoSystemTableFactory(catalogConnectorManager);
        } catch (Exception e) {
          String message = "Initialization of the GravitinoConnector failed" + e.getMessage();
          LOG.error(message);
          throw new TrinoException(GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR, message, e);
        }
      }
    }

    if (config.isDynamicConnector()) {
      // The dynamic connector is an instance of GravitinoConnector. It is loaded from Gravitino
      // server.
      return catalogConnectorManager.createConnector(catalogName, config, context);
    } else {
      // The static connector is an instance of GravitinoSystemConnector. It is loaded by Trino
      // using the connector configuration.
      String metalake = config.getMetalake();
      if (Strings.isNullOrEmpty(metalake)) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS, "No gravitino metalake selected");
      }
      if (config.simplifyCatalogNames() && !catalogConnectorManager.getUsedMetalakes().isEmpty()) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_MISSING_CONFIG,
            "Multiple metalakes are not supported when setting gravitino.simplify-catalog-names = true");
      }
      catalogConnectorManager.addMetalake(metalake);
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory =
          new GravitinoStoredProcedureFactory(catalogConnectorManager, metalake);
      return new GravitinoSystemConnector(gravitinoStoredProcedureFactory);
    }
  }

  @VisibleForTesting
  Supplier<GravitinoAdminClient> clientProvider() {
    return () -> null;
  }
}
