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
package org.apache.gravitino.trino.connector;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogRegister;
import org.apache.gravitino.trino.connector.catalog.DefaultCatalogConnectorFactory;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gravitino connector factory. */
public class GravitinoConnectorFactory implements ConnectorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoConnectorFactory.class);
  /** The default connector name. */
  public static final String DEFAULT_CONNECTOR_NAME = "gravitino";

  @SuppressWarnings("UnusedVariable")
  private GravitinoSystemTableFactory gravitinoSystemTableFactory;

  private CatalogConnectorManager catalogConnectorManager;

  @Override
  public String getName() {
    return DEFAULT_CONNECTOR_NAME;
  }

  /**
   * Retrieves the catalog connector manager.
   *
   * @return the catalog connector manager
   */
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

          CatalogConnectorFactory catalogConnectorFactory = createCatalogConnectorFactory(config);
          catalogConnectorManager =
              new CatalogConnectorManager(catalogRegister, catalogConnectorFactory);
          catalogConnectorManager.config(config, clientProvider().get());
          catalogConnectorManager.start(context);

          gravitinoSystemTableFactory = new GravitinoSystemTableFactory(catalogConnectorManager);
        } catch (Exception e) {
          String message = "Initialization of the GravitinoConnector failed " + e.getMessage();
          LOG.error(message);
          throw new TrinoException(GRAVITINO_RUNTIME_ERROR, message, e);
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
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory =
          new GravitinoStoredProcedureFactory(catalogConnectorManager, metalake);
      return new GravitinoSystemConnector(gravitinoStoredProcedureFactory);
    }
  }

  @VisibleForTesting
  Supplier<GravitinoAdminClient> clientProvider() {
    return () -> null;
  }

  private CatalogConnectorFactory createCatalogConnectorFactory(GravitinoConfig config) {
    // Create a CatalogConnectorFactory. If we specify a customized class name for the
    // CatalogConnectorFactory,
    // it creates a user-customized CatalogConnectorFactory; otherwise, it creates a
    // DefaultCatalogConnectorFactory.
    String className = config.getCatalogConnectorFactoryClassName();
    if (StringUtils.isEmpty(className)) {
      return new DefaultCatalogConnectorFactory(config);
    }

    try {
      Class<?> clazz = Class.forName(className);
      Object obj = clazz.getDeclaredConstructor(GravitinoConfig.class).newInstance(config);
      return (CatalogConnectorFactory) obj;
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_RUNTIME_ERROR, "Can not create CatalogConnectorFactory ", e);
    }
  }
}
