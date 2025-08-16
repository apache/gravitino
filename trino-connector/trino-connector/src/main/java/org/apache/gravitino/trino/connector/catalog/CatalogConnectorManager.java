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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the following main functions:
 *
 * <pre>
 * 1. Load catalogs from the Apache Gravitino server and create
 * catalog contexts.
 * 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Gravitino connectors and inner connectors related to the engine.
 * </pre>
 */
public class CatalogConnectorManager {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorManager.class);

  private static final int NUMBER_EXECUTOR_THREAD = 1;
  private static final int LOAD_METALAKE_TIMEOUT = 60;

  private int metadataUpdateIntervalSecond = 10;

  private final ScheduledExecutorService executorService;
  private final CatalogRegister catalogRegister;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private String targetMetalake;
  private final Map<String, GravitinoMetalake> metalakes = new ConcurrentHashMap<>();

  private GravitinoAdminClient gravitinoClient;
  private GravitinoConfig config;

  /**
   * Constructs a new CatalogConnectorManager with the specified catalog register and catalog
   * connector factory.
   *
   * @param catalogRegister the catalog register
   * @param catalogFactory the catalog connector factory
   */
  public CatalogConnectorManager(
      CatalogRegister catalogRegister, CatalogConnectorFactory catalogFactory) {
    this.catalogRegister = catalogRegister;
    this.catalogConnectorFactory = catalogFactory;
    this.executorService = createScheduledThreadPoolExecutor();
  }

  private static ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor() {
    return new ScheduledThreadPoolExecutor(
        NUMBER_EXECUTOR_THREAD,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("gravitino-connector-schedule-%d")
            .setUncaughtExceptionHandler(
                (thread, throwable) ->
                    LOG.warn("{} uncaught exception:", thread.getName(), throwable))
            .build());
  }

  /**
   * Configures the catalog connector manager with the specified Gravitino configuration and client.
   *
   * @param config the Gravitino configuration
   * @param client the Gravitino admin client
   */
  public void config(GravitinoConfig config, GravitinoAdminClient client) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
    if (client == null) {
      this.gravitinoClient =
          GravitinoAdminClient.builder(config.getURI())
              .withClientConfig(config.getClientConfig())
              .build();
    } else {
      this.gravitinoClient = client;
    }
    this.metadataUpdateIntervalSecond = Integer.parseInt(config.getMetadataRefreshIntervalSecond());
    this.targetMetalake = config.getMetalake();
  }

  /**
   * Starts the catalog connector manager with the specified Trino connector context.
   *
   * @param context the Trino connector context
   * @throws Exception if the catalog connector manager fails to start
   */
  public void start(ConnectorContext context) throws Exception {
    catalogRegister.init(context, config);
    if (catalogRegister.isCoordinator()) {
      executorService.scheduleWithFixedDelay(
          this::loadMetalake,
          metadataUpdateIntervalSecond,
          metadataUpdateIntervalSecond,
          TimeUnit.SECONDS);
    }

    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  private void loadMetalake() {
    try {
      if (!catalogRegister.isTrinoStarted()) {
        LOG.info("Waiting for the Trino started.");
        return;
      }

      Set<String> usedMetalakes = new HashSet<>();
      if (config.singleMetalakeMode()) {
        usedMetalakes.add(targetMetalake);
        metalakes.computeIfAbsent(targetMetalake, this::retrieveMetalake);
      } else {
        GravitinoMetalake[] allMetalakes = gravitinoClient.listMetalakes();
        for (GravitinoMetalake metalake : allMetalakes) {
          usedMetalakes.add(metalake.name());
          metalakes.put(metalake.name(), metalake);
        }
      }

      for (String usedMetalake : usedMetalakes) {
        try {
          GravitinoMetalake metalake = metalakes.get(usedMetalake);
          LOG.debug("Load metalake: {}", usedMetalake);
          loadCatalogs(metalake);
        } catch (Exception e) {
          LOG.error("Load Metalake {} failed.", usedMetalake, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Error when loading metalake", e);
      System.exit(-1);
    }
  }

  /**
   * Retrieves a metalake by its name.
   *
   * @param metalakeName the name of the metalake
   * @return the metalake
   * @throws TrinoException if the metalake does not exist
   */
  public GravitinoMetalake retrieveMetalake(String metalakeName) {
    try {
      return gravitinoClient.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS,
          "Metalake " + metalakeName + " not exists.");
    }
  }

  private void loadCatalogs(GravitinoMetalake metalake) {
    String[] catalogNames;
    try {
      catalogNames = metalake.listCatalogs();
    } catch (Exception e) {
      LOG.error("Failed to list catalogs in metalake {}.", metalake.name(), e);
      return;
    }

    LOG.debug(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));

    // Delete those catalogs that have been deleted in Gravitino server
    Set<String> catalogNameStrings =
        Arrays.stream(catalogNames)
            .map(id -> config.singleMetalakeMode() ? id : getTrinoCatalogName(metalake.name(), id))
            .collect(Collectors.toSet());

    for (Map.Entry<String, CatalogConnectorContext> entry : catalogConnectors.entrySet()) {
      if (!catalogNameStrings.contains(entry.getKey())
          &&
          // Skip the catalog doesn't belong to this metalake.
          entry.getValue().getMetalake().name().equals(metalake.name())) {
        try {
          unloadCatalog(entry.getValue().getCatalog());
        } catch (Exception e) {
          LOG.error("Failed to remove catalog {}.", entry.getKey(), e);
        }
      }
    }

    // Load new catalogs belows to the metalake.
    Arrays.stream(catalogNames)
        .forEach(
            (String catalogName) -> {
              try {
                Catalog catalog = metalake.loadCatalog(catalogName);
                GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(metalake.name(), catalog);
                if (catalogConnectors.containsKey(getTrinoCatalogName(gravitinoCatalog))) {
                  // Reload catalogs that have been updated in Gravitino server.
                  reloadCatalog(gravitinoCatalog);
                } else {
                  if (catalog.type() == Catalog.Type.RELATIONAL
                      && catalogConnectorFactory
                          .getSupportedCatalogProviders()
                          .contains(gravitinoCatalog.getProvider())) {
                    loadCatalog(gravitinoCatalog);
                  }
                }
              } catch (UnsupportedOperationException e) {
                LOG.warn(
                    "Unsupported catalog type for catalog {} in metalake {}: {}",
                    catalogName,
                    metalake.name(),
                    e.getMessage());
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.", metalake.name(), catalogName, e);
              }
            });
  }

  private void reloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    GravitinoCatalog oldCatalog = catalogConnectors.get(catalogFullName).getCatalog();
    if (catalog.getLastModifiedTime() <= oldCatalog.getLastModifiedTime()) {
      return;
    }

    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);

    loadCatalogImpl(catalog);
    LOG.info("Update catalog '{}' in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalog(GravitinoCatalog catalog) {
    loadCatalogImpl(catalog);
    LOG.info("Load catalog {} in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalogImpl(GravitinoCatalog catalog) {
    try {
      catalogRegister.registerCatalog(getTrinoCatalogName(catalog), catalog);
    } catch (Exception e) {
      String message =
          String.format("Failed to create internal catalog connector. The catalog is: %s", catalog);
      LOG.error(message, e);
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR, message, e);
    }
  }

  private void unloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);
    LOG.info(
        "Remove catalog '{}' in metalake {} successfully.",
        catalog.getName(),
        catalog.getMetalake());
  }

  /**
   * Retrieves the catalog connector context for the specified catalog name.
   *
   * @param catalogName the name of the catalog
   * @return the catalog connector context
   */
  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  /**
   * Checks if a catalog connector exists for the specified catalog name.
   *
   * @param catalogName the name of the catalog
   * @return true if the catalog connector exists, false otherwise
   */
  public boolean catalogConnectorExist(String catalogName) {
    return catalogConnectors.containsKey(catalogName);
  }

  /**
   * Retrieves all catalogs managed by this connector manager.
   *
   * @return a list of Gravitino catalogs
   */
  public List<GravitinoCatalog> getCatalogs() {
    return catalogConnectors.values().stream().map(CatalogConnectorContext::getCatalog).toList();
  }

  /** Shuts down the catalog connector manager. */
  public void shutdown() {
    LOG.info("Gravitino CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }

  /**
   * Retrieves the Trino catalog name for the specified metalake and catalog.
   *
   * @param metalake the name of the metalake
   * @param catalog the name of the catalog
   * @return the Trino catalog name
   */
  public String getTrinoCatalogName(String metalake, String catalog) {
    return config.singleMetalakeMode() ? catalog : String.format("\"%s.%s\"", metalake, catalog);
  }

  /**
   * Retrieves the Trino catalog name for the specified catalog.
   *
   * @param catalog the catalog
   * @return the Trino catalog name
   */
  public String getTrinoCatalogName(GravitinoCatalog catalog) {
    return getTrinoCatalogName(catalog.getMetalake(), catalog.getName());
  }

  /**
   * Retrieves the set of metalakes that have been used.
   *
   * @return the set of metalakes
   */
  public Set<String> getUsedMetalakes() {
    return metalakes.keySet();
  }

  /**
   * Creates a new connector for the specified catalog name.
   *
   * @param connectorName the name of the connector
   * @param config the Gravitino configuration
   * @param context the Trino connector context
   * @return the created connector
   */
  public Connector createConnector(
      String connectorName, GravitinoConfig config, ConnectorContext context) {
    try {
      String catalogConfig = config.getCatalogConfig();

      GravitinoCatalog catalog = GravitinoCatalog.fromJson(catalogConfig);
      CatalogConnectorContext.Builder builder =
          catalogConnectorFactory.createCatalogConnectorContextBuilder(catalog);
      builder
          .withMetalake(metalakes.computeIfAbsent(catalog.getMetalake(), this::retrieveMetalake))
          .withContext(context);

      CatalogConnectorContext connectorContext = builder.build();
      catalogConnectors.put(connectorName, connectorContext);
      LOG.info("Create connector {} successful", connectorName);
      return connectorContext.getConnector();
    } catch (Exception e) {
      LOG.error("Failed to create connector: {}", connectorName, e);
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_OPERATION_FAILED,
          "Failed to create connector: " + connectorName,
          e);
    }
  }

  /**
   * Loads the metalake synchronously.
   *
   * @throws Exception if the metalake fails to load
   */
  public void loadMetalakeSync() throws Exception {
    Future<?> future = executorService.submit(this::loadMetalake);
    future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Retrieves the Gravitino metalake for the specified name.
   *
   * @param metalake the name of the metalake
   * @return the Gravitino metalake
   * @throws TrinoException if the metalake is not found
   */
  public GravitinoMetalake getMetalake(String metalake) {
    return metalakes.computeIfAbsent(metalake, this::retrieveMetalake);
  }
}
