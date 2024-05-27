/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.GravitinoConfig;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the following main functions:
 *
 * <pre>
 * 1. Load catalogs from the Gravitino server and create
 * catalog contexts.
 * 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Gravitino connectors and inner connectors related to the engine.
 * </pre>
 */
public class CatalogConnectorManager {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorManager.class);

  private static final int CATALOG_LOAD_FREQUENCY_SECOND = 10;
  private static final int NUMBER_EXECUTOR_THREAD = 1;
  private static final int LOAD_METALAKE_TIMEOUT = 30;

  private final ScheduledExecutorService executorService;
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private final Set<String> usedMetalakes = new HashSet<>();
  private final Map<String, GravitinoMetalake> metalakes = new ConcurrentHashMap<>();

  private GravitinoAdminClient gravitinoClient;
  private GravitinoConfig config;

  public CatalogConnectorManager(
      CatalogInjector catalogInjector, CatalogConnectorFactory catalogFactory) {
    this.catalogInjector = catalogInjector;
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

  public void config(GravitinoConfig config) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
  }

  public void start(GravitinoAdminClient client) {
    if (client == null) {
      this.gravitinoClient = GravitinoAdminClient.builder(config.getURI()).build();
    } else {
      this.gravitinoClient = client;
    }

    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  public void loadMetalakeSync() {
    try {
      Future<?> future = executorService.submit(this::loadMetalakeImpl);
      future.get();
    } catch (Exception e) {
      LOG.error("Load metalake sync failed.", e);
    } finally {
      // Load metalake for handling catalog in the metalake updates.
      executorService.scheduleWithFixedDelay(
          this::loadMetalakeImpl,
          CATALOG_LOAD_FREQUENCY_SECOND,
          CATALOG_LOAD_FREQUENCY_SECOND,
          TimeUnit.SECONDS);
    }
  }

  private GravitinoMetalake retrieveMetalake(String metalakeName) {
    try {
      return gravitinoClient.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    }
  }

  private void loadMetalakeImpl() {
    for (String usedMetalake : usedMetalakes) {
      try {
        GravitinoMetalake metalake =
            metalakes.computeIfAbsent(usedMetalake, this::retrieveMetalake);
        LOG.info("Load metalake: {}", usedMetalake);
        loadCatalogs(metalake);
      } catch (NoSuchMetalakeException noSuchMetalakeException) {
        LOG.warn("Metalake {} does not exist.", usedMetalake);
      } catch (Exception e) {
        LOG.error("Load Metalake {} failed.", usedMetalake, e);
      }
    }
  }

  @VisibleForTesting
  public void loadCatalogs(GravitinoMetalake metalake) {
    NameIdentifier[] catalogNames;
    try {
      catalogNames = metalake.listCatalogs();
    } catch (Exception e) {
      LOG.error("Failed to list catalogs in metalake {}.", metalake.name(), e);
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Load metalake {}'s catalogs. catalogs: {}.",
          metalake.name(),
          Arrays.toString(catalogNames));
    }

    // Delete those catalogs that have been deleted in Gravitino server
    Set<String> catalogNameStrings =
        Arrays.stream(catalogNames)
            .map(config.simplifyCatalogNames() ? NameIdentifier::name : NameIdentifier::toString)
            .collect(Collectors.toSet());

    for (Map.Entry<String, CatalogConnectorContext> entry : catalogConnectors.entrySet()) {
      if (!catalogNameStrings.contains(entry.getKey())
          &&
          // Skip the catalog doesn't belong to this metalake.
          entry.getValue().getMetalake().name().equals(metalake.name())) {
        try {
          unloadCatalog(metalake, entry.getKey());
        } catch (Exception e) {
          LOG.error("Failed to remove catalog {}.", entry.getKey(), e);
        }
      }
    }

    // Load new catalogs belows to the metalake.
    Arrays.stream(catalogNames)
        .forEach(
            (NameIdentifier nameIdentifier) -> {
              try {
                Catalog catalog = metalake.loadCatalog(nameIdentifier.name());
                GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(metalake.name(), catalog);
                if (catalogConnectors.containsKey(getTrinoCatalogName(gravitinoCatalog))) {
                  // Reload catalogs that have been updated in Gravitino server.
                  reloadCatalog(metalake, gravitinoCatalog);

                } else {
                  if (catalog.type() == Catalog.Type.RELATIONAL) {
                    loadCatalog(metalake, gravitinoCatalog);
                  }
                }
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.", metalake.name(), nameIdentifier, e);
              }
            });
  }

  private void reloadCatalog(GravitinoMetalake metalake, GravitinoCatalog catalog) {
    GravitinoCatalog oldCatalog = catalogConnectors.get(getTrinoCatalogName(catalog)).getCatalog();
    if (catalog.getLastModifiedTime() <= oldCatalog.getLastModifiedTime()) {
      return;
    }

    catalogInjector.removeCatalogConnector((getTrinoCatalogName(catalog)));
    catalogConnectors.remove(getTrinoCatalogName(catalog));

    loadCatalogImpl(metalake, catalog);
    LOG.info("Update catalog '{}' in metalake {} successfully.", catalog, metalake.name());
  }

  private void loadCatalog(GravitinoMetalake metalake, GravitinoCatalog catalog) {
    loadCatalogImpl(metalake, catalog);
    LOG.info("Load catalog {} in metalake {} successfully.", catalog, metalake.name());
  }

  @SuppressWarnings("UnusedVariable")
  private void loadCatalogImpl(GravitinoMetalake metalake, GravitinoCatalog catalog) {
    try {
      throw new NotImplementedException();
    } catch (Exception e) {
      String message =
          String.format("Failed to create internal catalog connector. The catalog is: %s", catalog);
      LOG.error(message, e);
      throw new TrinoException(GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR, message, e);
    }
  }

  private void unloadCatalog(GravitinoMetalake metalake, String catalogFullName) {
    catalogInjector.removeCatalogConnector(catalogFullName);
    catalogConnectors.remove(catalogFullName);
    LOG.info("Remove catalog '{}' in metalake {} successfully.", catalogFullName, metalake.name());
  }

  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public List<GravitinoCatalog> getCatalogs() {
    return catalogConnectors.values().stream().map(CatalogConnectorContext::getCatalog).toList();
  }

  public void shutdown() {
    LOG.info("Gravitino CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }

  public String getTrinoCatalogName(String metalake, String catalog) {
    return config.simplifyCatalogNames() ? catalog : metalake + "." + catalog;
  }

  public String getTrinoCatalogName(GravitinoCatalog catalog) {
    return getTrinoCatalogName(catalog.getMetalake(), catalog.getName());
  }

  public void createCatalog(
      String metalakeName,
      String catalogName,
      String provider,
      Map<String, String> properties,
      boolean ignoreExist) {
    if (catalogConnectors.containsKey(getTrinoCatalogName(metalakeName, catalogName))) {
      if (!ignoreExist) {
        throw new TrinoException(
            GRAVITINO_CATALOG_ALREADY_EXISTS,
            String.format(
                "Catalog %s already exists.", NameIdentifier.of(metalakeName, catalogName)));
      }
      return;
    }

    try {
      GravitinoMetalake metalake = gravitinoClient.loadMetalake(metalakeName);
      metalake.createCatalog(
          catalogName, Catalog.Type.RELATIONAL, provider, "Trino created", properties);

      LOG.info("Create catalog {} in metalake {} successfully.", catalogName, metalake);

      Future<?> future = executorService.submit(this::loadMetalakeImpl);
      future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);

      if (!catalogConnectors.containsKey(getTrinoCatalogName(metalakeName, catalogName))) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Create catalog failed due to the loading process fails");
      }
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    } catch (CatalogAlreadyExistsException e) {
      throw new TrinoException(
          GRAVITINO_CATALOG_ALREADY_EXISTS,
          "Catalog "
              + NameIdentifier.of(metalakeName, catalogName)
              + " already exists in the server.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Create catalog failed. " + e.getMessage(), e);
    }
  }

  public void dropCatalog(String metalakeName, String catalogName, boolean ignoreNotExist) {
    try {
      GravitinoMetalake metalake = gravitinoClient.loadMetalake(metalakeName);

      //      NameIdentifier catalog = NameIdentifier.of(metalakeName, catalogName);
      if (!metalake.catalogExists(catalogName)) {
        if (ignoreNotExist) {
          return;
        }

        throw new TrinoException(
            GRAVITINO_CATALOG_NOT_EXISTS,
            "Catalog " + NameIdentifier.of(metalakeName, catalogName) + " not exists.");
      }
      boolean dropped = metalake.dropCatalog(catalogName);
      if (!dropped) {
        throw new TrinoException(
            GRAVITINO_UNSUPPORTED_OPERATION, "Drop catalog " + catalogName + " does not support.");
      }
      LOG.info("Drop catalog {} in metalake {} successfully.", catalogName, metalake);

      Future<?> future = executorService.submit(this::loadMetalakeImpl);
      future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);

      if (catalogConnectors.containsKey(getTrinoCatalogName(metalakeName, catalogName))) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Drop catalog failed due to the reloading process fails");
      }
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Drop catalog failed. " + e.getMessage(), e);
    }
  }

  public void alterCatalog(
      String metalakeName,
      String catalogName,
      Map<String, String> setProperties,
      List<String> removeProperties) {
    NameIdentifier catalog = NameIdentifier.of(metalakeName, catalogName);
    try {
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectors.get(getTrinoCatalogName(metalakeName, catalogName));
      GravitinoCatalog oldCatalog = catalogConnectorContext.getCatalog();

      List<CatalogChange> changes = new ArrayList<>();
      setProperties
          .entrySet()
          .forEach(
              e -> {
                // Skip the no changed attributes
                boolean matched =
                    oldCatalog.getProperties().entrySet().stream()
                        .anyMatch(
                            oe ->
                                oe.getKey().equals(e.getKey())
                                    && oe.getValue().equals(e.getValue()));
                if (!matched) {
                  changes.add(CatalogChange.setProperty(e.getKey(), e.getValue()));
                }
              });

      removeProperties.forEach(
          key -> {
            if (oldCatalog.getProperties().containsKey(key)) {
              changes.add(CatalogChange.removeProperty(key));
            }
          });

      if (changes.isEmpty()) {
        return;
      }

      GravitinoMetalake metalake = gravitinoClient.loadMetalake(metalakeName);
      metalake.alterCatalog(catalogName, changes.toArray(changes.toArray(new CatalogChange[0])));

      Future<?> future = executorService.submit(this::loadMetalakeImpl);
      future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);

      catalogConnectorContext =
          catalogConnectors.get(getTrinoCatalogName(metalakeName, catalogName));
      if (catalogConnectorContext == null
          || catalogConnectorContext.getCatalog().getLastModifiedTime()
              == oldCatalog.getLastModifiedTime()) {
        throw new TrinoException(
            GRAVITINO_OPERATION_FAILED, "Update catalog failed due to the reloading process fails");
      }

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(GRAVITINO_CATALOG_NOT_EXISTS, "Catalog " + catalog + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "alter catalog failed. " + e.getMessage(), e);
    }
  }

  public void addMetalake(String metalake) {
    if (config.simplifyCatalogNames() && usedMetalakes.size() > 1)
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG,
          "Multiple metalakes are not supported when setting gravitino.simplify-catalog-names = true");
    usedMetalakes.add(metalake);
  }

  public Set<String> getUsedMetalakes() {
    return usedMetalakes;
  }

  public Connector createConnector(
      String connectorName, GravitinoConfig config, ConnectorContext context) {
    try {
      String catalogConfig = config.getCatalogConfig();

      GravitinoCatalog catalog = GravitinoCatalog.fromJson(catalogConfig);
      CatalogConnectorContext.Builder builder =
          catalogConnectorFactory.createCatalogConnectorContextBuilder(catalog);
      builder.withMetalake(
          metalakes.computeIfAbsent(catalog.getMetalake(), this::retrieveMetalake));

      CatalogConnectorContext connectorContext = builder.build();
      catalogConnectors.put(connectorName, connectorContext);
      LOG.info("Create connector success");
      return connectorContext.getConnector();
    } catch (Exception e) {
      LOG.error("Failed to create connector: {}", connectorName, e);
      throw new TrinoException(
          GRAVITINO_OPERATION_FAILED, "Failed to create connector: " + connectorName, e);
    }
  }
}
