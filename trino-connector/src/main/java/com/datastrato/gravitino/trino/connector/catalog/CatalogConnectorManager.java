/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
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

  private static final int CATALOG_LOAD_FREQUENCY_SECOND = 3;
  private static final int NUMBER_EXECUTOR_THREAD = 1;

  private final ScheduledExecutorService executorService;
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private GravitinoAdminClient gravitinoClient;
  private GravitinoConfig config;
  private final Set<String> usedMetalakes = new HashSet<>();

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

  @VisibleForTesting
  public void setGravitinoClient(GravitinoAdminClient gravitinoClient) {
    this.gravitinoClient = gravitinoClient;
  }

  public void start() {
    if (gravitinoClient == null) {
      gravitinoClient = GravitinoAdminClient.builder(config.getURI()).build();
    }

    // Schedule a task to load catalog from gravitino server.
    executorService.execute(this::loadMetalake);
    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  void loadMetalake() {
    try {
      for (String usedMetalake : usedMetalakes) {
        GravitinoMetalake metalake;
        try {
          metalake = gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(usedMetalake));
        } catch (NoSuchMetalakeException noSuchMetalakeException) {
          LOG.warn("Metalake {} does not exist.", usedMetalake);
          continue;
        } catch (Exception e) {
          LOG.error("Load Metalake {} failed.", usedMetalake, e);
          continue;
        }

        LOG.info("Load metalake: {}", usedMetalake);
        loadCatalogs(metalake);
      }
    } finally {
      // Load metalake for handling catalog in the metalake updates.
      executorService.schedule(this::loadMetalake, CATALOG_LOAD_FREQUENCY_SECOND, TimeUnit.SECONDS);
    }
  }

  @VisibleForTesting
  public void loadCatalogs(GravitinoMetalake metalake) {
    NameIdentifier[] catalogNames;
    try {
      catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
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
        Arrays.stream(catalogNames).map(NameIdentifier::toString).collect(Collectors.toSet());

    for (Map.Entry<String, CatalogConnectorContext> entry : catalogConnectors.entrySet()) {
      if (!catalogNameStrings.contains(entry.getKey())
          &&
          // Skip the catalog doesn't belong to this metalake.
          entry.getValue().getMetalake().name().equals(metalake.name())) {
        unloadCatalog(metalake, entry.getKey());
      }
    }

    Arrays.stream(catalogNames)
        .forEach(
            (NameIdentifier nameIdentifier) -> {
              try {
                Catalog catalog = metalake.loadCatalog(nameIdentifier);
                GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(metalake.name(), catalog);
                if (!catalogConnectors.containsKey(gravitinoCatalog.getFullName())) {
                  // Load new catalogs belows to the metalake.
                  if (catalog.type() == Catalog.Type.RELATIONAL) {
                    loadCatalog(metalake, gravitinoCatalog);
                  }
                } else {
                  // Reload catalogs that have been updated in Gravitino server.
                  reloadCatalog(metalake, gravitinoCatalog);
                }
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.",
                    metalake.name(),
                    nameIdentifier.toString(),
                    e);
              }
            });
  }

  private void reloadCatalog(GravitinoMetalake metalake, GravitinoCatalog catalog) {
    String catalogFullName = catalog.getFullName();

    GravitinoCatalog oldCatalog = catalogConnectors.get(catalog.getFullName()).getCatalog();
    if (!catalog.getLastModifiedTime().isAfter(oldCatalog.getLastModifiedTime())) {
      return;
    }

    catalogInjector.removeCatalogConnector(catalog.getFullName());
    catalogConnectors.remove(catalog.getFullName());

    CatalogConnectorContext catalogConnectorContext =
        catalogConnectorFactory.loadCatalogConnector(metalake, catalog);

    catalogConnectors.put(catalogFullName, catalogConnectorContext);
    catalogInjector.injectCatalogConnector(catalogFullName);
    LOG.info("Update catalog '{}' in metalake {} successfully.", catalog, metalake.name());
  }

  private void loadCatalog(GravitinoMetalake metalake, GravitinoCatalog catalog) {
    CatalogConnectorContext catalogConnectorContext =
        catalogConnectorFactory.loadCatalogConnector(metalake, catalog);

    catalogConnectors.put(catalog.getFullName(), catalogConnectorContext);
    catalogInjector.injectCatalogConnector(catalog.getFullName());
    LOG.info(
        "Load catalog {} in metalake {} successfully.", catalog.getFullName(), metalake.name());
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

  public void createCatalog(
      String metalakeName,
      String catalogName,
      String provider,
      Map<String, String> properties,
      boolean ignoreExist) {
    NameIdentifier catalog = NameIdentifier.of(metalakeName, catalogName);
    if (catalogConnectors.containsKey(catalog.toString())) {
      if (!ignoreExist) {
        throw new TrinoException(
            GRAVITINO_CATALOG_ALREADY_EXISTS, String.format("Catalog %s already exists.", catalog));
      }
      return;
    }

    try {
      GravitinoMetalake metalake =
          gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(catalog.namespace().toString()));
      metalake.createCatalog(
          catalog, Catalog.Type.RELATIONAL, provider, "Trino created", properties);

      LOG.info("Create catalog {} in metalake {} successfully.", catalog, metalake);

      Future<?> future = executorService.submit(this::loadMetalake);
      future.get(30, TimeUnit.SECONDS);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    } catch (CatalogAlreadyExistsException e) {
      throw new TrinoException(
          GRAVITINO_CATALOG_ALREADY_EXISTS,
          "Catalog " + catalog + " already exists in the server.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "Create catalog failed. " + e.getMessage(), e);
    }
  }

  public void dropCatalog(String metalakeName, String catalogName, boolean ignoreNotExist) {
    try {
      GravitinoMetalake metalake =
          gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(metalakeName));

      NameIdentifier catalog = NameIdentifier.of(metalakeName, catalogName);
      if (!metalake.catalogExists(catalog)) {
        if (ignoreNotExist) {
          return;
        }

        throw new TrinoException(
            GRAVITINO_CATALOG_NOT_EXISTS, "Catalog " + catalog + " not exists.");
      }
      boolean dropped = metalake.dropCatalog(catalog);
      if (!dropped) {
        throw new TrinoException(
            GRAVITINO_UNSUPPORTED_OPERATION, "Drop catalog " + catalog + " does not support.");
      }
      LOG.info("Drop catalog {} in metalake {} successfully.", catalog, metalake);

      Future<?> future = executorService.submit(this::loadMetalake);
      future.get(30, TimeUnit.SECONDS);

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
    try {
      NameIdentifier catalogNameId = NameIdentifier.of(metalakeName, catalogName);
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectors.get(catalogNameId.toString());
      GravitinoCatalog oldCatalog = catalogConnectorContext.getCatalog();

      List<CatalogChange> changes = new ArrayList<>();
      setProperties.entrySet().stream()
          .forEach(
              e -> {
                if (!oldCatalog.getProperties().containsKey(e.getKey())
                    || !oldCatalog.getProperties().get(e.getKey()).equals(e.getValue())) {
                  changes.add(CatalogChange.setProperty(e.getKey(), e.getValue()));
                }
              });

      removeProperties.stream()
          .forEach(
              key -> {
                if (oldCatalog.getProperties().containsKey(key)) {
                  changes.add(CatalogChange.removeProperty(key));
                }
              });

      if (changes.isEmpty()) {
        return;
      }

      GravitinoMetalake metalake =
          gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(metalakeName));
      metalake.alterCatalog(
          NameIdentifier.of(metalakeName, catalogName),
          changes.toArray(changes.toArray(new CatalogChange[0])));

      Future<?> future = executorService.submit(this::loadMetalake);
      future.get(30, TimeUnit.SECONDS);

    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    } catch (NoSuchCatalogException e) {
      throw new TrinoException(
          GRAVITINO_CATALOG_NOT_EXISTS, "Catalog " + catalogName + " not exists.");
    } catch (Exception e) {
      throw new TrinoException(
          GRAVITINO_UNSUPPORTED_OPERATION, "alter catalog failed. " + e.getMessage(), e);
    }
  }

  public void addMetalake(String metalake) {
    usedMetalakes.add(metalake);
  }
}
