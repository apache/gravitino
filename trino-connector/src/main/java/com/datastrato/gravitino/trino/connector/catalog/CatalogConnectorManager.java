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
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.GravitinoConfig;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
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

  private GravitinoClient gravitinoClient;
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
  public void setGravitinoClient(GravitinoClient gravitinoClient) {
    this.gravitinoClient = gravitinoClient;
  }

  public void start() {
    if (gravitinoClient == null) {
      gravitinoClient = GravitinoClient.builder(config.getURI()).build();
    }

    // Schedule a task to load catalog from gravitino server.
    executorService.execute(this::loadMetalake);
    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  void loadMetalake() {
    try {
      for (String usedMetalake : usedMetalakes) {
        GravitinoMetaLake metalake;
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
  public void loadCatalogs(GravitinoMetaLake metalake) {
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

    catalogConnectors.entrySet().stream()
        .filter(
            entry ->
                !catalogNameStrings.contains(entry.getKey())
                    &&
                    // Skip the catalog doesn't belong to this metalake.
                    entry.getValue().getMetalake().name().equals(metalake.name()))
        .forEach(
            (entry) -> {
              catalogInjector.removeCatalogConnector(entry.getKey());
              catalogConnectors.remove(entry.getKey());
              LOG.info(
                  "Remove catalog '{}' in metalake {} successfully.",
                  entry.getKey(),
                  metalake.name());
            });

    Arrays.stream(catalogNames)
        .forEach(
            (NameIdentifier nameIdentifier) -> {
              String catalogName = "";
              try {
                catalogName = nameIdentifier.toString();

                // TODO (yuhui) need to handle catalog changed.
                if (!catalogConnectors.containsKey(catalogName)) {
                  Catalog catalog = metalake.loadCatalog(nameIdentifier);

                  GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(catalog);
                  CatalogConnectorContext catalogConnectorContext =
                      catalogConnectorFactory.loadCatalogConnector(
                          nameIdentifier, metalake, gravitinoCatalog);

                  catalogConnectors.put(catalogName, catalogConnectorContext);
                  catalogInjector.injectCatalogConnector(catalogName);
                  LOG.info(
                      "Load catalog {} in metalake {} successfully.", catalogName, metalake.name());
                }
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.", metalake.name(), catalogName, e);
              }
            });
  }

  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public List<String> getCatalogs() {
    return Arrays.asList(catalogConnectors.keySet().toArray(new String[catalogConnectors.size()]));
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
      GravitinoMetaLake metalake =
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
      GravitinoMetaLake metalake =
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

  public void addMetalake(String metalake) {
    usedMetalakes.add(metalake);
  }
}
