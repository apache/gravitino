/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.GravitinoConfig;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  private String usedMetalake;

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
    String metalake = config.getMetalake();
    if (Strings.isNullOrEmpty(metalake)) {
      throw new TrinoException(GRAVITINO_METALAKE_NOT_EXISTS, "No gravitino metalake selected");
    }
    this.usedMetalake = metalake;

    // Schedule a task to load catalog from gravitino server.
    executorService.execute(this::loadMetalake);
    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  void loadMetalake() {
    try {
      GravitinoMetaLake metalake;
      try {
        metalake = gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(usedMetalake));
      } catch (NoSuchMetalakeException noSuchMetalakeException) {
        LOG.warn("Metalake {} does not exist.", usedMetalake);
        return;
      } catch (Exception e) {
        LOG.error("Load Metalake {} failed.", usedMetalake, e);
        return;
      }

      LOG.debug("Load metalake: " + usedMetalake);
      loadCatalogs(metalake);
      // TODO (yuhui) need to handle metalake dropped.
    } finally {
      // Load metalake for handling catalog in the metalake updates.
      executorService.schedule(this::loadMetalake, CATALOG_LOAD_FREQUENCY_SECOND, TimeUnit.SECONDS);
    }
  }

  void loadCatalogs(GravitinoMetaLake metalake) {
    NameIdentifier[] catalogNames;
    try {
      catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
    } catch (Exception e) {
      LOG.error("Failed to list catalogs in metalake {}.", metalake.name(), e);
      return;
    }

    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));

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
                  LOG.info("Load catalog {} in metalake {} successfully.", catalogName, metalake);
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
}
