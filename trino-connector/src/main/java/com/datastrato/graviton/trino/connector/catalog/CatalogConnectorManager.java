/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_METALAKE_NOT_EXISTS;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.trino.connector.GravitonConfig;
import com.datastrato.graviton.trino.connector.metadata.GravitonCatalog;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;

/**
 * This class has the following main functions:
 *
 * <pre>
 * 1. Load catalogs from the Graviton server and create
 * catalog contexts.
 * 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Graviton connectors and inner connectors related to the engine.
 * </pre>
 */
public class CatalogConnectorManager {
  private static final Logger LOG = Logger.get(CatalogConnectorManager.class);

  private static final int CATALOG_LOAD_FREQUENCY_SECOND = 30;
  private static final int NUMBER_EXECUTOR_THREAD = 1;

  private final ScheduledExecutorService executorService;
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private GravitonClient gravitonClient;
  private GravitonConfig config;
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
            .setNameFormat("graviton-connector-schedule-%d")
            .setUncaughtExceptionHandler(
                (thread, throwable) ->
                    LOG.warn("%s uncaught exception:", thread.getName(), throwable))
            .build());
  }

  public void config(GravitonConfig config) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
  }

  public void start() {
    gravitonClient = GravitonClient.builder(config.getURI()).build();
    String metalake = config.getMetalake();
    if (Strings.isNullOrEmpty(metalake)) {
      throw new TrinoException(GRAVITON_METALAKE_NOT_EXISTS, "No graviton metalake selected");
    }
    this.usedMetalake = metalake;

    // Schedule a task to load catalog from graviton server.
    executorService.execute(this::loadMetalake);
    LOG.info("Graviton CatalogConnectorManager started.");
  }

  void loadMetalake() {
    try {
      GravitonMetaLake metalake;
      try {
        metalake = gravitonClient.loadMetalake(NameIdentifier.ofMetalake(usedMetalake));
      } catch (NoSuchMetalakeException noSuchMetalakeException) {
        LOG.warn("Metalake {} does not exist.", usedMetalake);
        return;
      } catch (Exception e) {
        LOG.error("Load Metalake {} failed.", e);
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

  void loadCatalogs(GravitonMetaLake metalake) {
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

                  GravitonCatalog gravitonCatalog = new GravitonCatalog(catalog);
                  CatalogConnectorContext catalogConnectorContext =
                      catalogConnectorFactory.loadCatalogConnector(
                          nameIdentifier, metalake, gravitonCatalog);

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

  public void shutdown() {
    LOG.info("Graviton CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }
}
