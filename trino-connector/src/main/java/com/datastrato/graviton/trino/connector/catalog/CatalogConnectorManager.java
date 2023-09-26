/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_NO_METALAKE_SELECTED;

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
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;

/**
 * This class has the following main functions: 1. Load catalogs from the Graviton server and create
 * catalog contexts. 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Graviton connectors and inner connectors related to the engine.
 */
public class CatalogConnectorManager {
  private static final Logger LOG = Logger.get(CatalogConnectorManager.class);

  private static final int CATALOG_LOAD_FREQUENCY_SECOND = 30;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private GravitonClient gravitonClient;
  private GravitonConfig config;
  private String usedMetalake;

  public CatalogConnectorManager(
      CatalogInjector catalogInjector, CatalogConnectorFactory catalogFactory) {
    Preconditions.checkArgument(catalogInjector != null, "catalogInjector is not null");
    Preconditions.checkArgument(catalogFactory != null, "catalogFactory is not null");
    this.catalogInjector = catalogInjector;
    this.catalogConnectorFactory = catalogFactory;
  }

  public void config(GravitonConfig config) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
  }

  public void start() {
    gravitonClient = GravitonClient.builder(config.getURI()).build();
    Preconditions.checkNotNull(gravitonClient, "catalogFactory is not null");

    String metalake = config.getMetalake();
    if (Strings.isNullOrEmpty(metalake)) {
      throw new TrinoException(GRAVITON_NO_METALAKE_SELECTED, "No graviton metalake selected");
    }
    this.usedMetalake = metalake;

    // schedule task to load catalog from graviton server.
    executorService.execute(this::loadMetalake);
    LOG.info("Graviton CatalogConnectorManager started.");
  }

  void loadMetalake() {
    try {

      GravitonMetaLake metalake = null;
      try {
        metalake = gravitonClient.loadMetalake(NameIdentifier.ofMetalake(usedMetalake));
      } catch (NoSuchMetalakeException noSuchMetalakeException) {
        LOG.warn("No such Metalake {}", metalake.name());
      }

      LOG.debug("Load metalake: " + metalake.name());
      loadCatalogs(metalake);
      // TODO need to handle metalake dropped.
    } finally {
      // load metalake for handling catalog in the metalake updates.
      executorService.schedule(this::loadMetalake, CATALOG_LOAD_FREQUENCY_SECOND, TimeUnit.SECONDS);
    }
  }

  void loadCatalogs(GravitonMetaLake metalake) {
    try {
      NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));

      LOG.info(
          "Load {} metalake's catalogs. catalogs: {}.",
          metalake.name(),
          Arrays.toString(catalogNames));
      Arrays.stream(catalogNames)
          .forEach(
              (NameIdentifier nameIdentifier) -> {
                try {
                  String catalogName = nameIdentifier.toString();

                  // TODO need to handle catalog changed.
                  if (!catalogConnectors.containsKey(catalogName)) {
                    Catalog catalog = metalake.loadCatalog(nameIdentifier);
                    Preconditions.checkNotNull(catalog, "catalog is not null");

                    GravitonCatalog gravitonCatalog = new GravitonCatalog(catalog);
                    CatalogConnectorContext catalogConnectorContext =
                        catalogConnectorFactory.loadCatalogConnector(
                            nameIdentifier, metalake, gravitonCatalog);

                    catalogConnectors.put(catalogName, catalogConnectorContext);
                    catalogInjector.injectCatalogConnector(catalogName);
                    LOG.info("Load {} metalake's catalog {} successfully.", metalake, catalogName);
                  }
                } catch (Exception e) {
                  LOG.error(
                      "Load {} metalake's catalogs {} failed.",
                      metalake.name(),
                      catalogNames,
                      e.getMessage());
                }
              });
    } catch (Throwable t) {
      LOG.error("Load {} metalake's catalog failed.", metalake.name(), t.getMessage());
    }
  }

  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public void shutdown() {
    LOG.info("Graviton CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }
}
