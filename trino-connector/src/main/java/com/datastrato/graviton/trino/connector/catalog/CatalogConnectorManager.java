/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_NO_METALAKE_SELECTED;
import static java.util.Objects.requireNonNull;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.GravitonConfig;
import com.datastrato.graviton.trino.connector.metadata.GravitonCatalog;
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;

/**
 * * This class has the following main functions: 1. Load catalogs from the Graviton server and
 * create catalog contexts. 2. Manage all catalog context instances, which primarily handle
 * communication with Trino through Graviton connectors and inner connectors related to the engine.
 */
public class CatalogConnectorManager {
  private static final Logger log = Logger.get(CatalogConnectorManager.class);

  private static int CatalogLoadFrequnceSecond = 30;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private GravitonClient gravitonClient;
  private GravitonConfig config;

  public CatalogConnectorManager(
      CatalogInjector catalogInjector, CatalogConnectorFactory catalogFactory) {
    this.catalogInjector =
        Preconditions.checkNotNull(catalogInjector, "catalogInjector is not null");
    this.catalogConnectorFactory =
        Preconditions.checkNotNull(catalogFactory, "catalogFactory is not null");
  }

  public void config(GravitonConfig config) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
  }

  public void start() {
    gravitonClient = GravitonClient.builder(config.getURI()).build();
    Preconditions.checkArgument(gravitonClient != null, "catalogFactory is not null");

    // schedule task to load catalog from graviton server.
    executorService.execute(this::loadMetalakes);
    log.info("Graviton CatalogConnectorManager started.");
  }

  void loadMetalakes() {
    try {
      GravitonMetaLake[] gravitonMetaLakes = gravitonClient.listMetalakes();

      String usedMetalake = config.getMetalake();
      if ("".equals(usedMetalake)) {
        throw new TrinoException(GRAVITON_NO_METALAKE_SELECTED, "No graviton metalake selected");
      }

      for (GravitonMetaLake metaLake : gravitonMetaLakes) {
        if (metaLake.name().equals(usedMetalake)) {
          log.debug("Load metalake: " + metaLake.name());
          loadCatalogs(metaLake);
          break;
        }
      }
      // TODO need to handle metalake dropped.
    } finally {
      executorService.schedule(this::loadMetalakes, CatalogLoadFrequnceSecond, TimeUnit.SECONDS);
    }
  }

  void loadCatalogs(GravitonMetaLake metalake) {
    try {
      NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));

      log.info(
          "Load {} metalake's catelog. catalogs: {}.",
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
                    requireNonNull(catalogInjector, "catalogInjector is null");

                    GravitonCatalog gravitonCatalog = new GravitonCatalog(catalog);
                    CatalogConnectorContext catalogConnectorContext =
                        catalogConnectorFactory.loadCatalogConnector(
                            nameIdentifier, metalake, gravitonCatalog);

                    catalogConnectors.put(catalogName, catalogConnectorContext);
                    catalogInjector.injectCatalogConnector(catalogName);
                    log.info("Load {} metalake's catalog {} successfully.", metalake, catalogName);
                  }
                } catch (Exception e) {
                  log.error(
                      "Load {} metalake's catelog {} failed.",
                      metalake.name(),
                      catalogNames,
                      e.getMessage());
                }
              });
    } catch (Throwable t) {
      log.error("Load {} metalake's catelog failed.", metalake.name(), t.getMessage());
    }
  }

  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public void shutdown(String catalogName) {
    log.info("Graviton CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }
}
