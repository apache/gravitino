/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.catalog;

import static java.util.Objects.requireNonNull;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonClient;
import com.datastrato.graviton.client.GravitonMetaLake;
import io.airlift.log.Logger;
import io.trino.plugin.graviton.GravitonConfig;
import io.trino.plugin.graviton.metadata.GravitonCatalog;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;

public class CatalogConnectorManager {
  private static final Logger log = Logger.get(CatalogConnectorManager.class);

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private final CatalogInjector catalogInjector;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnector> catalogConnectors =
      new ConcurrentHashMap<>();

  private GravitonClient gravitonClient;
  private GravitonConfig config;

  public CatalogConnectorManager(
      CatalogInjector catalogInjector, CatalogConnectorFactory catalogFactory) {
    requireNonNull(catalogInjector, "catalogInjector is null");
    requireNonNull(catalogFactory, "catalogFactory is null");
    this.catalogInjector = catalogInjector;
    this.catalogConnectorFactory = catalogFactory;
  }

  public void config(GravitonConfig config) {
    this.config = config;
  }

  public void start() {
    gravitonClient = GravitonClient.builder(config.getURI()).build();

    // schedule task to log catalog from graviton server.
    executorService.schedule(this::loadMetalakes, 3, TimeUnit.SECONDS);
    log.info("Graviton CatalogConnectorManager started.");
  }

  void loadMetalakes() {
    GravitonMetaLake[] gravitonMetaLakes = gravitonClient.listMetalakes();
    for (GravitonMetaLake metaLake : gravitonMetaLakes) {
      log.debug("Load metalake: " + metaLake.name());
      loadCatalogs(metaLake);
    }
    executorService.schedule(this::loadMetalakes, 30, TimeUnit.SECONDS);
  }

  void loadCatalogs(GravitonMetaLake metalake) {
    try {
      NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
      Arrays.stream(catalogNames)
          .forEach(
              (NameIdentifier nameIdentifier) -> {
                String catalogName = nameIdentifier.toString();

                if (!catalogConnectors.containsKey(catalogName)) {
                  Catalog catalog = metalake.loadCatalog(nameIdentifier);
                  requireNonNull(catalogInjector, "catalogInjector is null");

                  GravitonCatalog gravitonCatalog = new GravitonCatalog(catalog);
                  CatalogConnector catalogConnector =
                      catalogConnectorFactory.loadCatalogConnector(
                          nameIdentifier, metalake, gravitonCatalog);

                  catalogConnectors.put(catalogName, catalogConnector);
                  catalogInjector.injectCatalogConnector(catalogName);
                  log.info(
                      "load {} metalake's catalog successfully. catalogs: {}",
                      Arrays.toString(catalogNames));
                }
              });
    } catch (Throwable t) {
      log.error("Load {} metalake's catelog failed. ", metalake.name(), t.getMessage());
    }
  }

  public CatalogConnector getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public void shutdown(String catalogName) {
    log.info("Graviton CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }
}
