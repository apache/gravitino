/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.catalog.CatalogManager;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.meta.BaseMetalakesOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonEnv {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonEnv.class);

  private Config config;

  private EntityStore entityStore;

  private EntitySerDe entitySerDe;

  private CatalogManager catalogManager;

  private CatalogOperationDispatcher catalogOperationDispatcher;

  // TODO. We should refactor the BaseMetalakesOperations to align with other class names. @jerry
  private BaseMetalakesOperations metalakesOperations;

  private GravitonEnv() {}

  private static class InstanceHolder {
    private static final GravitonEnv INSTANCE = new GravitonEnv();
  }

  public static GravitonEnv getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public void initialize(Config config) {
    LOG.info("Initializing Graviton Environment...");

    this.config = config;

    // Initialize EntitySerDe
    this.entitySerDe = EntitySerDeFactory.createEntitySerDe(config);

    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    // initialize the entitySerDe facade
    EntitySerDeFacadeImpl entitySeDeFacade = new EntitySerDeFacadeImpl();
    entitySeDeFacade.setEntitySeDe(entitySerDe);
    entityStore.setSerDeFacade(entitySeDeFacade);

    // Create and initialize metalake related modules
    this.metalakesOperations = new BaseMetalakesOperations();

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore);
    this.catalogOperationDispatcher = new CatalogOperationDispatcher(catalogManager);

    LOG.info("Graviton Environment is initialized.");
  }

  public Config config() {
    return config;
  }

  public EntitySerDe entitySerDe() {
    return entitySerDe;
  }

  public EntityStore entityStore() {
    return entityStore;
  }

  public CatalogManager catalogManager() {
    return catalogManager;
  }

  public CatalogOperationDispatcher catalogOperationDispatcher() {
    return catalogOperationDispatcher;
  }

  public BaseMetalakesOperations metalakesOperations() {
    return metalakesOperations;
  }

  public void shutdown() {
    LOG.info("Shutting down Graviton Environment...");

    if (entityStore != null) {
      try {
        entityStore.close();
      } catch (Exception e) {
        LOG.warn("Failed to close EntityStore.", e);
      }
    }

    if (catalogManager != null) {
      catalogManager.close();
    }

    LOG.info("Graviton Environment is shut down.");
  }
}
