/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.catalog.CatalogManager;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.meta.MetalakeManager;
import com.datastrato.graviton.storage.InMemoryNameMappingService;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.storage.kv.EntityKeyEncoder;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class manages the Graviton environment.
 */
public class GravitonEnv {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonEnv.class);

  private Config config;

  private EntityStore entityStore;

  private EntitySerDe entitySerDe;

  private CatalogManager catalogManager;

  private CatalogOperationDispatcher catalogOperationDispatcher;

  private MetalakeManager metalakeManager;

  @Getter private NameMappingService nameMappingService = new InMemoryNameMappingService();

  @Getter private EntityKeyEncoder entityKeyEncoder;

  private GravitonEnv() {}

  private static class InstanceHolder {
    private static final GravitonEnv INSTANCE = new GravitonEnv();
  }

  /**
   * Get the singleton instance of the GravitonEnv.
   *
   * @return The singleton instance of the GravitonEnv.
   */
  public static GravitonEnv getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * Initialize the Graviton environment.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initialize(Config config) {
    LOG.info("Initializing Graviton Environment...");

    this.config = config;

    // Initialize EntitySerDe
    this.entitySerDe = EntitySerDeFactory.createEntitySerDe(config);

    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);
    entityStore.setSerDe(entitySerDe);

    // Create and initialize metalake related modules
    this.metalakeManager = new MetalakeManager(entityStore);

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore);
    this.catalogOperationDispatcher = new CatalogOperationDispatcher(catalogManager);

    LOG.info("Graviton Environment is initialized.");
  }

  /**
   * Get the configuration object associated with the Graviton environment.
   *
   * @return The configuration object.
   */
  public Config config() {
    return config;
  }

  /**
   * Get the EntitySerDe associated with the Graviton environment.
   *
   * @return The EntitySerDe instance.
   */
  public EntitySerDe entitySerDe() {
    return entitySerDe;
  }

  /**
   * Get the EntityStore associated with the Graviton environment.
   *
   * @return The EntityStore instance.
   */
  public EntityStore entityStore() {
    Preconditions.checkNotNull(entityStore, "GravitonEnv is not initialized.");
    return entityStore;
  }

  /**
   * Get the CatalogManager associated with the Graviton environment.
   *
   * @return The CatalogManager instance.
   */
  public CatalogManager catalogManager() {
    return catalogManager;
  }

  /**
   * Get the CatalogOperationDispatcher associated with the Graviton environment.
   *
   * @return The CatalogOperationDispatcher instance.
   */
  public CatalogOperationDispatcher catalogOperationDispatcher() {
    return catalogOperationDispatcher;
  }

  /**
   * Get the MetalakeManager associated with the Graviton environment.
   *
   * @return The MetalakeManager instance.
   */
  public MetalakeManager metalakesManager() {
    return metalakeManager;
  }

  /** Shutdown the Graviton environment. */
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
