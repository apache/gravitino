/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.metrics.MetricsSystem;
import com.datastrato.gravitino.metrics.source.JVMMetricsSource;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class manages the Gravitino environment.
 */
public class GravitinoEnv {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoEnv.class);

  private Config config;

  private EntityStore entityStore;

  private CatalogManager catalogManager;

  private CatalogOperationDispatcher catalogOperationDispatcher;

  private MetalakeManager metalakeManager;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;

  private MetricsSystem metricsSystem;

  private LockManager lockManager;

  private GravitinoEnv() {}

  private static class InstanceHolder {
    private static final GravitinoEnv INSTANCE = new GravitinoEnv();
  }

  /**
   * Get the singleton instance of the GravitinoEnv.
   *
   * @return The singleton instance of the GravitinoEnv.
   */
  public static GravitinoEnv getInstance() {
    return InstanceHolder.INSTANCE;
  }

  /**
   * This method is used for testing purposes only to set the lock manager for test in package
   * `com.datastrato.gravitino.server.web.rest`, as tree lock depends on the lock manager and we did
   * not mock the lock manager in the test, so we need to set the lock manager for test.
   *
   * @param lockManager The lock manager to be set.
   */
  @VisibleForTesting
  public void setLockManager(LockManager lockManager) {
    this.lockManager = lockManager;
  }

  /**
   * Initialize the Gravitino environment.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initialize(Config config) {
    LOG.info("Initializing Gravitino Environment...");

    this.config = config;
    this.metricsSystem = new MetricsSystem();
    metricsSystem.register(new JVMMetricsSource());

    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    // create and initialize a random id generator
    this.idGenerator = new RandomIdGenerator();

    // Create and initialize metalake related modules
    this.metalakeManager = new MetalakeManager(entityStore, idGenerator);

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator);
    this.catalogOperationDispatcher =
        new CatalogOperationDispatcher(catalogManager, entityStore, idGenerator);

    this.auxServiceManager = new AuxiliaryServiceManager();
    this.auxServiceManager.serviceInit(
        config.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX));

    // Tree lock
    this.lockManager = new LockManager(config);
    LOG.info("Gravitino Environment is initialized.");
  }

  /**
   * Get the configuration object associated with the Gravitino environment.
   *
   * @return The configuration object.
   */
  public Config config() {
    return config;
  }

  /**
   * Get the EntityStore associated with the Gravitino environment.
   *
   * @return The EntityStore instance.
   */
  public EntityStore entityStore() {
    Preconditions.checkNotNull(entityStore, "GravitinoEnv is not initialized.");
    return entityStore;
  }

  /**
   * Get the CatalogManager associated with the Gravitino environment.
   *
   * @return The CatalogManager instance.
   */
  public CatalogManager catalogManager() {
    return catalogManager;
  }

  /**
   * Get the CatalogOperationDispatcher associated with the Gravitino environment.
   *
   * @return The CatalogOperationDispatcher instance.
   */
  public CatalogOperationDispatcher catalogOperationDispatcher() {
    return catalogOperationDispatcher;
  }

  /**
   * Get the MetalakeManager associated with the Gravitino environment.
   *
   * @return The MetalakeManager instance.
   */
  public MetalakeManager metalakesManager() {
    return metalakeManager;
  }

  /**
   * Get the IdGenerator associated with the Gravitino environment.
   *
   * @return The IdGenerator instance.
   */
  public IdGenerator idGenerator() {
    return idGenerator;
  }

  /**
   * Get the MetricsSystem associated with the Gravitino environment.
   *
   * @return The MetricsSystem instance.
   */
  public MetricsSystem metricsSystem() {
    return metricsSystem;
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public void start() {
    auxServiceManager.serviceStart();
    metricsSystem.start();
  }

  /** Shutdown the Gravitino environment. */
  public void shutdown() {
    LOG.info("Shutting down Gravitino Environment...");

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

    if (auxServiceManager != null) {
      try {
        auxServiceManager.serviceStop();
      } catch (Exception e) {
        LOG.warn("Failed to stop AuxServiceManager", e);
      }
    }

    if (metricsSystem != null) {
      metricsSystem.close();
    }

    LOG.info("Gravitino Environment is shut down.");
  }
}
