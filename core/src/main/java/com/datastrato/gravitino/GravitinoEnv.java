/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.auth.Authenticator;
import com.datastrato.gravitino.auth.AuthenticatorFactory;
import com.datastrato.gravitino.aux.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
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

  private EntitySerDe entitySerDe;

  private CatalogManager catalogManager;

  private CatalogOperationDispatcher catalogOperationDispatcher;

  private MetalakeManager metalakeManager;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;
  private Authenticator authenticator;

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
   * Initialize the Gravitino environment.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initialize(Config config) {
    LOG.info("Initializing Gravitino Environment...");

    this.config = config;

    // Initialize EntitySerDe
    this.entitySerDe = EntitySerDeFactory.createEntitySerDe(config);

    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);
    entityStore.setSerDe(entitySerDe);

    // create and initialize a random id generator
    this.idGenerator = new RandomIdGenerator();

    // Create and initialize metalake related modules
    this.metalakeManager = new MetalakeManager(entityStore, idGenerator);

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator);
    this.catalogOperationDispatcher =
        new CatalogOperationDispatcher(catalogManager, entityStore, idGenerator);

    this.authenticator = AuthenticatorFactory.createAuthenticator(config);
    this.authenticator.initialize(config);

    this.auxServiceManager = new AuxiliaryServiceManager();
    this.auxServiceManager.serviceInit(
        config.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX));

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
   * Get the EntitySerDe associated with the Gravitino environment.
   *
   * @return The EntitySerDe instance.
   */
  public EntitySerDe entitySerDe() {
    return entitySerDe;
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

  public Authenticator authenticator() {
    return authenticator;
  }

  public void start() {
    auxServiceManager.serviceStart();
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

    LOG.info("Gravitino Environment is shut down.");
  }
}
