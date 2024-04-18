/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.CatalogDispatcher;
import com.datastrato.gravitino.catalog.CatalogEventDispatcher;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.catalog.FilesetDispatcher;
import com.datastrato.gravitino.catalog.FilesetEventDispatcher;
import com.datastrato.gravitino.catalog.FilesetOperationDispatcher;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.catalog.SchemaEventDispatcher;
import com.datastrato.gravitino.catalog.SchemaOperationDispatcher;
import com.datastrato.gravitino.catalog.TableDispatcher;
import com.datastrato.gravitino.catalog.TableEventDispatcher;
import com.datastrato.gravitino.catalog.TableOperationDispatcher;
import com.datastrato.gravitino.catalog.TopicDispatcher;
import com.datastrato.gravitino.catalog.TopicEventDispatcher;
import com.datastrato.gravitino.catalog.TopicOperationDispatcher;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.EventListenerManager;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.metalake.MetalakeDispatcher;
import com.datastrato.gravitino.metalake.MetalakeEventDispatcher;
import com.datastrato.gravitino.metalake.MetalakeManager;
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

  private CatalogDispatcher catalogDispatcher;

  private CatalogManager catalogManager;

  private SchemaDispatcher schemaDispatcher;

  private TableDispatcher tableDispatcher;

  private FilesetDispatcher filesetDispatcher;

  private TopicDispatcher topicDispatcher;

  private MetalakeDispatcher metalakeDispatcher;

  private AccessControlManager accessControlManager;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;

  private MetricsSystem metricsSystem;

  private LockManager lockManager;
  private EventListenerManager eventListenerManager;

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
   * This method is used for testing purposes only to set the access manager for test in package
   * `com.datastrato.gravitino.server.web.rest` and `com.datastrato.gravitino.authorization`.
   *
   * @param accessControlManager The access control manager to be set.
   */
  @VisibleForTesting
  public void setAccessControlManager(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  /**
   * This method is used for testing purposes only to set the entity store for test in package
   * `com.datastrato.gravitino.authorization`.
   *
   * @param entityStore The entity store to be set.
   */
  @VisibleForTesting
  public void setEntityStore(EntityStore entityStore) {
    this.entityStore = entityStore;
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

    this.eventListenerManager = new EventListenerManager();
    eventListenerManager.init(
        config.getConfigsWithPrefix(EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX));
    EventBus eventBus = eventListenerManager.createEventBus();

    // Create and initialize metalake related modules
    MetalakeManager metalakeManager = new MetalakeManager(entityStore, idGenerator);
    this.metalakeDispatcher = new MetalakeEventDispatcher(eventBus, metalakeManager);

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator);
    this.catalogDispatcher = new CatalogEventDispatcher(eventBus, catalogManager);

    SchemaOperationDispatcher schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    this.schemaDispatcher = new SchemaEventDispatcher(eventBus, schemaOperationDispatcher);
    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
    this.tableDispatcher = new TableEventDispatcher(eventBus, tableOperationDispatcher);
    FilesetOperationDispatcher filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
    this.filesetDispatcher = new FilesetEventDispatcher(eventBus, filesetOperationDispatcher);
    TopicOperationDispatcher topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);
    this.topicDispatcher = new TopicEventDispatcher(eventBus, topicOperationDispatcher);

    // Create and initialize access control related modules
    boolean enableAuthorization = config.get(Configs.ENABLE_AUTHORIZATION);
    if (enableAuthorization) {
      this.accessControlManager = new AccessControlManager(entityStore, idGenerator, config);
    } else {
      this.accessControlManager = null;
    }

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
   * Get the CatalogDispatcher associated with the Gravitino environment.
   *
   * @return The CatalogDispatcher instance.
   */
  public CatalogDispatcher catalogDispatcher() {
    return catalogDispatcher;
  }

  /**
   * Get the SchemaDispatcher associated with the Gravitino environment.
   *
   * @return The SchemaDispatcher instance.
   */
  public SchemaDispatcher schemaDispatcher() {
    return schemaDispatcher;
  }

  /**
   * Get the TableDispatcher associated with the Gravitino environment.
   *
   * @return The TableDispatcher instance.
   */
  public TableDispatcher tableDispatcher() {
    return tableDispatcher;
  }

  /**
   * Get the FilesetDispatcher associated with the Gravitino environment.
   *
   * @return The FilesetDispatcher instance.
   */
  public FilesetDispatcher filesetDispatcher() {
    return filesetDispatcher;
  }

  /**
   * Get the TopicDispatcher associated with the Gravitino environment.
   *
   * @return The TopicDispatcher instance.
   */
  public TopicDispatcher topicDispatcher() {
    return topicDispatcher;
  }

  /**
   * Get the MetalakeDispatcher associated with the Gravitino environment.
   *
   * @return The MetalakeDispatcher instance.
   */
  public MetalakeDispatcher metalakeDispatcher() {
    return metalakeDispatcher;
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

  /**
   * Get the AccessControlManager associated with the Gravitino environment.
   *
   * @return The AccessControlManager instance.
   */
  public AccessControlManager accessControlManager() {
    return accessControlManager;
  }

  public void start() {
    auxServiceManager.serviceStart();
    metricsSystem.start();
    eventListenerManager.start();
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

    if (eventListenerManager != null) {
      eventListenerManager.stop();
    }

    LOG.info("Gravitino Environment is shut down.");
  }
}
