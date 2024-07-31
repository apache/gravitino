/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino;

import com.google.common.base.Preconditions;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FilesetNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetOperationDispatcher;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.catalog.PartitionNormalizeDispatcher;
import org.apache.gravitino.catalog.PartitionOperationDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.SchemaNormalizeDispatcher;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TableNormalizeDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.TopicNormalizeDispatcher;
import org.apache.gravitino.catalog.TopicOperationDispatcher;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.FilesetEventDispatcher;
import org.apache.gravitino.listener.MetalakeEventDispatcher;
import org.apache.gravitino.listener.PartitionEventDispatcher;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.TableEventDispatcher;
import org.apache.gravitino.listener.TopicEventDispatcher;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metalake.MetalakeNormalizeDispatcher;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.JVMMetricsSource;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.tag.TagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class manages the Apache Gravitino environment.
 */
public class GravitinoEnv {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoEnv.class);

  private Config config;

  private EntityStore entityStore;

  private CatalogDispatcher catalogDispatcher;

  private CatalogManager catalogManager;

  private SchemaDispatcher schemaDispatcher;

  private TableDispatcher tableDispatcher;

  private PartitionDispatcher partitionDispatcher;

  private FilesetDispatcher filesetDispatcher;

  private TopicDispatcher topicDispatcher;

  private MetalakeDispatcher metalakeDispatcher;

  private AccessControlManager accessControlManager;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;

  private MetricsSystem metricsSystem;

  private LockManager lockManager;

  private EventListenerManager eventListenerManager;

  private TagManager tagManager;
  private EventBus eventBus;

  protected GravitinoEnv() {}

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
   * @param isGravitinoServer A boolean flag indicating whether the initialization is for the
   *     Gravitino server. If true, server-specific components will be initialized in addition to
   *     the base components.
   */
  public void initialize(Config config, boolean isGravitinoServer) {
    LOG.info("Initializing Gravitino Environment...");
    this.config = config;
    initBaseComponents();
    if (isGravitinoServer) {
      initGravitinoServerComponents();
    }
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
   * Get the PartitionDispatcher associated with the Gravitino environment.
   *
   * @return The PartitionDispatcher instance.
   */
  public PartitionDispatcher partitionDispatcher() {
    return partitionDispatcher;
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
   * Get the CatalogManager associated with the Gravitino environment.
   *
   * @return The CatalogManager instance.
   */
  public CatalogManager catalogManager() {
    Preconditions.checkArgument(catalogManager != null, "GravitinoEnv is not initialized.");
    return catalogManager;
  }

  /**
   * Get the EventBus associated with the Gravitino environment.
   *
   * @return The EventBus instance.
   */
  public EventBus eventBus() {
    Preconditions.checkArgument(eventBus != null, "GravitinoEnv is not initialized.");
    return eventBus;
  }

  /**
   * Get the MetricsSystem associated with the Gravitino environment.
   *
   * @return The MetricsSystem instance.
   */
  public MetricsSystem metricsSystem() {
    return metricsSystem;
  }

  public LockManager lockManager() {
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

  /**
   * Get the TagManager associated with the Gravitino environment.
   *
   * @return The TagManager instance.
   */
  public TagManager tagManager() {
    return tagManager;
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

  private void initBaseComponents() {
    this.metricsSystem = new MetricsSystem();
    metricsSystem.register(new JVMMetricsSource());

    this.eventListenerManager = new EventListenerManager();
    eventListenerManager.init(
        config.getConfigsWithPrefix(EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX));
    this.eventBus = eventListenerManager.createEventBus();
  }

  private void initGravitinoServerComponents() {
    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    // create and initialize a random id generator
    this.idGenerator = new RandomIdGenerator();

    // Create and initialize metalake related modules
    MetalakeManager metalakeManager = new MetalakeManager(entityStore, idGenerator);
    MetalakeNormalizeDispatcher metalakeNormalizeDispatcher =
        new MetalakeNormalizeDispatcher(metalakeManager);
    this.metalakeDispatcher = new MetalakeEventDispatcher(eventBus, metalakeNormalizeDispatcher);

    // Create and initialize Catalog related modules
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator);
    CatalogNormalizeDispatcher catalogNormalizeDispatcher =
        new CatalogNormalizeDispatcher(catalogManager);
    this.catalogDispatcher = new CatalogEventDispatcher(eventBus, catalogNormalizeDispatcher);

    SchemaOperationDispatcher schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    SchemaNormalizeDispatcher schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(schemaOperationDispatcher, catalogManager);
    this.schemaDispatcher = new SchemaEventDispatcher(eventBus, schemaNormalizeDispatcher);

    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
    TableNormalizeDispatcher tableNormalizeDispatcher =
        new TableNormalizeDispatcher(tableOperationDispatcher, catalogManager);
    this.tableDispatcher = new TableEventDispatcher(eventBus, tableNormalizeDispatcher);

    PartitionOperationDispatcher partitionOperationDispatcher =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator);
    PartitionNormalizeDispatcher partitionNormalizeDispatcher =
        new PartitionNormalizeDispatcher(partitionOperationDispatcher, catalogManager);
    this.partitionDispatcher = new PartitionEventDispatcher(eventBus, partitionNormalizeDispatcher);

    FilesetOperationDispatcher filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
    FilesetNormalizeDispatcher filesetNormalizeDispatcher =
        new FilesetNormalizeDispatcher(filesetOperationDispatcher, catalogManager);
    this.filesetDispatcher = new FilesetEventDispatcher(eventBus, filesetNormalizeDispatcher);

    TopicOperationDispatcher topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);
    TopicNormalizeDispatcher topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(topicOperationDispatcher, catalogManager);
    this.topicDispatcher = new TopicEventDispatcher(eventBus, topicNormalizeDispatcher);

    // Create and initialize access control related modules
    boolean enableAuthorization = config.get(Configs.ENABLE_AUTHORIZATION);
    if (enableAuthorization) {
      this.accessControlManager = new AccessControlManager(entityStore, idGenerator, config);
    } else {
      this.accessControlManager = null;
    }

    this.auxServiceManager = new AuxiliaryServiceManager();
    this.auxServiceManager.serviceInit(config);

    // Tree lock
    this.lockManager = new LockManager(config);

    // Tag manager
    this.tagManager = new TagManager(idGenerator, entityStore);
  }
}
