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
package com.datastrato.gravitino;

import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.CatalogDispatcher;
import com.datastrato.gravitino.catalog.CatalogManager;
import com.datastrato.gravitino.catalog.CatalogNormalizeDispatcher;
import com.datastrato.gravitino.catalog.FilesetDispatcher;
import com.datastrato.gravitino.catalog.FilesetNormalizeDispatcher;
import com.datastrato.gravitino.catalog.FilesetOperationDispatcher;
import com.datastrato.gravitino.catalog.PartitionDispatcher;
import com.datastrato.gravitino.catalog.PartitionNormalizeDispatcher;
import com.datastrato.gravitino.catalog.PartitionOperationDispatcher;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.catalog.SchemaNormalizeDispatcher;
import com.datastrato.gravitino.catalog.SchemaOperationDispatcher;
import com.datastrato.gravitino.catalog.TableDispatcher;
import com.datastrato.gravitino.catalog.TableNormalizeDispatcher;
import com.datastrato.gravitino.catalog.TableOperationDispatcher;
import com.datastrato.gravitino.catalog.TopicDispatcher;
import com.datastrato.gravitino.catalog.TopicNormalizeDispatcher;
import com.datastrato.gravitino.catalog.TopicOperationDispatcher;
import com.datastrato.gravitino.listener.CatalogEventDispatcher;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.EventListenerManager;
import com.datastrato.gravitino.listener.FilesetEventDispatcher;
import com.datastrato.gravitino.listener.MetalakeEventDispatcher;
import com.datastrato.gravitino.listener.SchemaEventDispatcher;
import com.datastrato.gravitino.listener.TableEventDispatcher;
import com.datastrato.gravitino.listener.TopicEventDispatcher;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.metalake.MetalakeDispatcher;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.metalake.MetalakeNormalizeDispatcher;
import com.datastrato.gravitino.metrics.MetricsSystem;
import com.datastrato.gravitino.metrics.source.JVMMetricsSource;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.tag.TagManager;
import com.google.common.base.Preconditions;
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
        new SchemaNormalizeDispatcher(schemaOperationDispatcher);
    this.schemaDispatcher = new SchemaEventDispatcher(eventBus, schemaNormalizeDispatcher);

    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
    TableNormalizeDispatcher tableNormalizeDispatcher =
        new TableNormalizeDispatcher(tableOperationDispatcher);
    this.tableDispatcher = new TableEventDispatcher(eventBus, tableNormalizeDispatcher);

    PartitionOperationDispatcher partitionOperationDispatcher =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator);
    // todo: support PartitionEventDispatcher
    this.partitionDispatcher = new PartitionNormalizeDispatcher(partitionOperationDispatcher);

    FilesetOperationDispatcher filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
    FilesetNormalizeDispatcher filesetNormalizeDispatcher =
        new FilesetNormalizeDispatcher(filesetOperationDispatcher);
    this.filesetDispatcher = new FilesetEventDispatcher(eventBus, filesetNormalizeDispatcher);

    TopicOperationDispatcher topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);
    TopicNormalizeDispatcher topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(topicOperationDispatcher);
    this.topicDispatcher = new TopicEventDispatcher(eventBus, topicNormalizeDispatcher);

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

    // Tag manager
    this.tagManager = new TagManager(idGenerator, entityStore);

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
}
