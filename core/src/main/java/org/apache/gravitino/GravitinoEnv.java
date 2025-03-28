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
import org.apache.gravitino.audit.AuditLogManager;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.FutureGrantManager;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FilesetNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetOperationDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.ModelNormalizeDispatcher;
import org.apache.gravitino.catalog.ModelOperationDispatcher;
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
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.hook.AccessControlHookDispatcher;
import org.apache.gravitino.hook.CatalogHookDispatcher;
import org.apache.gravitino.hook.FilesetHookDispatcher;
import org.apache.gravitino.hook.MetalakeHookDispatcher;
import org.apache.gravitino.hook.SchemaHookDispatcher;
import org.apache.gravitino.hook.TableHookDispatcher;
import org.apache.gravitino.hook.TopicHookDispatcher;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.FilesetEventDispatcher;
import org.apache.gravitino.listener.MetalakeEventDispatcher;
import org.apache.gravitino.listener.ModelEventDispatcher;
import org.apache.gravitino.listener.PartitionEventDispatcher;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.TableEventDispatcher;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.listener.TopicEventDispatcher;
import org.apache.gravitino.listener.api.event.AccessControlEventDispatcher;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metalake.MetalakeNormalizeDispatcher;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.JVMMetricsSource;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class manages the Apache Gravitino environment.
 */
public class GravitinoEnv {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoEnv.class);

  private Config config;
  // Iceberg REST server use base components while Gravitino Server use full components.
  private boolean manageFullComponents = true;

  private EntityStore entityStore;

  private CatalogDispatcher catalogDispatcher;

  private CatalogManager catalogManager;

  private MetalakeManager metalakeManager;

  private SchemaDispatcher schemaDispatcher;

  private TableDispatcher tableDispatcher;

  private PartitionDispatcher partitionDispatcher;

  private FilesetDispatcher filesetDispatcher;

  private TopicDispatcher topicDispatcher;

  private ModelDispatcher modelDispatcher;

  private MetalakeDispatcher metalakeDispatcher;

  private CredentialOperationDispatcher credentialOperationDispatcher;

  private TagDispatcher tagDispatcher;

  private AccessControlDispatcher accessControlDispatcher;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;

  private MetricsSystem metricsSystem;

  private LockManager lockManager;

  private EventListenerManager eventListenerManager;

  private AuditLogManager auditLogManager;

  private EventBus eventBus;
  private OwnerManager ownerManager;
  private FutureGrantManager futureGrantManager;

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
   * Initialize base components, used for Iceberg REST server.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initializeBaseComponents(Config config) {
    LOG.info("Initializing Gravitino base environment...");
    this.config = config;
    this.manageFullComponents = false;
    initBaseComponents();
    LOG.info("Gravitino base environment is initialized.");
  }

  /**
   * Initialize all components, used for Gravitino server.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initializeFullComponents(Config config) {
    LOG.info("Initializing Gravitino full environment...");
    this.config = config;
    this.manageFullComponents = true;
    initBaseComponents();
    initGravitinoServerComponents();
    LOG.info("Gravitino full environment is initialized.");
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
   * Get the ModelDispatcher associated with the Gravitino environment.
   *
   * @return The ModelDispatcher instance.
   */
  public ModelDispatcher modelDispatcher() {
    return modelDispatcher;
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
   * Get the {@link CredentialOperationDispatcher} associated with the Gravitino environment.
   *
   * @return The {@link CredentialOperationDispatcher} instance.
   */
  public CredentialOperationDispatcher credentialOperationDispatcher() {
    return credentialOperationDispatcher;
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
   * Get the AccessControlDispatcher associated with the Gravitino environment.
   *
   * @return The AccessControlDispatcher instance.
   */
  public AccessControlDispatcher accessControlDispatcher() {
    return accessControlDispatcher;
  }

  /**
   * Get the tagDispatcher associated with the Gravitino environment.
   *
   * @return The tagDispatcher instance.
   */
  public TagDispatcher tagDispatcher() {
    return tagDispatcher;
  }

  /**
   * Get the OwnerManager associated with the Gravitino environment.
   *
   * @return The OwnerManager instance.
   */
  public OwnerManager ownerManager() {
    return ownerManager;
  }

  /**
   * Get the FutureGrantManager associated with the Gravitino environment.
   *
   * @return The FutureGrantManager instance.
   */
  public FutureGrantManager futureGrantManager() {
    return futureGrantManager;
  }

  public void start() {
    metricsSystem.start();
    eventListenerManager.start();
    if (manageFullComponents) {
      auxServiceManager.serviceStart();
    }
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

    if (metalakeManager != null) {
      metalakeManager.close();
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

    this.auditLogManager = new AuditLogManager();
    auditLogManager.init(config, eventListenerManager);
  }

  private void initGravitinoServerComponents() {
    // Initialize EntityStore
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    // create and initialize a random id generator
    this.idGenerator = new RandomIdGenerator();

    // Tree lock
    this.lockManager = new LockManager(config);

    // Create and initialize metalake related modules, the operation chain is:
    // MetalakeEventDispatcher -> MetalakeNormalizeDispatcher -> MetalakeHookDispatcher ->
    // MetalakeManager
    this.metalakeManager = new MetalakeManager(entityStore, idGenerator);
    MetalakeHookDispatcher metalakeHookDispatcher = new MetalakeHookDispatcher(metalakeManager);
    MetalakeNormalizeDispatcher metalakeNormalizeDispatcher =
        new MetalakeNormalizeDispatcher(metalakeHookDispatcher);
    this.metalakeDispatcher = new MetalakeEventDispatcher(eventBus, metalakeNormalizeDispatcher);

    // Create and initialize Catalog related modules, the operation chain is:
    // CatalogEventDispatcher -> CatalogNormalizeDispatcher -> CatalogHookDispatcher ->
    // CatalogManager
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator);
    CatalogHookDispatcher catalogHookDispatcher = new CatalogHookDispatcher(catalogManager);
    CatalogNormalizeDispatcher catalogNormalizeDispatcher =
        new CatalogNormalizeDispatcher(catalogHookDispatcher);
    this.catalogDispatcher = new CatalogEventDispatcher(eventBus, catalogNormalizeDispatcher);

    this.credentialOperationDispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator);

    SchemaOperationDispatcher schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    SchemaHookDispatcher schemaHookDispatcher = new SchemaHookDispatcher(schemaOperationDispatcher);
    SchemaNormalizeDispatcher schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(schemaHookDispatcher, catalogManager);
    this.schemaDispatcher = new SchemaEventDispatcher(eventBus, schemaNormalizeDispatcher);

    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
    TableHookDispatcher tableHookDispatcher = new TableHookDispatcher(tableOperationDispatcher);
    TableNormalizeDispatcher tableNormalizeDispatcher =
        new TableNormalizeDispatcher(tableHookDispatcher, catalogManager);
    this.tableDispatcher = new TableEventDispatcher(eventBus, tableNormalizeDispatcher);

    // TODO: We can install hooks when we need, we only supports ownership post hook,
    //  partition doesn't have ownership, so we don't need it now.
    PartitionOperationDispatcher partitionOperationDispatcher =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator);
    PartitionNormalizeDispatcher partitionNormalizeDispatcher =
        new PartitionNormalizeDispatcher(partitionOperationDispatcher, catalogManager);
    this.partitionDispatcher = new PartitionEventDispatcher(eventBus, partitionNormalizeDispatcher);

    FilesetOperationDispatcher filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator);
    FilesetHookDispatcher filesetHookDispatcher =
        new FilesetHookDispatcher(filesetOperationDispatcher);
    FilesetNormalizeDispatcher filesetNormalizeDispatcher =
        new FilesetNormalizeDispatcher(filesetHookDispatcher, catalogManager);
    this.filesetDispatcher = new FilesetEventDispatcher(eventBus, filesetNormalizeDispatcher);

    TopicOperationDispatcher topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);
    TopicHookDispatcher topicHookDispatcher = new TopicHookDispatcher(topicOperationDispatcher);
    TopicNormalizeDispatcher topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(topicHookDispatcher, catalogManager);
    this.topicDispatcher = new TopicEventDispatcher(eventBus, topicNormalizeDispatcher);

    // TODO(jerryshao). Add Hook support for Model.
    ModelOperationDispatcher modelOperationDispatcher =
        new ModelOperationDispatcher(catalogManager, entityStore, idGenerator);
    ModelNormalizeDispatcher modelNormalizeDispatcher =
        new ModelNormalizeDispatcher(modelOperationDispatcher, catalogManager);
    this.modelDispatcher = new ModelEventDispatcher(eventBus, modelNormalizeDispatcher);

    // Create and initialize access control related modules
    boolean enableAuthorization = config.get(Configs.ENABLE_AUTHORIZATION);
    if (enableAuthorization) {
      AccessControlManager accessControlManager =
          new AccessControlManager(entityStore, idGenerator, config);
      AccessControlHookDispatcher accessControlHookDispatcher =
          new AccessControlHookDispatcher(accessControlManager);
      this.accessControlDispatcher =
          new AccessControlEventDispatcher(eventBus, accessControlHookDispatcher);
      this.ownerManager = new OwnerManager(entityStore);
      this.futureGrantManager = new FutureGrantManager(entityStore, ownerManager);
    } else {
      this.accessControlDispatcher = null;
      this.ownerManager = null;
      this.futureGrantManager = null;
    }

    this.auxServiceManager = new AuxiliaryServiceManager();
    this.auxServiceManager.serviceInit(config);

    // Create and initialize Tag related modules
    this.tagDispatcher = new TagEventDispatcher(eventBus, new TagManager(idGenerator, entityStore));
  }
}
