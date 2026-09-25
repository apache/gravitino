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
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.OwnerEventManager;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FilesetNormalizeDispatcher;
import org.apache.gravitino.catalog.FilesetOperationDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.catalog.FunctionNormalizeDispatcher;
import org.apache.gravitino.catalog.FunctionOperationDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.ModelNormalizeDispatcher;
import org.apache.gravitino.catalog.ModelOperationDispatcher;
import org.apache.gravitino.catalog.PartitionDispatcher;
import org.apache.gravitino.catalog.PartitionNormalizeDispatcher;
import org.apache.gravitino.catalog.PartitionOperationDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.SchemaNormalizeDispatcher;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.catalog.SemanticModelNormalizeDispatcher;
import org.apache.gravitino.catalog.SemanticModelOperationDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TableNormalizeDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.TopicNormalizeDispatcher;
import org.apache.gravitino.catalog.TopicOperationDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.catalog.ViewNormalizeDispatcher;
import org.apache.gravitino.catalog.ViewOperationDispatcher;
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.encryption.kms.KmsClientRegistry;
import org.apache.gravitino.hook.AccessControlHookDispatcher;
import org.apache.gravitino.hook.CatalogHookDispatcher;
import org.apache.gravitino.hook.FilesetHookDispatcher;
import org.apache.gravitino.hook.FunctionHookDispatcher;
import org.apache.gravitino.hook.JobHookDispatcher;
import org.apache.gravitino.hook.MetalakeHookDispatcher;
import org.apache.gravitino.hook.ModelHookDispatcher;
import org.apache.gravitino.hook.PolicyHookDispatcher;
import org.apache.gravitino.hook.SchemaHookDispatcher;
import org.apache.gravitino.hook.TableHookDispatcher;
import org.apache.gravitino.hook.TagHookDispatcher;
import org.apache.gravitino.hook.TopicHookDispatcher;
import org.apache.gravitino.hook.ViewHookDispatcher;
import org.apache.gravitino.job.BuiltInJobTemplateEventListener;
import org.apache.gravitino.job.JobManager;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.job.JobTemplateValidationDispatcher;
import org.apache.gravitino.listener.AccessControlEventDispatcher;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.FilesetEventDispatcher;
import org.apache.gravitino.listener.FunctionEventDispatcher;
import org.apache.gravitino.listener.JobEventDispatcher;
import org.apache.gravitino.listener.MetalakeEventDispatcher;
import org.apache.gravitino.listener.ModelEventDispatcher;
import org.apache.gravitino.listener.PartitionEventDispatcher;
import org.apache.gravitino.listener.PolicyEventDispatcher;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.listener.StatisticEventDispatcher;
import org.apache.gravitino.listener.TableEventDispatcher;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.listener.TopicEventDispatcher;
import org.apache.gravitino.listener.ViewEventDispatcher;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metalake.MetalakeNormalizeDispatcher;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.JVMMetricsSource;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.stats.StatisticDispatcher;
import org.apache.gravitino.stats.StatisticManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.utils.FileFetcher;
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
  private CatalogDispatcher internalCatalogDispatcher;

  private CatalogManager catalogManager;

  private MetalakeManager metalakeManager;

  private SchemaDispatcher schemaDispatcher;
  private SchemaDispatcher internalSchemaDispatcher;

  private TableDispatcher tableDispatcher;
  private TableDispatcher internalTableDispatcher;

  private PartitionDispatcher partitionDispatcher;
  private PartitionDispatcher internalPartitionDispatcher;

  private FilesetDispatcher filesetDispatcher;

  private FilesetDispatcher internalFilesetDispatcher;

  private TopicDispatcher topicDispatcher;

  private TopicDispatcher internalTopicDispatcher;

  private ModelDispatcher modelDispatcher;
  private ModelDispatcher internalModelDispatcher;

  private FunctionDispatcher functionDispatcher;
  private FunctionDispatcher internalFunctionDispatcher;

  private SemanticModelDispatcher semanticModelDispatcher;

  private ViewDispatcher viewDispatcher;
  private ViewDispatcher internalViewDispatcher;

  private MetalakeDispatcher metalakeDispatcher;
  private MetalakeDispatcher internalMetalakeDispatcher;

  private CredentialOperationDispatcher credentialOperationDispatcher;

  private SecretPropertyOperationDispatcher secretPropertyOperationDispatcher;

  private KmsClientRegistry kmsClientRegistry;

  private SecretManager secretManager;

  private TagDispatcher tagDispatcher;
  private TagDispatcher internalTagDispatcher;

  private PolicyDispatcher policyDispatcher;
  private PolicyDispatcher internalPolicyDispatcher;

  private AccessControlDispatcher accessControlDispatcher;
  private AccessControlDispatcher internalAccessControlDispatcher;

  private IdGenerator idGenerator;

  private AuxiliaryServiceManager auxServiceManager;

  private MetricsSystem metricsSystem;

  private LockManager lockManager;

  private EventListenerManager eventListenerManager;

  private AuditLogManager auditLogManager;

  private JobOperationDispatcher jobOperationDispatcher;
  private JobOperationDispatcher internalJobOperationDispatcher;

  private EventBus eventBus;
  private OwnerDispatcher ownerDispatcher;
  private OwnerDispatcher internalOwnerDispatcher;
  private BulkManager bulkManager;
  private FutureGrantManager futureGrantManager;
  private GravitinoAuthorizer gravitinoAuthorizer;
  private StatisticDispatcher statisticDispatcher;
  private StatisticDispatcher internalStatisticDispatcher;

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
    initializeConfig(config);
    this.manageFullComponents = false;
    initBaseComponents();
    LOG.info("Gravitino base environment is initialized.");
  }

  /**
   * Initializes components required for normalized metadata operations.
   *
   * <p>This initialization profile does not initialize event listeners, audit logging, metadata
   * hooks, auxiliary services, or job management.
   *
   * <p>This method must be called on {@link #getInstance()}. Some metadata components read their
   * dependencies directly from that singleton instead of from the object being initialized.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initializeMetadataComponents(Config config) {
    Preconditions.checkState(
        this == getInstance(),
        "Metadata components must be initialized on GravitinoEnv.getInstance().");
    LOG.info("Initializing Gravitino metadata environment...");
    initializeConfig(config);
    this.manageFullComponents = false;
    initCommonComponents();
    initMetadataComponents();
    LOG.info("Gravitino metadata environment is initialized.");
  }

  /**
   * Initialize all components, used for Gravitino server.
   *
   * @param config The configuration object to initialize the environment.
   */
  public void initializeFullComponents(Config config) {
    LOG.info("Initializing Gravitino full environment...");
    initializeConfig(config);
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
   * Get the auxiliary service manager associated with the Gravitino environment.
   *
   * @return The auxiliary service manager instance.
   */
  public AuxiliaryServiceManager auxServiceManager() {
    return auxServiceManager;
  }

  /**
   * Get the EntityStore associated with the Gravitino environment.
   *
   * @return The EntityStore instance.
   */
  public EntityStore entityStore() {
    Preconditions.checkArgument(entityStore != null, "GravitinoEnv is not initialized.");
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
   * Get the internal CatalogDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission. It is
   * intended for infrastructure catalog lookups that should not be recorded as user API audit
   * events.
   *
   * @return The internal CatalogDispatcher instance.
   */
  public CatalogDispatcher internalCatalogDispatcher() {
    return internalCatalogDispatcher;
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
   * Get the internal SchemaDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission. It is
   * intended for infrastructure code that synchronizes metadata as part of another user-visible
   * operation.
   *
   * @return The internal SchemaDispatcher instance.
   */
  public SchemaDispatcher internalSchemaDispatcher() {
    return internalSchemaDispatcher;
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
   * Get the internal TableDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission. It is
   * intended for infrastructure code that synchronizes metadata as part of another user-visible
   * operation.
   *
   * @return The internal TableDispatcher instance.
   */
  public TableDispatcher internalTableDispatcher() {
    return internalTableDispatcher;
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
   * Get the internal ModelDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission.
   *
   * @return The internal ModelDispatcher instance.
   */
  public ModelDispatcher internalModelDispatcher() {
    return internalModelDispatcher;
  }

  /**
   * Get the FunctionDispatcher associated with the Gravitino environment.
   *
   * @return The FunctionDispatcher instance.
   */
  public FunctionDispatcher functionDispatcher() {
    return functionDispatcher;
  }

  /**
   * Get the internal FunctionDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission.
   *
   * @return The internal FunctionDispatcher instance.
   */
  public FunctionDispatcher internalFunctionDispatcher() {
    return internalFunctionDispatcher;
  }

  /**
   * Get the Semantic Model dispatcher associated with the Gravitino environment.
   *
   * @return The Semantic Model dispatcher.
   */
  public SemanticModelDispatcher semanticModelDispatcher() {
    return semanticModelDispatcher;
  }

  /**
   * Get the ViewDispatcher associated with the Gravitino environment.
   *
   * @return The ViewDispatcher instance.
   */
  public ViewDispatcher viewDispatcher() {
    return viewDispatcher;
  }

  /**
   * Get the internal ViewDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hook/event side effects from
   * dependent schema lookups. It is intended for infrastructure code that synchronizes metadata as
   * part of another user-visible operation.
   *
   * @return The internal ViewDispatcher instance.
   */
  public ViewDispatcher internalViewDispatcher() {
    return internalViewDispatcher;
  }

  /**
   * * Get the PartitionDispatcher associated with the Gravitino environment.
   *
   * @return The PartitionDispatcher instance.
   */
  public PartitionDispatcher partitionDispatcher() {
    return partitionDispatcher;
  }

  /**
   * Get the internal PartitionDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips event emission.
   *
   * @return The internal PartitionDispatcher instance.
   */
  public PartitionDispatcher internalPartitionDispatcher() {
    Preconditions.checkArgument(
        internalPartitionDispatcher != null, "GravitinoEnv is not initialized.");
    return internalPartitionDispatcher;
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
   * Get the internal FilesetDispatcher associated with the Gravitino environment.
   *
   * @return The internal FilesetDispatcher instance.
   */
  public FilesetDispatcher internalFilesetDispatcher() {
    Preconditions.checkArgument(
        internalFilesetDispatcher != null, "GravitinoEnv is not initialized.");
    return internalFilesetDispatcher;
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
   * Get the internal TopicDispatcher associated with the Gravitino environment.
   *
   * @return The internal TopicDispatcher instance.
   */
  public TopicDispatcher internalTopicDispatcher() {
    Preconditions.checkArgument(
        internalTopicDispatcher != null, "GravitinoEnv is not initialized.");
    return internalTopicDispatcher;
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
   * Get the internal MetalakeDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves normalization but skips hooks and event emission.
   *
   * @return The internal MetalakeDispatcher instance.
   */
  public MetalakeDispatcher internalMetalakeDispatcher() {
    return internalMetalakeDispatcher;
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
   * Get the {@link SecretPropertyOperationDispatcher} associated with the Gravitino environment.
   *
   * @return The {@link SecretPropertyOperationDispatcher} instance.
   */
  public SecretPropertyOperationDispatcher secretPropertyOperationDispatcher() {
    return secretPropertyOperationDispatcher;
  }

  /**
   * Get the metadata-only KMS client registry associated with the Gravitino environment.
   *
   * <p>The environment owns this registry. Callers may inject it into dependent components but must
   * not close it.
   *
   * @return The KMS client registry instance.
   * @throws IllegalStateException if the environment has not been initialized
   */
  public KmsClientRegistry kmsClientRegistry() {
    Preconditions.checkState(
        kmsClientRegistry != null, "GravitinoEnv components are not initialized.");
    return kmsClientRegistry;
  }

  /**
   * Get the {@link SecretManager} associated with the Gravitino environment.
   *
   * @return The SecretManager instance.
   * @throws IllegalStateException if the environment has not been initialized
   */
  public SecretManager secretManager() {
    Preconditions.checkState(secretManager != null, "GravitinoEnv components are not initialized.");
    return secretManager;
  }

  /**
   * Get the secrets-provider registry associated with the Gravitino environment.
   *
   * <p>Owned by {@link #secretManager()}. Callers may use it for discovery but must not close it.
   *
   * @return The secrets-provider registry instance.
   * @throws IllegalStateException if the environment has not been initialized
   */
  public SecretProviderRegistry secretProviderRegistry() {
    return secretManager().getRegistry();
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
   * Get the internal AccessControlDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher skips hooks and event emission. It is intended for authorization
   * infrastructure lookups that should not be recorded as user API audit events.
   *
   * @return The internal AccessControlDispatcher instance.
   */
  public AccessControlDispatcher internalAccessControlDispatcher() {
    return internalAccessControlDispatcher;
  }

  /**
   * Get the BulkManager associated with the Gravitino environment.
   *
   * @return The BulkManager instance.
   */
  public BulkManager bulkManager() {
    return bulkManager;
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
   * Get the internal TagDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher skips hooks and event emission.
   *
   * @return The internal TagDispatcher instance.
   */
  public TagDispatcher internalTagDispatcher() {
    return internalTagDispatcher;
  }

  /**
   * Get the PolicyDispatcher associated with the Gravitino environment.
   *
   * @return The PolicyDispatcher instance.
   */
  public PolicyDispatcher policyDispatcher() {
    return policyDispatcher;
  }

  /**
   * Get the internal PolicyDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher skips hooks and event emission.
   *
   * @return The internal PolicyDispatcher instance.
   */
  public PolicyDispatcher internalPolicyDispatcher() {
    return internalPolicyDispatcher;
  }

  /**
   * Get the Owner dispatcher associated with the Gravitino environment.
   *
   * @return The OwnerManager instance.
   */
  public OwnerDispatcher ownerDispatcher() {
    return ownerDispatcher;
  }

  /**
   * Get the internal OwnerDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher skips event emission. It is intended for infrastructure ownership
   * synchronization that happens as part of another user-visible operation.
   *
   * @return The internal OwnerDispatcher instance.
   */
  public OwnerDispatcher internalOwnerDispatcher() {
    return internalOwnerDispatcher;
  }

  /**
   * Get the FutureGrantManager associated with the Gravitino environment.
   *
   * @return The FutureGrantManager instance.
   */
  public FutureGrantManager futureGrantManager() {
    return futureGrantManager;
  }

  /**
   * Get the EventListenerManager associated with the Gravitino environment.
   *
   * @return The EventListenerManager instance.
   */
  public EventListenerManager eventListenerManager() {
    return eventListenerManager;
  }

  /**
   * Set GravitinoAuthorizer to GravitinoEnv
   *
   * @param gravitinoAuthorizer the GravitinoAuthorizer instance
   */
  public void setGravitinoAuthorizer(GravitinoAuthorizer gravitinoAuthorizer) {
    this.gravitinoAuthorizer = gravitinoAuthorizer;
  }

  /**
   * Get The GravitinoAuthorizer
   *
   * @return the GravitinoAuthorizer instance
   */
  public GravitinoAuthorizer gravitinoAuthorizer() {
    return gravitinoAuthorizer;
  }

  /**
   * Get the JobOperationDispatcher associated with the Gravitino environment.
   *
   * @return The JobOperationDispatcher instance.
   */
  public JobOperationDispatcher jobOperationDispatcher() {
    Preconditions.checkArgument(jobOperationDispatcher != null, "GravitinoEnv is not initialized.");
    return jobOperationDispatcher;
  }

  /**
   * Get the internal JobOperationDispatcher associated with the Gravitino environment.
   *
   * <p>The internal dispatcher preserves validation but skips hooks and event emission.
   *
   * @return The internal JobOperationDispatcher instance.
   */
  public JobOperationDispatcher internalJobOperationDispatcher() {
    Preconditions.checkArgument(
        internalJobOperationDispatcher != null, "GravitinoEnv is not initialized.");
    return internalJobOperationDispatcher;
  }

  public StatisticDispatcher statisticDispatcher() {
    return statisticDispatcher;
  }

  /**
   * Get the internal StatisticDispatcher associated with the Gravitino environment.
   *
   * @return The internal StatisticDispatcher instance.
   */
  public StatisticDispatcher internalStatisticDispatcher() {
    Preconditions.checkArgument(
        internalStatisticDispatcher != null, "GravitinoEnv is not initialized.");
    return internalStatisticDispatcher;
  }

  public boolean cacheEnabled() {
    return config == null || config.get(Configs.CACHE_ENABLED);
  }

  public void start() {
    metricsSystem.start();
    if (eventListenerManager != null) {
      eventListenerManager.start();
    }
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

    if (jobOperationDispatcher != null) {
      try {
        jobOperationDispatcher.close();
        jobOperationDispatcher = null;
      } catch (Exception e) {
        LOG.warn("Failed to close JobOperationDispatcher", e);
      }
    }

    StatisticDispatcher statisticDispatcherToClose =
        statisticDispatcher != null ? statisticDispatcher : internalStatisticDispatcher;
    if (statisticDispatcherToClose != null) {
      try {
        statisticDispatcherToClose.close();
      } catch (Exception e) {
        LOG.warn("Failed to close StatisticDispatcher", e);
      }
    }

    if (kmsClientRegistry != null) {
      kmsClientRegistry.close();
    }

    if (secretManager != null) {
      secretManager.close();
    }

    LOG.info("Gravitino Environment is shut down.");
  }

  private void initBaseComponents() {
    initCommonComponents();

    this.eventListenerManager = new EventListenerManager();
    eventListenerManager.init(
        config.getConfigsWithPrefix(EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX));
    this.eventBus = eventListenerManager.createEventBus();

    this.auditLogManager = new AuditLogManager();
    auditLogManager.init(config, eventListenerManager);
  }

  private void initializeConfig(Config config) {
    this.config = config;
    FileFetcher.get().initialize(config.get(Configs.BLOCK_UNSAFE_REMOTE_URI));
    SecretPropertyUtils.configureSensitiveKeyKeywords(config);
  }

  private void initCommonComponents() {
    this.kmsClientRegistry = new KmsClientRegistry(config);
    this.secretManager = new SecretManager(config);

    this.metricsSystem = new MetricsSystem();
    metricsSystem.register(new JVMMetricsSource());
  }

  private MetadataOperations initMetadataComponents() {
    initEntityStoreAndCatalogManager();

    this.metalakeManager = new MetalakeManager(entityStore, idGenerator, catalogManager);
    this.internalMetalakeDispatcher = new MetalakeNormalizeDispatcher(metalakeManager);
    this.internalCatalogDispatcher = new CatalogNormalizeDispatcher(catalogManager);

    this.credentialOperationDispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    this.secretPropertyOperationDispatcher =
        new SecretPropertyOperationDispatcher(
            catalogManager, entityStore, idGenerator, secretManager);

    // Fileset dispatcher is created before schema dispatcher so schema can take it directly.
    FilesetOperationDispatcher filesetOperationDispatcher = initInternalFilesetDispatcher();
    SchemaOperationDispatcher schemaOperationDispatcher = initInternalSchemaDispatcher();
    initInternalTableDispatcher();
    initInternalPartitionDispatcher();
    TopicOperationDispatcher topicOperationDispatcher = initInternalTopicDispatcher();
    ModelOperationDispatcher modelOperationDispatcher = initInternalModelDispatcher();
    FunctionOperationDispatcher functionOperationDispatcher =
        initInternalFunctionDispatcher(schemaOperationDispatcher);
    initInternalViewDispatcher();
    initSemanticModelDispatcher(schemaOperationDispatcher);

    this.internalStatisticDispatcher = new StatisticManager(entityStore, idGenerator, config);
    initInternalAuthorizationComponents();

    this.internalTagDispatcher = new TagManager(idGenerator, entityStore);
    this.internalPolicyDispatcher = new PolicyManager(idGenerator, entityStore);

    return new MetadataOperations(
        filesetOperationDispatcher,
        schemaOperationDispatcher,
        topicOperationDispatcher,
        modelOperationDispatcher,
        functionOperationDispatcher);
  }

  private FilesetOperationDispatcher initInternalFilesetDispatcher() {
    FilesetOperationDispatcher filesetOperationDispatcher =
        new FilesetOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    this.internalFilesetDispatcher =
        new FilesetNormalizeDispatcher(filesetOperationDispatcher, catalogManager);
    return filesetOperationDispatcher;
  }

  private SchemaOperationDispatcher initInternalSchemaDispatcher() {
    SchemaOperationDispatcher schemaOperationDispatcher =
        new SchemaOperationDispatcher(
            catalogManager, entityStore, idGenerator, secretManager, internalFilesetDispatcher);
    this.internalSchemaDispatcher =
        new SchemaNormalizeDispatcher(schemaOperationDispatcher, catalogManager);
    return schemaOperationDispatcher;
  }

  private void initInternalTableDispatcher() {
    TableOperationDispatcher internalTableOperationDispatcher =
        new TableOperationDispatcher(
            catalogManager,
            entityStore,
            idGenerator,
            () -> internalSchemaDispatcher,
            secretManager);
    this.internalTableDispatcher =
        new TableNormalizeDispatcher(internalTableOperationDispatcher, catalogManager);
  }

  private void initInternalPartitionDispatcher() {
    PartitionOperationDispatcher partitionOperationDispatcher =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    this.internalPartitionDispatcher =
        new PartitionNormalizeDispatcher(partitionOperationDispatcher, catalogManager);
  }

  private TopicOperationDispatcher initInternalTopicDispatcher() {
    TopicOperationDispatcher topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    this.internalTopicDispatcher =
        new TopicNormalizeDispatcher(topicOperationDispatcher, catalogManager);
    return topicOperationDispatcher;
  }

  private ModelOperationDispatcher initInternalModelDispatcher() {
    ModelOperationDispatcher modelOperationDispatcher =
        new ModelOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    this.internalModelDispatcher =
        new ModelNormalizeDispatcher(modelOperationDispatcher, catalogManager);
    return modelOperationDispatcher;
  }

  private FunctionOperationDispatcher initInternalFunctionDispatcher(
      SchemaOperationDispatcher schemaOperationDispatcher) {
    FunctionOperationDispatcher functionOperationDispatcher =
        new FunctionOperationDispatcher(
            catalogManager, schemaOperationDispatcher, entityStore, idGenerator, secretManager);
    this.internalFunctionDispatcher =
        new FunctionNormalizeDispatcher(functionOperationDispatcher, catalogManager);
    return functionOperationDispatcher;
  }

  private void initInternalViewDispatcher() {
    ViewOperationDispatcher internalViewOperationDispatcher =
        new ViewOperationDispatcher(
            catalogManager,
            entityStore,
            idGenerator,
            () -> internalSchemaDispatcher,
            secretManager);
    this.internalViewDispatcher =
        new ViewNormalizeDispatcher(internalViewOperationDispatcher, catalogManager);
  }

  private void initSemanticModelDispatcher(SchemaOperationDispatcher schemaOperationDispatcher) {
    // Semantic Model operation chain: SemanticModelNormalizeDispatcher ->
    // SemanticModelOperationDispatcher -> ManagedSemanticModelOperations.
    // TODO(#12594): Add Semantic Model ownership and privilege hooks.
    SemanticModelOperationDispatcher semanticModelOperationDispatcher =
        new SemanticModelOperationDispatcher(
            catalogManager, schemaOperationDispatcher, entityStore, idGenerator, secretManager);
    this.semanticModelDispatcher =
        new SemanticModelNormalizeDispatcher(semanticModelOperationDispatcher, catalogManager);
  }

  private void initInternalAuthorizationComponents() {
    if (config.get(Configs.ENABLE_AUTHORIZATION)) {
      this.internalAccessControlDispatcher =
          new AccessControlManager(entityStore, idGenerator, config);
      this.internalOwnerDispatcher = new OwnerManager(entityStore);
      this.bulkManager = new BulkManager(config);
      this.futureGrantManager = new FutureGrantManager(entityStore, internalOwnerDispatcher);
    } else {
      this.internalAccessControlDispatcher = null;
      this.internalOwnerDispatcher = null;
      this.bulkManager = null;
      this.futureGrantManager = null;
    }
  }

  private void initEntityStoreAndCatalogManager() {
    this.entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    this.idGenerator = new RandomIdGenerator();
    this.lockManager = new LockManager(config);

    // CatalogManager must be initialized before MetalakeManager so force-drop can remove child
    // catalogs through CatalogManager.dropCatalog, the same path used by FilesetCatalogOperations.
    // CatalogManager registers its own change-log listener with compatible entity stores, so no
    // external poller wiring is needed here.
    this.catalogManager = new CatalogManager(config, entityStore, idGenerator, secretManager);
  }

  private void initGravitinoServerComponents() {
    MetadataOperations metadataOperations = initMetadataComponents();
    initPublicMetadataDispatchers(metadataOperations);

    this.auxServiceManager = new AuxiliaryServiceManager();
    this.auxServiceManager.serviceInit(config);

    JobManager jobManager = new JobManager(config, entityStore, idGenerator);
    JobTemplateValidationDispatcher validationDispatcher =
        new JobTemplateValidationDispatcher(jobManager);
    this.internalJobOperationDispatcher = validationDispatcher;
    JobHookDispatcher jobHookDispatcher = new JobHookDispatcher(validationDispatcher);
    this.jobOperationDispatcher = new JobEventDispatcher(eventBus, jobHookDispatcher);

    // Register built-in job template event listener to automatically register templates
    // when metalakes are created
    BuiltInJobTemplateEventListener builtInJobTemplateListener =
        new BuiltInJobTemplateEventListener(jobManager, entityStore, idGenerator);
    eventListenerManager.addEventListener("builtin-job-template", builtInJobTemplateListener);
  }

  private void initPublicMetadataDispatchers(MetadataOperations metadataOperations) {
    // Create and initialize metalake related modules, the operation chain is:
    // MetalakeEventDispatcher -> MetalakeNormalizeDispatcher -> MetalakeHookDispatcher ->
    // MetalakeManager
    MetalakeHookDispatcher metalakeHookDispatcher = new MetalakeHookDispatcher(metalakeManager);
    MetalakeNormalizeDispatcher metalakeNormalizeDispatcher =
        new MetalakeNormalizeDispatcher(metalakeHookDispatcher);
    this.metalakeDispatcher = new MetalakeEventDispatcher(eventBus, metalakeNormalizeDispatcher);

    // CatalogEventDispatcher -> CatalogNormalizeDispatcher -> CatalogHookDispatcher ->
    // CatalogManager
    CatalogHookDispatcher catalogHookDispatcher = new CatalogHookDispatcher(catalogManager);
    CatalogNormalizeDispatcher catalogNormalizeDispatcher =
        new CatalogNormalizeDispatcher(catalogHookDispatcher);
    this.catalogDispatcher = new CatalogEventDispatcher(eventBus, catalogNormalizeDispatcher);

    FilesetHookDispatcher filesetHookDispatcher =
        new FilesetHookDispatcher(metadataOperations.filesetOperationDispatcher);
    FilesetNormalizeDispatcher filesetNormalizeDispatcher =
        new FilesetNormalizeDispatcher(filesetHookDispatcher, catalogManager);
    this.filesetDispatcher = new FilesetEventDispatcher(eventBus, filesetNormalizeDispatcher);

    SchemaHookDispatcher schemaHookDispatcher =
        new SchemaHookDispatcher(metadataOperations.schemaOperationDispatcher);
    SchemaNormalizeDispatcher schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(schemaHookDispatcher, catalogManager);
    this.schemaDispatcher = new SchemaEventDispatcher(eventBus, schemaNormalizeDispatcher);

    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    TableHookDispatcher tableHookDispatcher =
        new TableHookDispatcher(tableOperationDispatcher, this::internalOwnerDispatcher);
    TableNormalizeDispatcher tableNormalizeDispatcher =
        new TableNormalizeDispatcher(tableHookDispatcher, catalogManager);
    this.tableDispatcher = new TableEventDispatcher(eventBus, tableNormalizeDispatcher);

    // TODO: We can install hooks when we need, we only supports ownership post hook,
    //  partition doesn't have ownership, so we don't need it now.
    this.partitionDispatcher = new PartitionEventDispatcher(eventBus, internalPartitionDispatcher);

    TopicHookDispatcher topicHookDispatcher =
        new TopicHookDispatcher(metadataOperations.topicOperationDispatcher);
    TopicNormalizeDispatcher topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(topicHookDispatcher, catalogManager);
    this.topicDispatcher = new TopicEventDispatcher(eventBus, topicNormalizeDispatcher);

    ModelHookDispatcher modelHookDispatcher =
        new ModelHookDispatcher(metadataOperations.modelOperationDispatcher);
    ModelNormalizeDispatcher modelNormalizeDispatcher =
        new ModelNormalizeDispatcher(modelHookDispatcher, catalogManager);
    this.modelDispatcher = new ModelEventDispatcher(eventBus, modelNormalizeDispatcher);

    // Create and initialize Function related modules, the operation chain is:
    // FunctionEventDispatcher -> FunctionNormalizeDispatcher -> FunctionHookDispatcher ->
    // FunctionOperationDispatcher
    FunctionHookDispatcher functionHookDispatcher =
        new FunctionHookDispatcher(
            metadataOperations.functionOperationDispatcher, this::internalOwnerDispatcher);
    FunctionNormalizeDispatcher functionNormalizeDispatcher =
        new FunctionNormalizeDispatcher(functionHookDispatcher, catalogManager);
    this.functionDispatcher = new FunctionEventDispatcher(eventBus, functionNormalizeDispatcher);

    // View operation chain: ViewEventDispatcher -> ViewNormalizeDispatcher -> ViewHookDispatcher
    // -> ViewOperationDispatcher.
    ViewOperationDispatcher viewOperationDispatcher =
        new ViewOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    ViewHookDispatcher viewHookDispatcher =
        new ViewHookDispatcher(viewOperationDispatcher, this::internalOwnerDispatcher);
    ViewNormalizeDispatcher viewNormalizeDispatcher =
        new ViewNormalizeDispatcher(viewHookDispatcher, catalogManager);
    this.viewDispatcher = new ViewEventDispatcher(eventBus, viewNormalizeDispatcher);

    this.semanticModelDispatcher =
        new SemanticModelEventDispatcher(eventBus, semanticModelDispatcher);

    this.statisticDispatcher = new StatisticEventDispatcher(eventBus, internalStatisticDispatcher);

    // Create and initialize access control related modules
    if (internalAccessControlDispatcher != null) {
      AccessControlHookDispatcher accessControlHookDispatcher =
          new AccessControlHookDispatcher(internalAccessControlDispatcher);
      this.accessControlDispatcher =
          new AccessControlEventDispatcher(eventBus, accessControlHookDispatcher);
      this.ownerDispatcher = new OwnerEventManager(eventBus, internalOwnerDispatcher);
    } else {
      this.accessControlDispatcher = null;
      this.ownerDispatcher = null;
    }

    // Create and initialize Tag related modules
    TagHookDispatcher tagHookDispatcher = new TagHookDispatcher(internalTagDispatcher);
    this.tagDispatcher = new TagEventDispatcher(eventBus, tagHookDispatcher);

    PolicyHookDispatcher policyHookDispatcher = new PolicyHookDispatcher(internalPolicyDispatcher);
    this.policyDispatcher = new PolicyEventDispatcher(eventBus, policyHookDispatcher);
  }

  private static final class MetadataOperations {
    private final FilesetOperationDispatcher filesetOperationDispatcher;
    private final SchemaOperationDispatcher schemaOperationDispatcher;
    private final TopicOperationDispatcher topicOperationDispatcher;
    private final ModelOperationDispatcher modelOperationDispatcher;
    private final FunctionOperationDispatcher functionOperationDispatcher;

    private MetadataOperations(
        FilesetOperationDispatcher filesetOperationDispatcher,
        SchemaOperationDispatcher schemaOperationDispatcher,
        TopicOperationDispatcher topicOperationDispatcher,
        ModelOperationDispatcher modelOperationDispatcher,
        FunctionOperationDispatcher functionOperationDispatcher) {
      this.filesetOperationDispatcher = filesetOperationDispatcher;
      this.schemaOperationDispatcher = schemaOperationDispatcher;
      this.topicOperationDispatcher = topicOperationDispatcher;
      this.modelOperationDispatcher = modelOperationDispatcher;
      this.functionOperationDispatcher = functionOperationDispatcher;
    }
  }
}
