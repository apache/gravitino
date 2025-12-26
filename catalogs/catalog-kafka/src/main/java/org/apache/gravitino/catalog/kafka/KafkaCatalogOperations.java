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
package org.apache.gravitino.catalog.kafka;

import static org.apache.gravitino.StringIdentifier.DUMMY_ID;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.StringIdentifier.newPropertiesWithId;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.storage.RandomIdGenerator.MAX_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCatalogOperations implements CatalogOperations, SupportsSchemas, TopicCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalogOperations.class);
  private static final String DEFAULT_SCHEMA_NAME = "default";
  @VisibleForTesting static final String CLIENT_ID_TEMPLATE = "%s-%s.%s";

  private final EntityStore store;
  private final IdGenerator idGenerator;
  @VisibleForTesting NameIdentifier defaultSchemaIdent;
  @VisibleForTesting Properties adminClientConfig;
  private CatalogInfo info;
  private AdminClient adminClient;
  private HasPropertyMetadata propertiesMetadata;

  @VisibleForTesting
  KafkaCatalogOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  public KafkaCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore(), GravitinoEnv.getInstance().idGenerator());
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.propertiesMetadata = propertiesMetadata;
    Preconditions.checkArgument(
        config.containsKey(KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS),
        "Missing configuration: %s",
        KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS);
    Preconditions.checkArgument(config.containsKey(ID_KEY), "Missing configuration: %s", ID_KEY);

    this.info = info;
    this.defaultSchemaIdent =
        NameIdentifier.of(info.namespace().level(0), info.name(), DEFAULT_SCHEMA_NAME);

    // Initialize the Kafka AdminClient configuration
    adminClientConfig = new Properties();

    Map<String, String> bypassConfigs =
        config.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_BYPASS_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CATALOG_BYPASS_PREFIX.length()),
                    Map.Entry::getValue));
    adminClientConfig.putAll(bypassConfigs);
    adminClientConfig.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.get(KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));
    // use Gravitino catalog id as the admin client id
    adminClientConfig.put(
        AdminClientConfig.CLIENT_ID_CONFIG,
        String.format(CLIENT_ID_TEMPLATE, config.get(ID_KEY), info.namespace(), info.name()));

    try {
      adminClient = AdminClient.create(adminClientConfig);
    } catch (KafkaException e) {
      if (e.getCause() instanceof ConfigException) {
        throw new IllegalArgumentException(
            "Invalid configuration for Kafka AdminClient: " + e.getCause().getMessage(), e);
      }
      throw new RuntimeException("Failed to create Kafka AdminClient", e);
    }
    createDefaultSchemaIfNecessary();
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    checkSchemaExists(schemaIdent);

    try {
      ListTopicsResult result = adminClient.listTopics();
      Set<String> topicNames = result.names().get();
      return topicNames.stream()
          .map(name -> NameIdentifier.of(namespace, name))
          .toArray(NameIdentifier[]::new);
    } catch (ExecutionException e) {
      throw new RuntimeException(
          String.format(
              "Failed to list topics under the schema %s: %s",
              namespace, e.getCause().getMessage()),
          e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to list topics under the schema " + namespace, e);
    }
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    try {
      adminClient.listTopics().names().get();
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run listTopics in Kafka: %s", e.getMessage());
    }
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    checkSchemaExists(schemaIdent);

    DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(ident.name()));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, ident.name());
    DescribeConfigsResult configsResult =
        adminClient.describeConfigs(Collections.singleton(configResource));

    int partitions;
    int replicationFactor;
    Uuid topicId;
    Map<String, String> properties = Maps.newHashMap();
    try {
      TopicDescription topicDescription = result.topicNameValues().get(ident.name()).get();
      partitions = topicDescription.partitions().size();
      replicationFactor = topicDescription.partitions().get(0).replicas().size();
      topicId = topicDescription.topicId();

      Config topicConfigs = configsResult.all().get().get(configResource);
      topicConfigs.entries().forEach(e -> properties.put(e.name(), e.value()));
      properties.put(KafkaTopicPropertiesMetadata.PARTITION_COUNT, String.valueOf(partitions));
      properties.put(
          KafkaTopicPropertiesMetadata.REPLICATION_FACTOR, String.valueOf(replicationFactor));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new NoSuchTopicException(e, "Topic %s does not exist", ident);
      } else {
        throw new RuntimeException("Failed to load topic " + ident.name() + " from Kafka", e);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to load topic " + ident.name() + " from Kafka", e);
    }

    LOG.info("Loaded topic {} from Kafka", ident);

    return KafkaTopic.builder()
        .withName(ident.name())
        // Because there is no way to store the Gravitino ID in Kafka, therefor we use the topic ID
        // as the Gravitino ID
        .withProperties(newPropertiesWithId(convertToGravitinoId(topicId), properties))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    checkSchemaExists(schemaIdent);

    try {
      CreateTopicsResult createTopicsResult =
          adminClient.createTopics(Collections.singleton(buildNewTopic(ident, properties)));
      // Wait for topic creation to complete
      createTopicsResult.all().get();

      Uuid topicId = createTopicsResult.topicId(ident.name()).get();
      Integer numPartitions = createTopicsResult.numPartitions(ident.name()).get();
      Integer replicationFactor = createTopicsResult.replicationFactor(ident.name()).get();
      Config topicConfigs = createTopicsResult.config(ident.name()).get();

      Map<String, String> created_properties = Maps.newHashMap();

      LOG.info(
          "Created topic {}[id: {}] with {} partitions and replication factor {}",
          ident,
          topicId,
          numPartitions,
          replicationFactor);

      created_properties.put(
          KafkaTopicPropertiesMetadata.PARTITION_COUNT, String.valueOf(numPartitions));
      created_properties.put(
          KafkaTopicPropertiesMetadata.REPLICATION_FACTOR, String.valueOf(replicationFactor));
      topicConfigs.entries().forEach(e -> created_properties.put(e.name(), e.value()));

      return KafkaTopic.builder()
          .withName(ident.name())
          .withComment(comment)
          // Because there is no way to store the Gravitino ID in Kafka, therefor we use the topic
          // ID as the Gravitino ID
          .withProperties(newPropertiesWithId(convertToGravitinoId(topicId), created_properties))
          .withAuditInfo(
              AuditInfo.builder()
                  .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                  .withCreateTime(Instant.now())
                  .build())
          .build();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        throw new TopicAlreadyExistsException(e, "Topic %s already exists", ident);

      } else if (e.getCause() instanceof InvalidReplicationFactorException) {
        throw new IllegalArgumentException(
            "Invalid replication factor for topic " + ident + e.getCause().getMessage(), e);

      } else if (e.getCause() instanceof InvalidConfigurationException) {
        throw new IllegalArgumentException(
            "Invalid properties for topic " + ident + e.getCause().getMessage(), e);

      } else {
        throw new RuntimeException("Failed to create topic in Kafka" + ident, e);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to create topic in Kafka" + ident, e);
    }
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    checkSchemaExists(schemaIdent);

    KafkaTopic topic = (KafkaTopic) loadTopic(ident);
    String newComment = topic.comment();
    int oldPartitionCount =
        Integer.parseInt(topic.properties().get(KafkaTopicPropertiesMetadata.PARTITION_COUNT));
    int newPartitionCount = oldPartitionCount;
    Map<String, String> alteredProperties = Maps.newHashMap(topic.properties());
    List<AlterConfigOp> alterConfigOps = Lists.newArrayList();
    for (TopicChange change : changes) {
      if (change instanceof TopicChange.UpdateTopicComment) {
        newComment = ((TopicChange.UpdateTopicComment) change).getNewComment();

      } else if (change instanceof TopicChange.SetProperty) {
        TopicChange.SetProperty setProperty = (TopicChange.SetProperty) change;
        if (KafkaTopicPropertiesMetadata.PARTITION_COUNT.equals(setProperty.getProperty())) {
          // alter partition count
          newPartitionCount = setPartitionCount(setProperty, newPartitionCount, alteredProperties);
        } else {
          // alter other properties
          setProperty(setProperty, alteredProperties, alterConfigOps);
        }

      } else if (change instanceof TopicChange.RemoveProperty) {
        removeProperty((TopicChange.RemoveProperty) change, alteredProperties, alterConfigOps);

      } else {
        throw new IllegalArgumentException("Unsupported topic change: " + change);
      }
    }

    if (newPartitionCount != oldPartitionCount) {
      doPartitionCountIncrement(ident.name(), newPartitionCount);
    }

    if (!alterConfigOps.isEmpty()) {
      doAlterTopicConfig(ident.name(), alterConfigOps);
    }

    return KafkaTopic.builder()
        .withName(ident.name())
        .withComment(newComment)
        .withProperties(alteredProperties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(topic.auditInfo().creator())
                .withCreateTime(topic.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    checkSchemaExists(schemaIdent);

    try {
      adminClient.deleteTopics(Collections.singleton(ident.name())).all().get();
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        return false;
      } else {
        throw new RuntimeException("Failed to drop topic " + ident.name() + " from Kafka", e);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to drop topic " + ident.name() + " from Kafka", e);
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      List<SchemaEntity> schemas =
          store.list(namespace, SchemaEntity.class, Entity.EntityType.SCHEMA);
      return schemas.stream()
          .map(s -> NameIdentifier.of(namespace, s.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list schemas under namespace " + namespace, e);
    }
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    // It appears that the "default" schema suffices, so there is no need to support creating schema
    // currently
    throw new UnsupportedOperationException(
        "Kafka catalog does not support schema creation "
            + "because the \"default\" schema already includes all topics");
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      SchemaEntity schema = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);

      return KafkaSchema.builder()
          .withName(schema.name())
          .withComment(schema.comment())
          .withProperties(schema.properties())
          .withAuditInfo(schema.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, "Schema %s does not exist", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + ident, ioe);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    if (ident.equals(defaultSchemaIdent)) {
      throw new IllegalArgumentException("Cannot alter the default schema");
    }

    // TODO: Implement altering schema after adding support for schema creation
    throw new UnsupportedOperationException("Kafka catalog does not support schema alteration");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (ident.equals(defaultSchemaIdent)) {
      throw new IllegalArgumentException("Cannot drop the default schema");
    }
    // TODO: Implement dropping schema after adding support for schema creation
    throw new UnsupportedOperationException("Kafka catalog does not support schema deletion");
  }

  @Override
  public void close() throws IOException {
    if (adminClient != null) {
      adminClient.close();
      adminClient = null;
    }
  }

  /**
   * Make sure the schema exists, otherwise throw an exception.
   *
   * @param ident The schema identifier.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  private void checkSchemaExists(NameIdentifier ident) throws NoSuchSchemaException {
    if (!schemaExists(ident)) {
      LOG.warn("Kafka catalog schema {} does not exist", ident);
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    }
  }

  /**
   * Set the new partition count for the topic if it is greater than the current partition count.
   *
   * @param setProperty The property change to set the partition count.
   * @param currentPartitionCount The current partition count.
   * @param properties The properties map to update.
   * @return The new partition count.
   */
  private int setPartitionCount(
      TopicChange.SetProperty setProperty,
      int currentPartitionCount,
      Map<String, String> properties) {
    Preconditions.checkArgument(
        KafkaTopicPropertiesMetadata.PARTITION_COUNT.equals(setProperty.getProperty()),
        "Invalid property: %s",
        setProperty);

    int targetPartitionCount = Integer.parseInt(setProperty.getValue());
    if (targetPartitionCount == currentPartitionCount) {
      return currentPartitionCount;
    } else if (targetPartitionCount < currentPartitionCount) {
      throw new IllegalArgumentException(
          "Cannot reduce partition count from "
              + currentPartitionCount
              + " to "
              + targetPartitionCount);
    } else {
      properties.put(KafkaTopicPropertiesMetadata.PARTITION_COUNT, setProperty.getValue());
      return targetPartitionCount;
    }
  }

  private void setProperty(
      TopicChange.SetProperty setProperty,
      Map<String, String> alteredProperties,
      List<AlterConfigOp> alterConfigOps) {
    alteredProperties.put(setProperty.getProperty(), setProperty.getValue());
    alterConfigOps.add(
        new AlterConfigOp(
            new ConfigEntry(setProperty.getProperty(), setProperty.getValue()),
            AlterConfigOp.OpType.SET));
  }

  private void removeProperty(
      TopicChange.RemoveProperty removeProperty,
      Map<String, String> alteredProperties,
      List<AlterConfigOp> alterConfigOps) {
    Preconditions.checkArgument(
        !KafkaTopicPropertiesMetadata.PARTITION_COUNT.equals(removeProperty.getProperty()),
        "Cannot remove partition count");
    alteredProperties.remove(removeProperty.getProperty());
    alterConfigOps.add(
        new AlterConfigOp(
            new ConfigEntry(removeProperty.getProperty(), null), AlterConfigOp.OpType.DELETE));
  }

  private void doPartitionCountIncrement(String topicName, int newPartitionCount) {
    try {
      adminClient
          .createPartitions(
              Collections.singletonMap(topicName, NewPartitions.increaseTo(newPartitionCount)))
          .all()
          .get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to increase partition count for topic " + topicName, e);
    }
  }

  private void doAlterTopicConfig(String topicName, List<AlterConfigOp> alterConfigOps) {
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    try {
      adminClient
          .incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps))
          .all()
          .get();
    } catch (UnknownTopicOrPartitionException e) {
      throw new NoSuchTopicException(e, "Topic %s does not exist", topicName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter topic properties for topic " + topicName, e);
    }
  }

  private StringIdentifier convertToGravitinoId(Uuid topicId) {
    return StringIdentifier.fromId(topicId.getLeastSignificantBits() & MAX_ID);
  }

  private NewTopic buildNewTopic(NameIdentifier ident, Map<String, String> properties) {
    Optional<Integer> partitionCount =
        Optional.ofNullable(
            (Integer)
                propertiesMetadata
                    .topicPropertiesMetadata()
                    .getOrDefault(properties, KafkaTopicPropertiesMetadata.PARTITION_COUNT));
    Optional<Short> replicationFactor =
        Optional.ofNullable(
            (Short)
                propertiesMetadata
                    .topicPropertiesMetadata()
                    .getOrDefault(properties, KafkaTopicPropertiesMetadata.REPLICATION_FACTOR));
    NewTopic newTopic = new NewTopic(ident.name(), partitionCount, replicationFactor);
    return newTopic.configs(buildNewTopicConfigs(properties));
  }

  private Map<String, String> buildNewTopicConfigs(Map<String, String> properties) {
    Map<String, String> topicConfigs = Maps.newHashMap(properties);
    topicConfigs.remove(KafkaTopicPropertiesMetadata.PARTITION_COUNT);
    topicConfigs.remove(KafkaTopicPropertiesMetadata.REPLICATION_FACTOR);
    topicConfigs.remove(ID_KEY);
    return topicConfigs;
  }

  private void createDefaultSchemaIfNecessary() {
    // If the default schema already exists or is testConnection operation, do nothing
    try {
      if (DUMMY_ID.toString().equals(info.properties().get(ID_KEY))
          || store.exists(defaultSchemaIdent, Entity.EntityType.SCHEMA)) {
        return;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to check if schema " + defaultSchemaIdent + " exists", e);
    }

    // Create the default schema
    long uid = idGenerator.nextId();
    ImmutableMap<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(ID_KEY, StringIdentifier.fromId(uid).toString())
            .build();

    SchemaEntity defaultSchema =
        SchemaEntity.builder()
            .withName(defaultSchemaIdent.name())
            .withId(uid)
            .withNamespace(NamespaceUtil.ofSchema(info.namespace().level(0), info.name()))
            .withComment("The default schema of Kafka catalog including all topics")
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(info.auditInfo().creator())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(defaultSchema, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create default schema for Kafka catalog", ioe);
    }
  }
}
