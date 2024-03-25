/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.PARTITION_COUNT;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.REPLICATION_FACTOR;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCatalogOperations implements CatalogOperations, SupportsSchemas, TopicCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalogOperations.class);
  private static final KafkaCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new KafkaCatalogPropertiesMetadata();
  private static final KafkaSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new KafkaSchemaPropertiesMetadata();
  private static final KafkaTopicPropertiesMetadata TOPIC_PROPERTIES_METADATA =
      new KafkaTopicPropertiesMetadata();

  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final String DEFAULT_SCHEMA_NAME = "default";
  @VisibleForTesting NameIdentifier defaultSchemaIdent;
  @VisibleForTesting Properties adminClientConfig;
  private CatalogInfo info;
  private AdminClient adminClient;

  @VisibleForTesting
  KafkaCatalogOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  public KafkaCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore(), GravitinoEnv.getInstance().idGenerator());
  }

  @Override
  public void initialize(Map<String, String> config, CatalogInfo info) throws RuntimeException {
    Preconditions.checkArgument(
        config.containsKey(BOOTSTRAP_SERVERS), "Missing configuration: %s", BOOTSTRAP_SERVERS);
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
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(BOOTSTRAP_SERVERS));
    // use gravitino catalog id as the admin client id
    adminClientConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, config.get(ID_KEY));

    createDefaultSchema();
    adminClient = AdminClient.create(adminClientConfig);
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      LOG.warn("Kafka catalog schema {} does not exist", schemaIdent);
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }

    try {
      ListTopicsResult result = adminClient.listTopics();
      Set<String> topicNames = result.names().get();
      return topicNames.stream()
          .map(name -> NameIdentifier.of(namespace, name))
          .toArray(NameIdentifier[]::new);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to list topics under the schema " + namespace, e);
    }
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(ident.name()));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, ident.name());
    DescribeConfigsResult configsResult =
        adminClient.describeConfigs(Collections.singleton(configResource));
    int partitions;
    int replicationFactor;
    Map<String, String> properties = Maps.newHashMap();
    try {
      TopicDescription topicDescription = result.topicNameValues().get(ident.name()).get();
      partitions = topicDescription.partitions().size();
      replicationFactor = topicDescription.partitions().get(0).replicas().size();

      Config topicConfigs = configsResult.all().get().get(configResource);
      topicConfigs.entries().forEach(e -> properties.put(e.name(), e.value()));
      properties.put(PARTITION_COUNT, String.valueOf(partitions));
      properties.put(REPLICATION_FACTOR, String.valueOf(replicationFactor));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
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
        .withProperties(properties)
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
    if (!schemaExists(schemaIdent)) {
      LOG.warn("Kafka catalog schema {} does not exist", schemaIdent);
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }

    try {
      CreateTopicsResult createTopicsResult =
          adminClient.createTopics(Collections.singleton(buildNewTopic(ident, properties)));
      LOG.info(
          "Created topic {} with {} partitions and replication factor {}",
          ident,
          createTopicsResult.numPartitions(ident.name()).get(),
          createTopicsResult.replicationFactor(ident.name()).get());
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

    return KafkaTopic.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    KafkaTopic topic = (KafkaTopic) loadTopic(ident);
    String newComment = topic.comment();
    int oldPartitionCount = Integer.parseInt(topic.properties().get(PARTITION_COUNT));
    int newPartitionCount = oldPartitionCount;
    Map<String, String> alteredProperties = Maps.newHashMap(topic.properties());
    List<AlterConfigOp> alterConfigOps = Lists.newArrayList();
    for (TopicChange change : changes) {
      if (change instanceof TopicChange.UpdateTopicComment) {
        newComment = ((TopicChange.UpdateTopicComment) change).getNewComment();

      } else if (change instanceof TopicChange.SetProperty) {
        TopicChange.SetProperty setProperty = (TopicChange.SetProperty) change;
        // alter partition count
        if (PARTITION_COUNT.equals(setProperty.getProperty())) {
          int targetPartitionCount = Integer.parseInt(setProperty.getValue());
          if (targetPartitionCount == newPartitionCount) {
            continue;
          } else if (targetPartitionCount < newPartitionCount) {
            throw new IllegalArgumentException(
                "Cannot reduce partition count from "
                    + newPartitionCount
                    + " to "
                    + targetPartitionCount);
          } else {
            newPartitionCount = targetPartitionCount;
            alteredProperties.put(PARTITION_COUNT, setProperty.getValue());
            continue;
          }
        }

        // alter other properties
        alteredProperties.put(setProperty.getProperty(), setProperty.getValue());
        alterConfigOps.add(
            new AlterConfigOp(
                new ConfigEntry(setProperty.getProperty(), setProperty.getValue()),
                AlterConfigOp.OpType.SET));

      } else if (change instanceof TopicChange.RemoveProperty) {
        TopicChange.RemoveProperty removeProperty = (TopicChange.RemoveProperty) change;
        Preconditions.checkArgument(
            !PARTITION_COUNT.equals(removeProperty.getProperty()), "Cannot remove partition count");
        alteredProperties.remove(removeProperty.getProperty());
        alterConfigOps.add(
            new AlterConfigOp(
                new ConfigEntry(removeProperty.getProperty(), null), AlterConfigOp.OpType.DELETE));

      } else {
        throw new IllegalArgumentException("Unsupported topic change: " + change);
      }
    }

    // increase partition count
    if (newPartitionCount != oldPartitionCount) {
      try {
        adminClient
            .createPartitions(
                Collections.singletonMap(ident.name(), NewPartitions.increaseTo(newPartitionCount)))
            .all()
            .get();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to increase partition count for topic " + ident.name(), e);
      }
    }

    // alter topic properties
    if (!alterConfigOps.isEmpty()) {
      ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, ident.name());
      try {
        adminClient
            .incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps))
            .all()
            .get();
      } catch (UnknownTopicOrPartitionException e) {
        throw new NoSuchTopicException(e, "Topic %s does not exist", ident);
      } catch (Exception e) {
        throw new RuntimeException("Failed to alter topic properties for topic " + ident.name(), e);
      }
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
  public boolean dropTopic(NameIdentifier ident) throws NoSuchTopicException {
    try {
      adminClient.deleteTopics(Collections.singleton(ident.name())).all().get();
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
        throw new NoSuchTopicException(e, "Topic %s does not exist", ident);
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
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    return TOPIC_PROPERTIES_METADATA;
  }

  @Override
  public void close() throws IOException {
    adminClient.close();
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Kafka catalog does not support fileset operations");
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Kafka catalog does not support table operations");
  }

  private NewTopic buildNewTopic(NameIdentifier ident, Map<String, String> properties) {
    Optional<Integer> partitionCount =
        Optional.ofNullable(
            (int) TOPIC_PROPERTIES_METADATA.getOrDefault(properties, PARTITION_COUNT));
    Optional<Short> replicationFactor =
        Optional.ofNullable(
            (short) TOPIC_PROPERTIES_METADATA.getOrDefault(properties, REPLICATION_FACTOR));
    NewTopic newTopic = new NewTopic(ident.name(), partitionCount, replicationFactor);
    return newTopic.configs(buildNewTopicConfigs(properties));
  }

  private Map<String, String> buildNewTopicConfigs(Map<String, String> properties) {
    Map<String, String> topicConfigs = Maps.newHashMap(properties);
    topicConfigs.remove(PARTITION_COUNT);
    topicConfigs.remove(REPLICATION_FACTOR);
    return topicConfigs;
  }

  private void createDefaultSchema() {
    // If the default schema already exists, do nothing
    try {
      if (store.exists(defaultSchemaIdent, Entity.EntityType.SCHEMA)) {
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
            .put(BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY, Boolean.TRUE.toString())
            .build();

    SchemaEntity defaultSchema =
        SchemaEntity.builder()
            .withName(defaultSchemaIdent.name())
            .withId(uid)
            .withNamespace(Namespace.ofSchema(info.namespace().level(0), info.name()))
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
