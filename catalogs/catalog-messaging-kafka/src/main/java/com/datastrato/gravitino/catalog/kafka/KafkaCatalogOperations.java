/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaCatalogOperations implements CatalogOperations, SupportsSchemas, TopicCatalog {

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
    adminClientConfig.put(BOOTSTRAP_SERVERS, config.get(BOOTSTRAP_SERVERS));
    // use gravitino catalog id as the admin client id
    adminClientConfig.put("client.id", config.get(ID_KEY));

    createDefaultSchema();
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) throws NoSuchTopicException {
    throw new UnsupportedOperationException();
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
  public void close() throws IOException {}

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Kafka catalog does not support fileset operations");
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Kafka catalog does not support table operations");
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
