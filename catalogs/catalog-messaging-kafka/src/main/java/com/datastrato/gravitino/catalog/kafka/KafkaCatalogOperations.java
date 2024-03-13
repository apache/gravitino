/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KafkaCatalogOperations implements CatalogOperations, SupportsSchemas, TopicCatalog {

  private static final KafkaCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new KafkaCatalogPropertiesMetadata();
  private static final KafkaSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new KafkaSchemaPropertiesMetadata();
  private static final KafkaTopicPropertiesMetadata TOPIC_PROPERTIES_METADATA =
      new KafkaTopicPropertiesMetadata();

  private final EntityStore store;
  private CatalogEntity entity;

  public KafkaCatalogOperations() {
    this.store = GravitinoEnv.getInstance().entityStore();
  }

  @Override
  public void initialize(Map<String, String> config, CatalogEntity entity) throws RuntimeException {
    this.entity = entity;
    // TODO: Implement Kafka catalog initialization, such as creating a default schema.
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
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException();
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
}
