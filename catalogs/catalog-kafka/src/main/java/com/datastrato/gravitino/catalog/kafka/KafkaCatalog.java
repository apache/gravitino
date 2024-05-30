/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.capability.Capability;
import java.util.Map;

/** Kafka catalog is a messaging catalog that can manage topics on the Kafka messaging system. */
public class KafkaCatalog extends BaseCatalog<KafkaCatalog> {

  static final KafkaCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new KafkaCatalogPropertiesMetadata();
  static final KafkaSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new KafkaSchemaPropertiesMetadata();
  static final KafkaTopicPropertiesMetadata TOPIC_PROPERTIES_METADATA =
      new KafkaTopicPropertiesMetadata();

  @Override
  public String shortName() {
    return "kafka";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    KafkaCatalogOperations ops = new KafkaCatalogOperations();
    return ops;
  }

  @Override
  protected Capability newCapability() {
    return new KafkaCatalogCapability();
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
}
