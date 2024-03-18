/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.rel.SupportsSchemas;
import java.util.Map;

/** Kafka catalog is a messaging catalog that can manage topics on the Kafka messaging system. */
public class KafkaCatalog extends BaseCatalog<KafkaCatalog> {

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
  public SupportsSchemas asSchemas() throws UnsupportedOperationException {
    return (KafkaCatalogOperations) ops();
  }

  @Override
  public TopicCatalog asTopicCatalog() throws UnsupportedOperationException {
    return (KafkaCatalogOperations) ops();
  }
}
