/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.Collections;
import java.util.Map;

public class KafkaCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  // The "bootstrap.servers" property specifies the Kafka broker(s) to connect to, allowing for
  // multiple brokers by comma-separating them.
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  private static final Map<String, PropertyEntry<?>> KAFKA_CATALOG_PROPERTY_ENTRIES =
      Collections.singletonMap(
          BOOTSTRAP_SERVERS,
          PropertyEntry.stringRequiredPropertyEntry(
              BOOTSTRAP_SERVERS,
              "The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them",
              true /* immutable */,
              false /* hidden */));

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return KAFKA_CATALOG_PROPERTY_ENTRIES;
  }
}
