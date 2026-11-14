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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Property definitions for Apache Gravitino Kafka catalogs. */
public class KafkaCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  /** Comma-separated Kafka broker host:port pairs to connect to. */
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  /**
   * Optional Iceberg catalog name in the same Metalake where AutoMQ Table Topics are materialized.
   */
  public static final String TABLE_TOPIC_ICEBERG_CATALOG = "table-topic.iceberg-catalog";

  /**
   * Default Iceberg namespace used when a Table Topic does not set automq.table.topic.namespace.
   */
  public static final String TABLE_TOPIC_DEFAULT_NAMESPACE = "table-topic.default-namespace";

  private static final Map<String, PropertyEntry<?>> KAFKA_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              BOOTSTRAP_SERVERS,
              PropertyEntry.stringRequiredPropertyEntry(
                  BOOTSTRAP_SERVERS,
                  "The Kafka broker(s) to connect to, allowing for multiple brokers by "
                      + "comma-separating them",
                  false /* immutable */,
                  false /* hidden */))
          .put(
              TABLE_TOPIC_ICEBERG_CATALOG,
              nonBlankStringOptionalPropertyEntry(
                  TABLE_TOPIC_ICEBERG_CATALOG,
                  "The name of the Iceberg catalog in the same metalake where AutoMQ Table Topics"
                      + " are materialized",
                  null /* default value */))
          .put(
              TABLE_TOPIC_DEFAULT_NAMESPACE,
              nonBlankStringOptionalPropertyEntry(
                  TABLE_TOPIC_DEFAULT_NAMESPACE,
                  "The default Iceberg namespace (schema) used when an AutoMQ Table Topic does"
                      + " not specify automq.table.topic.namespace",
                  "default" /* default value */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return KAFKA_CATALOG_PROPERTY_ENTRIES;
  }

  private static PropertyEntry<String> nonBlankStringOptionalPropertyEntry(
      String name, String description, String defaultValue) {
    return new PropertyEntry.Builder<String>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(String.class)
        .withDefaultValue(defaultValue)
        .withDecoder(
            value -> {
              if (value != null && value.trim().isEmpty()) {
                throw new IllegalArgumentException("Property " + name + " must not be blank");
              }
              return value == null ? null : value.trim();
            })
        .withEncoder(v -> v)
        .withHidden(false)
        .withReserved(false)
        .build();
  }
}
