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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Property definitions and Table Topic validation rules for Kafka catalog topics. */
public class KafkaTopicPropertiesMetadata extends BasePropertiesMetadata {
  /** Number of partitions configured for the topic. */
  public static final String PARTITION_COUNT = "partition-count";

  /** Replication factor configured for the topic. */
  public static final String REPLICATION_FACTOR = "replication-factor";

  /** Persisted AutoMQ Table Topic enablement flag. */
  public static final String AUTOMQ_TABLE_TOPIC_ENABLE = "automq.table.topic.enable";

  /** AutoMQ Table Topic Iceberg commit interval in milliseconds. */
  public static final String AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS =
      "automq.table.topic.commit.interval.ms";

  /** Target Iceberg namespace (schema) configured on the AutoMQ Table Topic. */
  public static final String AUTOMQ_TABLE_TOPIC_NAMESPACE = "automq.table.topic.namespace";

  /** AutoMQ Table Topic schema evolution mode. */
  public static final String AUTOMQ_TABLE_TOPIC_SCHEMA_TYPE = "automq.table.topic.schema.type";

  /** Comma-separated primary key column names for Iceberg equality deletes and upserts. */
  public static final String AUTOMQ_TABLE_TOPIC_ID_COLUMNS = "automq.table.topic.id.columns";

  /** Iceberg partition transform expressions for the materialized Table Topic. */
  public static final String AUTOMQ_TABLE_TOPIC_PARTITION_BY = "automq.table.topic.partition.by";

  /** AutoMQ Table Topic upsert enablement flag. */
  public static final String AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE = "automq.table.topic.upsert.enable";

  /** Field name carrying the CDC operation code for AutoMQ Table Topic materialization. */
  public static final String AUTOMQ_TABLE_TOPIC_CDC_FIELD = "automq.table.topic.cdc.field";

  /** Deserialization error tolerance policy for AutoMQ Table Topic ingestion. */
  public static final String AUTOMQ_TABLE_TOPIC_ERRORS_TOLERANCE =
      "automq.table.topic.errors.tolerance";

  /** Synthetic read-only property indicating whether Table Topic materialization is enabled. */
  public static final String TABLE_TOPIC_ENABLED = "table-topic.enabled";

  /** Synthetic read-only property carrying the linked Iceberg catalog name. */
  public static final String TABLE_TOPIC_ICEBERG_CATALOG = "table-topic.iceberg-catalog";

  /** Synthetic read-only property carrying the resolved target Iceberg namespace. */
  public static final String TABLE_TOPIC_ICEBERG_NAMESPACE = "table-topic.iceberg-namespace";

  /** Synthetic read-only property carrying the target Iceberg table name. */
  public static final String TABLE_TOPIC_ICEBERG_TABLE = "table-topic.iceberg-table";

  /** Synthetic read-only property carrying the qualified linked Iceberg table identifier. */
  public static final String TABLE_TOPIC_ICEBERG_IDENT = "table-topic.iceberg-ident";

  /** Set of synthetic read-only Table Topic linkage keys managed by Gravitino. */
  public static final Set<String> RESERVED_TABLE_TOPIC_LINKAGE_KEYS =
      ImmutableSet.of(
          TABLE_TOPIC_ENABLED,
          TABLE_TOPIC_ICEBERG_CATALOG,
          TABLE_TOPIC_ICEBERG_NAMESPACE,
          TABLE_TOPIC_ICEBERG_TABLE,
          TABLE_TOPIC_ICEBERG_IDENT);

  /** Supported AutoMQ Table Topic schema evolution modes. */
  public enum TableTopicSchemaType {
    /** Schemaless payload ingestion without external schema registry enforcement. */
    SCHEMALESS,
    /** Schema-driven ingestion derived from attached record schemas. */
    SCHEMA,
    /** Subject naming strategy based on the record name. */
    RECORD_NAME,
    /** Subject naming strategy based on the topic name. */
    TOPIC_NAME
  }

  /** Supported AutoMQ Table Topic deserialization error tolerance policies. */
  public enum TableTopicErrorsTolerance {
    /** Fail immediately on record conversion errors. */
    NONE,
    /** Tolerate and skip invalid records during ingestion. */
    ALL
  }

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.<PropertyEntry<?>>builder()
            .add(
                PropertyEntry.integerOptionalPropertyEntry(
                    PARTITION_COUNT,
                    "The number of partitions for the topic, if not specified, "
                        + "will use the num.partitions property in the broker",
                    false /* immutable */,
                    null /* default value */,
                    false /* hidden */),
                // TODO: make REPLICATION_FACTOR mutable if needed
                PropertyEntry.shortOptionalPropertyEntry(
                    REPLICATION_FACTOR,
                    "The number of replications for the topic, if not specified, "
                        + "will use the default.replication.factor property in the broker",
                    true /* immutable */,
                    null /* default value */,
                    false /* hidden */),
                strictBooleanPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_ENABLE,
                    "Whether to materialize this Kafka topic as an AutoMQ Table Topic in Iceberg"),
                positiveLongPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS,
                    "Commit interval in milliseconds for materializing AutoMQ Table Topic records"),
                nonBlankStringPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_NAMESPACE,
                    "Target Iceberg namespace (schema) for the materialized AutoMQ Table Topic"),
                PropertyEntry.enumPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_SCHEMA_TYPE,
                    "Schema mode for AutoMQ Table Topic conversion (schemaless, schema,"
                        + " record_name, or topic_name)",
                    false /* required */,
                    false /* immutable */,
                    TableTopicSchemaType.class,
                    null /* default value */,
                    false /* hidden */,
                    false /* reserved */),
                columnListPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_ID_COLUMNS,
                    "Comma-separated primary key column names for Iceberg equality deletes and"
                        + " upserts"),
                nonBlankStringPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_PARTITION_BY,
                    "Iceberg partition transform expressions for the materialized Table Topic"),
                strictBooleanPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE,
                    "Whether upsert mode is enabled for the materialized AutoMQ Table Topic"),
                nonBlankStringPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_CDC_FIELD,
                    "Field name carrying the CDC operation code for AutoMQ Table Topic"
                        + " materialization"),
                PropertyEntry.enumPropertyEntry(
                    AUTOMQ_TABLE_TOPIC_ERRORS_TOLERANCE,
                    "Error tolerance policy when converting records for an AutoMQ Table Topic"
                        + " (none or all)",
                    false /* required */,
                    false /* immutable */,
                    TableTopicErrorsTolerance.class,
                    null /* default value */,
                    false /* hidden */,
                    false /* reserved */),
                PropertyEntry.stringReservedPropertyEntry(
                    TABLE_TOPIC_ENABLED,
                    "Synthetic read-only flag indicating whether AutoMQ Table Topic"
                        + " materialization is enabled",
                    false /* hidden */),
                PropertyEntry.stringReservedPropertyEntry(
                    TABLE_TOPIC_ICEBERG_CATALOG,
                    "Synthetic read-only name of the linked Iceberg catalog configured on the"
                        + " Kafka catalog",
                    false /* hidden */),
                PropertyEntry.stringReservedPropertyEntry(
                    TABLE_TOPIC_ICEBERG_NAMESPACE,
                    "Synthetic read-only Iceberg namespace resolved for the Table Topic",
                    false /* hidden */),
                PropertyEntry.stringReservedPropertyEntry(
                    TABLE_TOPIC_ICEBERG_TABLE,
                    "Synthetic read-only Iceberg table name linked to the Table Topic",
                    false /* hidden */),
                PropertyEntry.stringReservedPropertyEntry(
                    TABLE_TOPIC_ICEBERG_IDENT,
                    "Synthetic read-only qualified Iceberg identifier linked to the Table Topic",
                    false /* hidden */))
            .build();

    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  /**
   * Validates AutoMQ Table Topic property values and cross-property constraints.
   *
   * @param properties topic properties to validate; null or empty maps are accepted as no-ops
   */
  public static void validateTableTopicProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (RESERVED_TABLE_TOPIC_LINKAGE_KEYS.contains(key)) {
        continue;
      }
      PropertyEntry<?> propertyEntry = PROPERTIES_METADATA.get(key);
      if (propertyEntry != null && key.startsWith("automq.table.topic.")) {
        propertyEntry.decode(entry.getValue());
      }
    }

    String tableTopicEnable = properties.get(AUTOMQ_TABLE_TOPIC_ENABLE);
    boolean tableTopicDisabled =
        tableTopicEnable != null && "false".equalsIgnoreCase(tableTopicEnable.trim());
    String upsertEnable = properties.get(AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE);
    if (!tableTopicDisabled
        && upsertEnable != null
        && "true".equalsIgnoreCase(upsertEnable.trim())) {
      String idColumns = properties.get(AUTOMQ_TABLE_TOPIC_ID_COLUMNS);
      if (isBlank(idColumns)) {
        throw new IllegalArgumentException(
            "Property "
                + AUTOMQ_TABLE_TOPIC_ID_COLUMNS
                + " is required when "
                + AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE
                + " is true");
      }
    }
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  private static PropertyEntry<Boolean> strictBooleanPropertyEntry(
      String name, String description) {
    return new PropertyEntry.Builder<Boolean>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(Boolean.class)
        .withDefaultValue(null)
        .withDecoder(
            value -> {
              if (value == null) {
                throw new IllegalArgumentException(
                    "Invalid boolean value for property " + name + ": null");
              }
              String normalized = value.trim().toLowerCase(Locale.ROOT);
              if ("true".equals(normalized)) {
                return Boolean.TRUE;
              }
              if ("false".equals(normalized)) {
                return Boolean.FALSE;
              }
              throw new IllegalArgumentException(
                  String.format(
                      "Invalid boolean value '%s' for property %s; expected 'true' or 'false'",
                      value, name));
            })
        .withEncoder(String::valueOf)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  private static PropertyEntry<Long> positiveLongPropertyEntry(String name, String description) {
    return new PropertyEntry.Builder<Long>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(Long.class)
        .withDefaultValue(null)
        .withDecoder(
            value -> {
              if (isBlank(value)) {
                throw new IllegalArgumentException(
                    "Invalid positive long value for property " + name + ": " + value);
              }
              try {
                long parsed = Long.parseLong(value.trim());
                if (parsed <= 0) {
                  throw new IllegalArgumentException(
                      String.format("Property %s must be greater than 0, got %d", name, parsed));
                }
                return parsed;
              } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    String.format("Invalid long value '%s' for property %s", value, name), e);
              }
            })
        .withEncoder(String::valueOf)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  private static PropertyEntry<String> nonBlankStringPropertyEntry(
      String name, String description) {
    return new PropertyEntry.Builder<String>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(String.class)
        .withDefaultValue(null)
        .withDecoder(
            value -> {
              if (isBlank(value)) {
                throw new IllegalArgumentException(
                    "Property " + name + " must not be null or blank");
              }
              return value.trim();
            })
        .withEncoder(v -> v)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  private static PropertyEntry<String> columnListPropertyEntry(String name, String description) {
    return new PropertyEntry.Builder<String>()
        .withName(name)
        .withDescription(description)
        .withRequired(false)
        .withImmutable(false)
        .withJavaType(String.class)
        .withDefaultValue(null)
        .withDecoder(
            value -> {
              if (isBlank(value)) {
                throw new IllegalArgumentException(
                    "Property " + name + " must not be null or blank");
              }
              String[] columns = value.split(",", -1);
              Set<String> seen = new HashSet<>();
              for (String column : columns) {
                String trimmed = column.trim();
                if (trimmed.isEmpty()) {
                  throw new IllegalArgumentException(
                      "Property " + name + " contains a blank column name: '" + value + "'");
                }
                if (!seen.add(trimmed)) {
                  throw new IllegalArgumentException(
                      "Property "
                          + name
                          + " contains duplicate column name '"
                          + trimmed
                          + "': '"
                          + value
                          + "'");
                }
              }
              return value.trim();
            })
        .withEncoder(v -> v)
        .withHidden(false)
        .withReserved(false)
        .build();
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
