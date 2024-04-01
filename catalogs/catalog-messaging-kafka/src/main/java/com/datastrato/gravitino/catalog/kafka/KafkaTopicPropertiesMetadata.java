/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class KafkaTopicPropertiesMetadata extends BasePropertiesMetadata {
  public static final String PARTITION_COUNT = "partition-count";
  public static final String REPLICATION_FACTOR = "replication-factor";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            PropertyEntry.integerOptionalPropertyEntry(
                PARTITION_COUNT,
                "The number of partitions for the topic, if not specified, "
                    + "will use the num.partition property in the broker",
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
                false /* hidden */));

    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
