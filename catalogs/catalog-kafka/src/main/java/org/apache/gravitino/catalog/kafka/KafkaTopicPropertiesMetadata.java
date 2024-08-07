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
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

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
