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
package org.apache.gravitino.catalog.clickhouse;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestClickHouseTablePropertiesMetadata {

  private ClickHouseTablePropertiesMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new ClickHouseTablePropertiesMetadata();
  }

  @Test
  void testTransformToJdbcProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(StringIdentifier.ID_KEY, "gravitino-id-123");
    properties.put(TableConstants.PARTITION_KEY, "cityHash64(x) % 7");
    properties.put(TableConstants.ENGINE, "MergeTree");
    properties.put(TableConstants.SETTINGS_PREFIX + "index_granularity", "8192");

    Map<String, String> jdbcProperties = metadata.transformToJdbcProperties(properties);

    // gravitino.identifier and partition-key must not be written to ClickHouse.
    Assertions.assertFalse(jdbcProperties.containsKey(StringIdentifier.ID_KEY));
    Assertions.assertFalse(jdbcProperties.containsKey(TableConstants.PARTITION_KEY));
    // engine is renamed to the ClickHouse-specific key.
    Assertions.assertEquals("MergeTree", jdbcProperties.get(TableConstants.ENGINE_UPPER));
    Assertions.assertFalse(jdbcProperties.containsKey(TableConstants.ENGINE));
    // Other properties pass through unchanged.
    Assertions.assertEquals(
        "8192", jdbcProperties.get(TableConstants.SETTINGS_PREFIX + "index_granularity"));
  }

  @Test
  void testConvertFromJdbcProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableConstants.ENGINE_UPPER, "MergeTree");
    properties.put(TableConstants.PARTITION_KEY, "cityHash64(x) % 7");

    Map<String, String> gravitinoProperties = metadata.convertFromJdbcProperties(properties);

    // engine is restored to the Gravitino key, and partition-key is preserved.
    Assertions.assertEquals("MergeTree", gravitinoProperties.get(TableConstants.ENGINE));
    Assertions.assertEquals(
        "cityHash64(x) % 7", gravitinoProperties.get(TableConstants.PARTITION_KEY));
  }

  @Test
  void testPartitionKeyPropertyEntry() {
    PropertyEntry<?> entry = ClickHouseTablePropertiesMetadata.PARTITION_KEY_PROPERTY_ENTRY;
    Assertions.assertEquals(TableConstants.PARTITION_KEY, entry.getName());
    Assertions.assertTrue(entry.isReserved());
    Assertions.assertTrue(entry.isImmutable());
    Assertions.assertFalse(entry.isHidden());
    Assertions.assertEquals("", entry.getDefaultValue());
  }
}
