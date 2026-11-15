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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKafkaTopicPropertiesMetadata {

  @Test
  public void testTableTopicPropertyDescriptors() {
    KafkaTopicPropertiesMetadata metadata = new KafkaTopicPropertiesMetadata();
    Map<String, PropertyEntry<?>> entries = metadata.propertyEntries();

    List<String> autoMqKeys =
        Arrays.asList(
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ENABLE,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_NAMESPACE,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_SCHEMA_TYPE,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_PARTITION_BY,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_CDC_FIELD,
            KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ERRORS_TOLERANCE);

    for (String key : autoMqKeys) {
      PropertyEntry<?> entry = entries.get(key);
      Assertions.assertNotNull(entry, "Missing property entry for " + key);
      Assertions.assertFalse(entry.isRequired(), key + " should be optional");
      Assertions.assertFalse(entry.isImmutable(), key + " should be mutable");
      Assertions.assertFalse(entry.isHidden(), key + " should be visible");
      Assertions.assertFalse(entry.isReserved(), key + " should not be reserved");
      Assertions.assertNull(entry.getDefaultValue(), key + " should default to null");
    }

    for (String reservedKey : KafkaTopicPropertiesMetadata.RESERVED_TABLE_TOPIC_LINKAGE_KEYS) {
      PropertyEntry<?> entry = entries.get(reservedKey);
      Assertions.assertNotNull(entry, "Missing reserved property entry for " + reservedKey);
      Assertions.assertTrue(entry.isReserved(), reservedKey + " should be reserved");
      Assertions.assertFalse(entry.isHidden(), reservedKey + " should not be hidden");
      Assertions.assertTrue(entry.isImmutable(), reservedKey + " should be immutable");
    }
  }

  @Test
  public void testValidateTableTopicPropertiesAcceptsValidValues() {
    KafkaTopicPropertiesMetadata.validateTableTopicProperties(null);
    KafkaTopicPropertiesMetadata.validateTableTopicProperties(Collections.emptyMap());

    Map<String, String> validProps =
        ImmutableMap.<String, String>builder()
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ENABLE, "TRUE")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS, "5000")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_NAMESPACE, "analytics")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_SCHEMA_TYPE, "record_name")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE, "true")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS, "id,tenant_id")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_PARTITION_BY, "day(event_ts)")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_CDC_FIELD, "_op")
            .put(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ERRORS_TOLERANCE, "all")
            .build();
    Assertions.assertDoesNotThrow(
        () -> KafkaTopicPropertiesMetadata.validateTableTopicProperties(validProps));
  }

  @Test
  public void testValidateTableTopicPropertiesRejectsInvalidValues() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ENABLE, "yes")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS, "0")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS, "-10")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_COMMIT_INTERVAL_MS, "abc")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_NAMESPACE, "   ")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_SCHEMA_TYPE, "invalid")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ERRORS_TOLERANCE, "skip")));

    IllegalArgumentException upsertError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                    ImmutableMap.of(
                        KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ENABLE,
                        "true",
                        KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE,
                        "true")));
    Assertions.assertTrue(
        upsertError
            .getMessage()
            .contains(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE,
                    "true",
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS,
                    "  ")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS, "id, ,ts")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS, ",")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS, "id,")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ID_COLUMNS, "id, id")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_PARTITION_BY, "   ")));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_CDC_FIELD, "")));

    // Disabling Table Topic (or leaving automq.table.topic.enable unset) allows omitting
    // id.columns even if upsert.enable is true
    Assertions.assertDoesNotThrow(
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_ENABLE,
                    "false",
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE,
                    "true")));
    Assertions.assertDoesNotThrow(
        () ->
            KafkaTopicPropertiesMetadata.validateTableTopicProperties(
                ImmutableMap.of(
                    KafkaTopicPropertiesMetadata.AUTOMQ_TABLE_TOPIC_UPSERT_ENABLE, "true")));
  }
}
