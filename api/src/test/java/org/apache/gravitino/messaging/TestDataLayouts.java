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

package org.apache.gravitino.messaging;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataLayouts {

  @Test
  public void testApplyChangesInOrder() {
    DataLayout initialLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.PROTOBUF)
            .withSchemaSubject("order-value-v1")
            .build();
    DataLayout removedLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.AVRO)
            .withSchemaSubject("order-value-v2")
            .build();
    DataLayout finalLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("order-value-v3")
            .build();

    Assertions.assertEquals(
        DataLayouts.ofValue(finalLayout),
        DataLayouts.applyChanges(
            DataLayouts.ofValue(initialLayout),
            TopicChange.updateDataLayout(DataLayouts.VALUE, removedLayout),
            TopicChange.removeDataLayout(DataLayouts.VALUE),
            TopicChange.updateDataLayout(DataLayouts.VALUE, finalLayout)));
  }

  @Test
  public void testOfValueAndOfKeyValue() {
    DataLayout keyLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("order-key")
            .build();
    DataLayout valueLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.PROTOBUF)
            .withSchemaSubject("order-value")
            .build();

    Assertions.assertEquals(
        ImmutableMap.of(DataLayouts.VALUE, valueLayout), DataLayouts.ofValue(valueLayout));
    Assertions.assertEquals(
        ImmutableMap.of(DataLayouts.KEY, keyLayout, DataLayouts.VALUE, valueLayout),
        DataLayouts.of(keyLayout, valueLayout));
    Assertions.assertEquals(
        ImmutableMap.of(DataLayouts.KEY, keyLayout), DataLayouts.of(keyLayout, null));
    Assertions.assertEquals(
        ImmutableMap.of(DataLayouts.VALUE, valueLayout), DataLayouts.of(null, valueLayout));
    Assertions.assertNull(DataLayouts.of(null, null));
  }

  @Test
  public void testValidateNoReservedProperties() {
    DataLayouts.validateNoReservedProperties(null);
    DataLayouts.validateNoReservedProperties(ImmutableMap.of("key", "value"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DataLayouts.validateNoReservedProperties(
                ImmutableMap.of(DataLayouts.ENTITY_STORAGE_KEY, "{}")));
  }

  @Test
  public void testCopyOrNull() {
    Assertions.assertNull(DataLayouts.copyOrNull(null));
    Assertions.assertNull(DataLayouts.copyOrNull(ImmutableMap.of()));

    DataLayout layout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.AVRO)
            .withSchemaSubject("orders")
            .build();
    Map<String, DataLayout> copy = DataLayouts.copyOrNull(ImmutableMap.of("value", layout));
    Assertions.assertEquals(ImmutableMap.of("value", layout), copy);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> copy.put("key", layout));

    Map<String, DataLayout> blankName = new HashMap<>();
    blankName.put("  ", layout);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DataLayouts.copyOrNull(blankName));

    Map<String, DataLayout> nullLayout = new HashMap<>();
    nullLayout.put("value", null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DataLayouts.copyOrNull(nullLayout));
  }

  @Test
  public void testMutableCopyIsNeverNullAndDetached() {
    Map<String, DataLayout> empty = DataLayouts.mutableCopy(null);
    Assertions.assertTrue(empty.isEmpty());

    DataLayout layout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("orders")
            .build();
    Map<String, DataLayout> source = new HashMap<>();
    source.put("value", layout);
    Map<String, DataLayout> copy = DataLayouts.mutableCopy(source);
    source.clear();

    Assertions.assertEquals(ImmutableMap.of("value", layout), copy);
    copy.put("key", layout); // must be mutable
  }

  @Test
  public void testToSchemaDataLayoutCopiesForeignImplementations() {
    Assertions.assertNull(DataLayouts.toSchemaDataLayout(null));

    SchemaDataLayout schemaLayout =
        SchemaDataLayout.builder().withFormat(DataLayout.Format.PROTOBUF).build();
    Assertions.assertSame(schemaLayout, DataLayouts.toSchemaDataLayout(schemaLayout));

    // A minimal custom implementation relying on interface defaults.
    DataLayout foreign =
        new DataLayout() {
          @Override
          public String schemaSubject() {
            return "orders";
          }
        };
    SchemaDataLayout converted = DataLayouts.toSchemaDataLayout(foreign);
    Assertions.assertEquals(DataLayout.Format.UNKNOWN, converted.format());
    Assertions.assertEquals("orders", converted.schemaSubject());
    Assertions.assertNull(converted.typeName());
    Assertions.assertNull(converted.schemaId());
    Assertions.assertNull(converted.schemaVersion());
    Assertions.assertNull(converted.schemaUri());
    Assertions.assertNull(converted.schemaText());
    Assertions.assertEquals(ImmutableMap.of(), converted.properties());
  }

  @Test
  public void testIsLayoutChange() {
    DataLayout layout = SchemaDataLayout.builder().withFormat(DataLayout.Format.AVRO).build();

    Assertions.assertTrue(
        DataLayouts.isLayoutChange(TopicChange.updateDataLayout(DataLayouts.VALUE, layout)));
    Assertions.assertTrue(
        DataLayouts.isLayoutChange(TopicChange.removeDataLayout(DataLayouts.VALUE)));
    Assertions.assertTrue(DataLayouts.isLayoutChange(TopicChange.removeDataLayouts()));
    Assertions.assertFalse(DataLayouts.isLayoutChange(TopicChange.updateComment("comment")));
    Assertions.assertFalse(DataLayouts.isLayoutChange(TopicChange.setProperty("k", "v")));
  }

  @Test
  public void testApplyChangesRemoveAllYieldsNull() {
    DataLayout layout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.PROTOBUF)
            .withSchemaSubject("order-value")
            .build();

    Assertions.assertNull(
        DataLayouts.applyChanges(DataLayouts.ofValue(layout), TopicChange.removeDataLayouts()));
    Assertions.assertNull(
        DataLayouts.applyChanges(
            DataLayouts.ofValue(layout), TopicChange.removeDataLayout(DataLayouts.VALUE)));
    Assertions.assertNull(DataLayouts.applyChanges(null));
  }
}
