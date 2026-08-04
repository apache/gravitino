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

public class TestSchemaDataLayout {

  @Test
  void testBuilderAndEquals() {
    SchemaDataLayout layout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.PROTOBUF)
            .withTypeName("com.example.Order")
            .withSchemaId("1")
            .withSchemaVersion("2")
            .withSchemaUri("http://localhost:8081")
            .withSchemaSubject("order-value")
            .withSchemaText("syntax = \"proto3\";")
            .withProperties(ImmutableMap.of("k", "v"))
            .build();

    Assertions.assertEquals(DataLayout.Format.PROTOBUF, layout.format());
    Assertions.assertEquals("com.example.Order", layout.typeName());
    Assertions.assertEquals("1", layout.schemaId());
    Assertions.assertEquals("2", layout.schemaVersion());
    Assertions.assertEquals("http://localhost:8081", layout.schemaUri());
    Assertions.assertEquals("order-value", layout.schemaSubject());
    Assertions.assertEquals("syntax = \"proto3\";", layout.schemaText());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), layout.properties());

    SchemaDataLayout copy = DataLayouts.toSchemaDataLayout(layout);
    Assertions.assertSame(layout, copy);

    SchemaDataLayout fromInterface =
        DataLayouts.toSchemaDataLayout(
            new DataLayout() {
              @Override
              public Format format() {
                return Format.AVRO;
              }

              @Override
              public String schemaSubject() {
                return "s";
              }
            });
    Assertions.assertEquals(DataLayout.Format.AVRO, fromInterface.format());
    Assertions.assertEquals("s", fromInterface.schemaSubject());
  }

  @Test
  void testBuilderDefaults() {
    SchemaDataLayout layout = SchemaDataLayout.builder().build();

    Assertions.assertEquals(DataLayout.Format.UNKNOWN, layout.format());
    Assertions.assertNull(layout.typeName());
    Assertions.assertNull(layout.schemaId());
    Assertions.assertNull(layout.schemaVersion());
    Assertions.assertNull(layout.schemaUri());
    Assertions.assertNull(layout.schemaSubject());
    Assertions.assertNull(layout.schemaText());
    Assertions.assertEquals(ImmutableMap.of(), layout.properties());

    SchemaDataLayout nullFormat = SchemaDataLayout.builder().withFormat(null).build();
    Assertions.assertEquals(DataLayout.Format.UNKNOWN, nullFormat.format());
  }

  @Test
  void testPropertiesAreDefensivelyCopiedAndImmutable() {
    Map<String, String> properties = new HashMap<>();
    properties.put("compatibility", "BACKWARD");

    SchemaDataLayout layout = SchemaDataLayout.builder().withProperties(properties).build();
    properties.put("mutated", "true");

    Assertions.assertEquals(ImmutableMap.of("compatibility", "BACKWARD"), layout.properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> layout.properties().put("k", "v"));
  }

  @Test
  void testToStringRedactsSchemaText() {
    SchemaDataLayout layout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.AVRO)
            .withSchemaText("{\"type\":\"record\",\"name\":\"SecretPayload\",\"fields\":[]}")
            .build();

    String toString = layout.toString();
    Assertions.assertFalse(toString.contains("SecretPayload"), toString);
    Assertions.assertTrue(toString.contains("chars>"), toString);
  }

  @Test
  void testHashCodeConsistentWithEquals() {
    SchemaDataLayout layout1 =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("orders")
            .build();
    SchemaDataLayout layout2 =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("orders")
            .build();
    SchemaDataLayout different =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("other")
            .build();

    Assertions.assertEquals(layout1, layout2);
    Assertions.assertEquals(layout1.hashCode(), layout2.hashCode());
    Assertions.assertNotEquals(layout1, different);
    Assertions.assertNotEquals(layout1, null);
    Assertions.assertNotEquals(layout1, "not a layout");
  }
}
