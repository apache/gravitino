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
package org.apache.gravitino.dto.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.requests.TopicUpdateRequest;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.DataLayouts;
import org.apache.gravitino.messaging.TopicChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataLayoutDTO {

  private static final ObjectMapper MAPPER = JsonUtils.anyFieldMapper();

  @Test
  public void testJsonRoundTrip() throws Exception {
    DataLayoutDTO layout =
        DataLayoutDTO.builder()
            .format(DataLayout.Format.PROTOBUF)
            .typeName("com.example.Order")
            .schemaId("42")
            .schemaVersion("7")
            .schemaUri("http://localhost:8081")
            .schemaSubject("order-value")
            .schemaText("syntax = \"proto3\";")
            .properties(ImmutableMap.of("compatibility", "BACKWARD"))
            .build();

    String json = MAPPER.writeValueAsString(layout);
    DataLayoutDTO roundTripped = MAPPER.readValue(json, DataLayoutDTO.class);

    Assertions.assertEquals(layout, roundTripped);
  }

  @Test
  public void testNullAndMissingFormatDefaultsToUnknown() throws Exception {
    DataLayoutDTO missingFormat = MAPPER.readValue("{\"schemaId\":\"1\"}", DataLayoutDTO.class);
    DataLayoutDTO nullFormat =
        MAPPER.readValue("{\"format\":null,\"schemaId\":\"1\"}", DataLayoutDTO.class);

    Assertions.assertEquals(DataLayout.Format.UNKNOWN, missingFormat.format());
    Assertions.assertEquals(DataLayout.Format.UNKNOWN, nullFormat.format());
  }

  @Test
  public void testPropertiesAreDefensivelyCopied() {
    Map<String, String> properties = new HashMap<>();
    properties.put("compatibility", "BACKWARD");

    DataLayoutDTO layout =
        DataLayoutDTO.builder()
            .format(DataLayout.Format.AVRO)
            .schemaSubject("orders")
            .properties(properties)
            .build();
    properties.put("compatibility", "FORWARD");
    properties.put("mutated", "true");

    Assertions.assertEquals(ImmutableMap.of("compatibility", "BACKWARD"), layout.properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> layout.properties().put("new", "value"));
  }

  @Test
  public void testTopicCreateRequestJsonDeserializeWithDataLayouts() throws Exception {
    String json =
        "{"
            + "\"name\":\"orders\","
            + "\"comment\":\"order topic\","
            + "\"properties\":{\"partition-count\":\"3\"},"
            + "\"dataLayouts\":{"
            + "\"key\":{\"format\":\"JSON\",\"schemaSubject\":\"order-key\"},"
            + "\"value\":{\"format\":\"PROTOBUF\",\"schemaSubject\":\"order-value\"}"
            + "}"
            + "}";

    TopicCreateRequest request = MAPPER.readValue(json, TopicCreateRequest.class);

    Assertions.assertEquals("orders", request.getName());
    Assertions.assertEquals("order topic", request.getComment());
    Assertions.assertEquals(ImmutableMap.of("partition-count", "3"), request.getProperties());
    Assertions.assertEquals(DataLayout.Format.JSON, request.getDataLayouts().get("key").format());
    Assertions.assertEquals(
        DataLayout.Format.PROTOBUF, request.getDataLayouts().get("value").format());
  }

  @Test
  public void testTopicUpdateRequestJsonDeserializeWithTypeDiscrimination() throws Exception {
    TopicUpdateRequest update =
        MAPPER.readValue(
            "{"
                + "\"@type\":\"updateDataLayout\","
                + "\"name\":\"value\","
                + "\"newDataLayout\":{\"format\":\"AVRO\",\"schemaSubject\":\"order-value\"}"
                + "}",
            TopicUpdateRequest.class);
    Assertions.assertInstanceOf(TopicUpdateRequest.UpdateTopicDataLayoutRequest.class, update);
    TopicChange.UpdateDataLayout updateChange = (TopicChange.UpdateDataLayout) update.topicChange();
    Assertions.assertEquals(DataLayouts.VALUE, updateChange.getName());
    Assertions.assertEquals(DataLayout.Format.AVRO, updateChange.getNewDataLayout().format());

    TopicUpdateRequest remove =
        MAPPER.readValue(
            "{\"@type\":\"removeDataLayout\",\"name\":\"key\"}", TopicUpdateRequest.class);
    Assertions.assertInstanceOf(TopicUpdateRequest.RemoveTopicDataLayoutRequest.class, remove);
    Assertions.assertEquals(TopicChange.removeDataLayout(DataLayouts.KEY), remove.topicChange());

    TopicUpdateRequest removeAll =
        MAPPER.readValue("{\"@type\":\"removeDataLayouts\"}", TopicUpdateRequest.class);
    Assertions.assertInstanceOf(TopicUpdateRequest.RemoveTopicDataLayoutsRequest.class, removeAll);
    Assertions.assertEquals(TopicChange.removeDataLayouts(), removeAll.topicChange());
  }

  @Test
  public void testTopicCreateRequestRejectsReservedProperties() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TopicCreateRequest.builder()
                .name("orders")
                .properties(ImmutableMap.of(DataLayouts.ENTITY_STORAGE_KEY, "{}"))
                .build()
                .validate());
  }

  @Test
  public void testSetTopicPropertyRequestRejectsReservedProperties() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TopicUpdateRequest.SetTopicPropertyRequest(DataLayouts.ENTITY_STORAGE_KEY, "{}")
                .validate());
  }

  @Test
  public void testValidateRejectsEmptyLayout() {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> DataLayoutDTO.builder().build().validate());
    Assertions.assertTrue(e.getMessage().contains("at least one schema field"), e.getMessage());
  }

  @Test
  public void testValidateRejectsBlankFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DataLayoutDTO.builder()
                .format(DataLayout.Format.AVRO)
                .schemaSubject("  ")
                .build()
                .validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DataLayoutDTO.builder()
                .format(DataLayout.Format.PROTOBUF)
                .typeName("")
                .build()
                .validate());
  }

  @Test
  public void testValidateRejectsMalformedSchemaUri() {
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> DataLayoutDTO.builder().schemaUri("http://<invalid>").build().validate());
    Assertions.assertTrue(e.getMessage().contains("schemaUri"), e.getMessage());
  }

  @Test
  public void testValidateRejectsNonJsonSchemaTextForJsonFormats() {
    for (DataLayout.Format format :
        new DataLayout.Format[] {DataLayout.Format.AVRO, DataLayout.Format.JSON}) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  DataLayoutDTO.builder()
                      .format(format)
                      .schemaText("syntax = \"proto3\";")
                      .build()
                      .validate());
      Assertions.assertTrue(e.getMessage().contains("valid JSON"), e.getMessage());
    }
  }

  @Test
  public void testValidateAcceptsJsonSchemaTextForJsonFormats() {
    DataLayoutDTO.builder()
        .format(DataLayout.Format.AVRO)
        .schemaText("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}")
        .build()
        .validate();
    DataLayoutDTO.builder()
        .format(DataLayout.Format.JSON)
        .schemaText("{\"type\":\"object\"}")
        .build()
        .validate();
  }

  @Test
  public void testValidateAcceptsProtoSchemaTextForProtobuf() {
    DataLayoutDTO.builder()
        .format(DataLayout.Format.PROTOBUF)
        .typeName("com.example.Order")
        .schemaText("syntax = \"proto3\"; message Order { string id = 1; }")
        .build()
        .validate();
  }

  @Test
  public void testValidateAcceptsRegistryReference() {
    DataLayoutDTO.builder()
        .format(DataLayout.Format.PROTOBUF)
        .schemaUri("http://localhost:8081")
        .schemaSubject("order-value")
        .schemaVersion("3")
        .build()
        .validate();
  }

  @Test
  public void testFormatParsingIsCaseInsensitive() throws Exception {
    DataLayoutDTO lower = MAPPER.readValue("{\"format\":\"protobuf\"}", DataLayoutDTO.class);
    DataLayoutDTO mixed = MAPPER.readValue("{\"format\":\"Avro\"}", DataLayoutDTO.class);

    Assertions.assertEquals(DataLayout.Format.PROTOBUF, lower.format());
    Assertions.assertEquals(DataLayout.Format.AVRO, mixed.format());
  }

  @Test
  public void testTopicCreateRequestRejectsInvalidNestedLayouts() throws Exception {
    TopicCreateRequest blankName =
        MAPPER.readValue(
            "{\"name\":\"orders\",\"dataLayouts\":{\" \":{\"format\":\"AVRO\"}}}",
            TopicCreateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, blankName::validate);

    TopicCreateRequest nullLayout =
        MAPPER.readValue(
            "{\"name\":\"orders\",\"dataLayouts\":{\"value\":null}}", TopicCreateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, nullLayout::validate);

    TopicCreateRequest emptyLayout =
        MAPPER.readValue(
            "{\"name\":\"orders\",\"dataLayouts\":{\"value\":{}}}", TopicCreateRequest.class);
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, emptyLayout::validate);
    Assertions.assertTrue(e.getMessage().contains("\"value\""), e.getMessage());
  }

  @Test
  public void testUpdateDataLayoutRequestValidatesNestedLayout() {
    TopicUpdateRequest.UpdateTopicDataLayoutRequest request =
        new TopicUpdateRequest.UpdateTopicDataLayoutRequest(
            DataLayouts.VALUE, DataLayoutDTO.builder().build());
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertTrue(e.getMessage().contains(DataLayouts.VALUE), e.getMessage());

    new TopicUpdateRequest.UpdateTopicDataLayoutRequest(
            DataLayouts.VALUE,
            DataLayoutDTO.builder()
                .format(DataLayout.Format.PROTOBUF)
                .schemaSubject("order-value")
                .build())
        .validate();
  }

  @Test
  public void testFromDataLayoutNormalizesEmptyPropertiesToNull() {
    DataLayoutDTO layout =
        DataLayoutDTO.fromDataLayout(
            DataLayoutDTO.builder().format(DataLayout.Format.AVRO).schemaSubject("orders").build());

    Assertions.assertNull(layout.getProperties());
    Assertions.assertEquals(ImmutableMap.of(), layout.properties());
  }
}
