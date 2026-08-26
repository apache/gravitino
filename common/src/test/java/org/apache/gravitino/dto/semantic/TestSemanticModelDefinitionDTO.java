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
package org.apache.gravitino.dto.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelDefinitionDTO {

  private final ObjectMapper objectMapper = JsonUtils.objectMapper();
  private final ObjectMapper persistenceMapper = JsonUtils.anyFieldMapper();

  @Test
  public void testDefinitionConversionAndJsonRoundTrip() throws JsonProcessingException {
    SemanticModelDefinition definition = definition();
    SemanticModelDefinitionDTO dto = SemanticModelDefinitionDTO.fromDefinition(definition);

    String json = persistenceMapper.writeValueAsString(dto);
    JsonNode root = persistenceMapper.readTree(json);
    JsonNode dataset = root.path("datasets").get(0);
    JsonNode field = dataset.path("fields").get(0);
    JsonNode relationship = root.path("relationships").get(0);

    assertTrue(root.has("ai_context"));
    assertTrue(root.has("custom_extensions"));
    assertFalse(root.has("aiContext"));
    assertFalse(json.contains("customExtensions"));
    assertEquals(List.of("sales", "mart"), stringValues(dataset.path("source").path("namespace")));
    assertEquals("orders", dataset.path("source").path("name").textValue());
    assertTrue(dataset.has("primary_key"));
    assertTrue(dataset.has("unique_keys"));
    assertTrue(dataset.has("ai_context"));
    assertEquals("DateTimeTz", field.path("datatype").textValue());
    assertTrue(field.path("dimension").path("is_time").booleanValue());
    assertEquals(
        "ANSI_SQL", field.path("expression").path("dialects").get(0).path("dialect").textValue());
    assertTrue(relationship.has("from_columns"));
    assertTrue(relationship.has("to_columns"));
    assertEquals("example", root.path("custom_extensions").get(0).path("vendor_name").textValue());

    JsonNode aiContext = root.path("ai_context");
    assertEquals(
        List.of(
            "instructions", "synonyms", "examples", "priority", "semantic_hints", "nullable_hint"),
        fieldNames(aiContext));
    assertEquals(List.of("first", "second"), fieldNames(aiContext.path("semantic_hints")));
    assertTrue(aiContext.path("nullable_hint").isNull());

    SemanticModelDefinitionDTO deserialized =
        persistenceMapper.readValue(json, SemanticModelDefinitionDTO.class);
    assertEquals(dto, deserialized);
    assertEquals(definition, deserialized.toDefinition());
    assertEquals(json, persistenceMapper.writeValueAsString(deserialized));
  }

  @Test
  public void testAIContextUnionAndUnknownPropertyOrder() throws JsonProcessingException {
    AIContextDTO textContext = AIContextDTO.fromAIContext(AIContext.of(""));
    String textJson = objectMapper.writeValueAsString(textContext);
    assertEquals("\"\"", textJson);
    assertEquals(
        AIContext.of(""), objectMapper.readValue(textJson, AIContextDTO.class).toAIContext());

    String objectJson =
        "{\"instructions\":\"Use governed data\","
            + "\"first_unknown\":{\"alpha\":1,\"beta\":2},"
            + "\"synonyms\":[\"governed\"],"
            + "\"second_unknown\":[true,null],"
            + "\"precise\":0.123456789012345678901234567890}";
    AIContextDTO objectContext = objectMapper.readValue(objectJson, AIContextDTO.class);

    assertNull(objectContext.getText());
    assertEquals(
        List.of("first_unknown", "second_unknown", "precise"),
        new ArrayList<>(objectContext.getObject().getAdditionalProperties().keySet()));
    assertEquals(
        new BigDecimal("0.123456789012345678901234567890"),
        objectContext.getObject().getAdditionalProperties().get("precise"));
    assertEquals(
        List.of("alpha", "beta"),
        new ArrayList<>(
            ((Map<?, ?>) objectContext.getObject().getAdditionalProperties().get("first_unknown"))
                .keySet()));
    assertEquals(
        BigInteger.ONE,
        ((Map<?, ?>) objectContext.getObject().getAdditionalProperties().get("first_unknown"))
            .get("alpha"));

    AIContextDTO converted = AIContextDTO.fromAIContext(objectContext.toAIContext());
    assertEquals(
        List.of("first_unknown", "second_unknown", "precise"),
        new ArrayList<>(converted.getObject().getAdditionalProperties().keySet()));
    assertEquals(
        List.of("instructions", "synonyms", "first_unknown", "second_unknown", "precise"),
        fieldNames(objectMapper.readTree(objectMapper.writeValueAsString(converted))));
    assertEquals(
        objectContext.getObject().getAdditionalProperties(),
        converted.getObject().getAdditionalProperties());
    assertTrue(
        objectMapper.writeValueAsString(converted).contains("0.123456789012345678901234567890"));

    assertThrows(
        JsonProcessingException.class, () -> objectMapper.readValue("42", AIContextDTO.class));
  }

  @Test
  public void testAbsentAndEmptyArraysRemainDistinct() throws JsonProcessingException {
    Dataset absentDataset =
        Dataset.builder()
            .withName("absent")
            .withSource(NameIdentifier.of("sales", "mart", "absent"))
            .build();
    SemanticModelDefinition absentDefinition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {absentDataset}).build();
    SemanticModelDefinitionDTO absent = SemanticModelDefinitionDTO.fromDefinition(absentDefinition);
    String absentJson = objectMapper.writeValueAsString(absent);

    assertFalse(absentJson.contains("fields"));
    assertFalse(absentJson.contains("relationships"));
    assertEquals(absentDefinition, absent.toDefinition());

    Dataset emptyDataset =
        Dataset.builder()
            .withName("empty")
            .withSource(NameIdentifier.of("sales", "mart", "empty"))
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withFields(new Field[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinition emptyDefinition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {emptyDataset})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinitionDTO empty = SemanticModelDefinitionDTO.fromDefinition(emptyDefinition);
    JsonNode emptyJson = objectMapper.readTree(objectMapper.writeValueAsString(empty));

    assertArrayEquals(new Relationship[0], empty.toDefinition().relationships());
    assertTrue(emptyJson.path("relationships").isEmpty());
    assertTrue(emptyJson.path("datasets").get(0).path("fields").isEmpty());
    assertEquals(emptyDefinition, empty.toDefinition());
  }

  @Test
  public void testExactDataTypeJsonValuesAndOpenDialect() throws JsonProcessingException {
    Map<DataType, String> dataTypeValues = new LinkedHashMap<>();
    dataTypeValues.put(DataType.STRING, "String");
    dataTypeValues.put(DataType.INTEGER, "Integer");
    dataTypeValues.put(DataType.DECIMAL, "Decimal");
    dataTypeValues.put(DataType.FLOAT, "Float");
    dataTypeValues.put(DataType.BOOLEAN, "Boolean");
    dataTypeValues.put(DataType.DATE, "Date");
    dataTypeValues.put(DataType.TIME, "Time");
    dataTypeValues.put(DataType.DATE_TIME, "DateTime");
    dataTypeValues.put(DataType.DATE_TIME_TZ, "DateTimeTz");
    dataTypeValues.put(DataType.OPAQUE, "Opaque");

    for (Map.Entry<DataType, String> entry : dataTypeValues.entrySet()) {
      MetricDTO metric =
          MetricDTO.builder()
              .withName("metric")
              .withExpression(ExpressionDTO.fromExpression(expression("value")))
              .withDatatype(entry.getKey())
              .build();
      String json = objectMapper.writeValueAsString(metric);
      assertEquals(entry.getValue(), objectMapper.readTree(json).path("datatype").textValue());
      assertEquals(entry.getKey(), objectMapper.readValue(json, MetricDTO.class).getDatatype());
    }

    DialectExpressionDTO dialectExpression =
        DialectExpressionDTO.builder().withDialect("TRINO").withExpression("value").build();
    String json = objectMapper.writeValueAsString(dialectExpression);
    assertEquals("TRINO", objectMapper.readTree(json).path("dialect").textValue());
    assertEquals("TRINO", objectMapper.readValue(json, DialectExpressionDTO.class).getDialect());
  }

  @Test
  public void testUnknownPropertiesAreOnlyAllowedInAIContext() {
    String dataset =
        "{\"name\":\"orders\","
            + "\"source\":{\"namespace\":[\"sales\",\"mart\"],\"name\":\"orders\"},"
            + "\"unknown\":true}";

    assertThrows(
        JsonProcessingException.class, () -> objectMapper.readValue(dataset, DatasetDTO.class));
  }

  private static SemanticModelDefinition definition() {
    Map<String, Object> semanticHints = new LinkedHashMap<>();
    semanticHints.put("first", "prefer certified metrics");
    semanticHints.put("second", List.of("month", "region"));
    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    additionalProperties.put("priority", 1);
    additionalProperties.put("semantic_hints", semanticHints);
    additionalProperties.put("nullable_hint", null);

    AIContextObject aiContextObject =
        AIContextObject.builder()
            .withInstructions("Use governed data")
            .withSynonyms(new String[] {"sales", "revenue"})
            .withExamples(new String[] {"Revenue by month"})
            .withAdditionalProperties(additionalProperties)
            .build();
    CustomExtension extension =
        CustomExtension.builder()
            .withVendorName("example")
            .withData("{\"semantic_type\":\"money\"}")
            .build();
    Field orderTime =
        Field.builder()
            .withName("order_time")
            .withExpression(expression("order_time"))
            .withDimension(Dimension.builder().withIsTime(true).build())
            .withLabel("Order time")
            .withDescription("Order creation time")
            .withDatatype(DataType.DATE_TIME_TZ)
            .withAIContext(AIContext.of("Use the business timezone"))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Field amount =
        Field.builder()
            .withName("amount")
            .withExpression(expression("order_amount"))
            .withDimension(Dimension.builder().withIsTime(false).build())
            .withDatatype(DataType.DECIMAL)
            .build();
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .withUniqueKeys(new String[][] {{"order_id"}, {"external_id", "source_system"}})
            .withDescription("Governed orders")
            .withAIContext(AIContext.of(aiContextObject))
            .withFields(new Field[] {orderTime, amount})
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("sales", "mart", "customers"))
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"id"})
            .withAIContext(AIContext.of("Join orders to the canonical customer"))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();
    Metric revenue =
        Metric.builder()
            .withName("revenue")
            .withExpression(expression("SUM(order_amount)"))
            .withDescription("Total recognized revenue")
            .withDatatype(DataType.DECIMAL)
            .withAIContext(AIContext.of("Use for booked revenue"))
            .withCustomExtensions(new CustomExtension[] {extension})
            .build();

    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of(aiContextObject))
        .withDatasets(new Dataset[] {orders, customers})
        .withRelationships(new Relationship[] {relationship})
        .withMetrics(new Metric[] {revenue})
        .withCustomExtensions(new CustomExtension[] {extension})
        .build();
  }

  private static Expression expression(String value) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(value)
                  .build(),
              DialectExpression.builder()
                  .withDialect(Dialects.BIGQUERY)
                  .withExpression(value)
                  .build()
            })
        .build();
  }

  private static List<String> fieldNames(JsonNode object) {
    List<String> names = new ArrayList<>();
    Iterator<String> fields = object.fieldNames();
    fields.forEachRemaining(names::add);
    return names;
  }

  private static List<String> stringValues(JsonNode array) {
    List<String> values = new ArrayList<>();
    array.forEach(value -> values.add(value.textValue()));
    return values;
  }
}
