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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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
    JsonNode metric = root.path("metrics").get(0);

    assertTrue(root.has("aiContext"));
    assertTrue(root.has("customExtensions"));
    assertFalse(root.has("ai_context"));
    assertFalse(root.has("custom_extensions"));
    assertEquals(List.of("sales", "mart"), stringValues(dataset.path("source").path("namespace")));
    assertEquals("orders", dataset.path("source").path("name").textValue());
    assertTrue(dataset.has("primaryKey"));
    assertTrue(dataset.has("uniqueKeys"));
    assertTrue(dataset.has("aiContext"));
    assertTrue(dataset.has("customExtensions"));
    assertFalse(dataset.has("primary_key"));
    assertFalse(dataset.has("unique_keys"));
    assertFalse(dataset.has("ai_context"));
    assertFalse(dataset.has("custom_extensions"));
    assertEquals("DateTimeTz", field.path("datatype").textValue());
    assertTrue(field.path("dimension").path("isTime").booleanValue());
    assertFalse(field.path("dimension").has("is_time"));
    assertTrue(field.has("customExtensions"));
    assertFalse(field.has("custom_extensions"));
    assertEquals(
        "ANSI_SQL", field.path("expression").path("dialects").get(0).path("dialect").textValue());
    assertTrue(relationship.has("fromColumns"));
    assertTrue(relationship.has("toColumns"));
    assertTrue(relationship.has("customExtensions"));
    assertFalse(relationship.has("from_columns"));
    assertFalse(relationship.has("to_columns"));
    assertFalse(relationship.has("custom_extensions"));
    assertTrue(metric.has("customExtensions"));
    assertFalse(metric.has("custom_extensions"));
    assertEquals("example", root.path("customExtensions").get(0).path("vendorName").textValue());

    JsonNode aiContext = root.path("aiContext");
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
    String convertedJson = objectMapper.writeValueAsString(converted);
    AIContextDTO roundTripped = objectMapper.readValue(convertedJson, AIContextDTO.class);
    assertEquals(
        List.of("first_unknown", "second_unknown", "precise"),
        new ArrayList<>(converted.getObject().getAdditionalProperties().keySet()));
    assertEquals(
        List.of("instructions", "synonyms", "first_unknown", "second_unknown", "precise"),
        fieldNames(objectMapper.readTree(convertedJson)));
    assertEquals(
        objectContext.getObject().getAdditionalProperties(),
        converted.getObject().getAdditionalProperties());
    assertEquals(
        new BigDecimal("0.123456789012345678901234567890"),
        roundTripped.getObject().getAdditionalProperties().get("precise"));

    assertThrows(
        JsonProcessingException.class, () -> objectMapper.readValue("42", AIContextDTO.class));
  }

  @Test
  public void testAIContextObjectExplicitNullHandling() throws JsonProcessingException {
    AIContextDTO explicitNull =
        objectMapper.readValue(
            "{\"instructions\":null,\"synonyms\":null,\"examples\":null,\"unknown\":null}",
            AIContextDTO.class);

    assertNull(explicitNull.getObject().getInstructions());
    assertNull(explicitNull.getObject().getSynonyms());
    assertNull(explicitNull.getObject().getExamples());
    assertTrue(explicitNull.getObject().getAdditionalProperties().containsKey("unknown"));
    assertNull(explicitNull.getObject().getAdditionalProperties().get("unknown"));

    JsonNode serialized = objectMapper.readTree(objectMapper.writeValueAsString(explicitNull));
    assertFalse(serialized.has("instructions"));
    assertFalse(serialized.has("synonyms"));
    assertFalse(serialized.has("examples"));
    assertTrue(serialized.path("unknown").isNull());

    AIContextDTO emptyArrays =
        objectMapper.readValue("{\"synonyms\":[],\"examples\":[]}", AIContextDTO.class);
    assertArrayEquals(new String[0], emptyArrays.getObject().getSynonyms());
    assertArrayEquals(new String[0], emptyArrays.getObject().getExamples());

    JsonMappingException nullElement =
        assertThrows(
            JsonMappingException.class,
            () -> objectMapper.readValue("{\"synonyms\":[null]}", AIContextDTO.class));
    assertEquals("synonyms must be a string", nullElement.getOriginalMessage());

    JsonMappingException invalidInstructions =
        assertThrows(
            JsonMappingException.class,
            () -> objectMapper.readValue("{\"instructions\":42}", AIContextDTO.class));
    assertEquals("instructions must be a string", invalidInstructions.getOriginalMessage());
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
    JsonNode absentJson = objectMapper.readTree(objectMapper.writeValueAsString(absent));
    JsonNode absentDatasetJson = absentJson.path("datasets").get(0);

    assertFalse(absentDatasetJson.has("fields"));
    assertFalse(absentJson.has("relationships"));
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

  @Test
  public void testAIContextObjectDTOIsDeeplyImmutable() {
    String[] synonyms = {"sales"};
    String[] examples = {"Revenue by month"};
    List<Object> hints = new ArrayList<>();
    hints.add("certified");
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("hints", hints);
    Map<String, Object> additionalProperties = new LinkedHashMap<>();
    additionalProperties.put("semantic", nested);

    AIContextObjectDTO dto =
        AIContextObjectDTO.builder()
            .withSynonyms(synonyms)
            .withExamples(examples)
            .withAdditionalProperties(additionalProperties)
            .build();
    int originalHashCode = dto.hashCode();

    synonyms[0] = "changed";
    examples[0] = "changed";
    hints.add("changed");
    nested.put("changed", true);
    additionalProperties.put("changed", true);

    assertArrayEquals(new String[] {"sales"}, dto.getSynonyms());
    assertArrayEquals(new String[] {"Revenue by month"}, dto.getExamples());
    Map<?, ?> immutableNested = (Map<?, ?>) dto.getAdditionalProperties().get("semantic");
    List<?> immutableHints = (List<?>) immutableNested.get("hints");
    assertEquals(List.of("certified"), immutableHints);
    assertEquals(originalHashCode, dto.hashCode());

    dto.getSynonyms()[0] = "changed";
    dto.getExamples()[0] = "changed";
    assertArrayEquals(new String[] {"sales"}, dto.getSynonyms());
    assertArrayEquals(new String[] {"Revenue by month"}, dto.getExamples());
    assertThrows(UnsupportedOperationException.class, () -> dto.getAdditionalProperties().clear());
    assertThrows(UnsupportedOperationException.class, immutableNested::clear);
    assertThrows(UnsupportedOperationException.class, immutableHints::clear);
    assertEquals(originalHashCode, dto.hashCode());
  }

  @Test
  public void testSemanticDTOArraysAreDefensivelyCopied() {
    DialectExpressionDTO dialect =
        DialectExpressionDTO.builder().withDialect("ANSI_SQL").withExpression("value").build();
    DialectExpressionDTO[] dialects = {dialect};
    ExpressionDTO expression = ExpressionDTO.builder().withDialects(dialects).build();

    CustomExtensionDTO extension =
        CustomExtensionDTO.builder().withVendorName("example").withData("{}").build();
    CustomExtensionDTO[] fieldExtensions = {extension};
    FieldDTO field =
        FieldDTO.builder()
            .withName("id")
            .withExpression(expression)
            .withCustomExtensions(fieldExtensions)
            .build();

    CustomExtensionDTO[] metricExtensions = {extension};
    MetricDTO metric =
        MetricDTO.builder()
            .withName("count")
            .withExpression(expression)
            .withCustomExtensions(metricExtensions)
            .build();

    String[] fromColumns = {"customer_id"};
    String[] toColumns = {"id"};
    CustomExtensionDTO[] relationshipExtensions = {extension};
    RelationshipDTO relationship =
        RelationshipDTO.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(fromColumns)
            .withToColumns(toColumns)
            .withCustomExtensions(relationshipExtensions)
            .build();

    String[] primaryKey = {"id"};
    String[][] uniqueKeys = {{"external_id", "source"}};
    FieldDTO[] fields = {field};
    CustomExtensionDTO[] datasetExtensions = {extension};
    DatasetDTO dataset =
        DatasetDTO.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "orders"))
            .withPrimaryKey(primaryKey)
            .withUniqueKeys(uniqueKeys)
            .withFields(fields)
            .withCustomExtensions(datasetExtensions)
            .build();

    DatasetDTO[] datasets = {dataset};
    RelationshipDTO[] relationships = {relationship};
    MetricDTO[] metrics = {metric};
    CustomExtensionDTO[] definitionExtensions = {extension};
    SemanticModelDefinitionDTO definition =
        SemanticModelDefinitionDTO.builder()
            .withDatasets(datasets)
            .withRelationships(relationships)
            .withMetrics(metrics)
            .withCustomExtensions(definitionExtensions)
            .build();

    assertDefensiveCopy(dialects, expression::getDialects);
    assertDefensiveCopy(fieldExtensions, field::getCustomExtensions);
    assertDefensiveCopy(metricExtensions, metric::getCustomExtensions);
    assertDefensiveCopy(fromColumns, relationship::getFromColumns);
    assertDefensiveCopy(toColumns, relationship::getToColumns);
    assertDefensiveCopy(relationshipExtensions, relationship::getCustomExtensions);
    assertDefensiveCopy(primaryKey, dataset::getPrimaryKey);
    assertDefensiveCopy(fields, dataset::getFields);
    assertDefensiveCopy(datasetExtensions, dataset::getCustomExtensions);
    assertDefensiveCopy(datasets, definition::getDatasets);
    assertDefensiveCopy(relationships, definition::getRelationships);
    assertDefensiveCopy(metrics, definition::getMetrics);
    assertDefensiveCopy(definitionExtensions, definition::getCustomExtensions);

    uniqueKeys[0][0] = "changed";
    assertArrayEquals(new String[] {"external_id", "source"}, dataset.getUniqueKeys()[0]);
    String[][] returnedUniqueKeys = dataset.getUniqueKeys();
    returnedUniqueKeys[0][0] = "changed";
    assertArrayEquals(new String[] {"external_id", "source"}, dataset.getUniqueKeys()[0]);
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

  private static <T> void assertDefensiveCopy(T[] source, Supplier<T[]> getter) {
    T expected = source[0];
    source[0] = null;
    assertEquals(expected, getter.get()[0]);

    T[] returned = getter.get();
    returned[0] = null;
    assertEquals(expected, getter.get()[0]);
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
