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
package org.apache.gravitino.storage.relational.po;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

public class TestSemanticModelDefinitionSerDe {

  private static final String GOLDEN_DEFINITION_JSON =
      """
      {
        "datasets": [
          {
            "name": "orders",
            "source": {
              "namespace": ["sales", "mart"],
              "name": "orders"
            }
          }
        ],
        "ai_context": "Certified"
      }
      """;

  @Test
  public void testFullyPopulatedDefinitionRoundTrip() throws Exception {
    SemanticModelDefinition definition = completeDefinition(structuredAIContext());

    SemanticModelDefinition restored =
        SemanticModelDefinitionSerDe.deserialize(
            SemanticModelDefinitionSerDe.serialize(definition));

    assertEquals(definition, restored);
    Field[] restoredFields = restored.datasets()[0].fields();
    assertEquals(DataType.values().length, restoredFields.length);
    for (int index = 0; index < DataType.values().length; index++) {
      assertEquals(DataType.values()[index], restoredFields[index].datatype());
    }
  }

  @Test
  public void testTextAIContextAndGoldenPersistedJson() throws Exception {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of("Certified"))
            .withDatasets(
                new Dataset[] {
                  Dataset.builder()
                      .withName("orders")
                      .withSource(NameIdentifier.of("sales", "mart", "orders"))
                      .build()
                })
            .build();

    String serialized = SemanticModelDefinitionSerDe.serialize(definition);

    assertEquals(
        JsonUtils.anyFieldMapper().readTree(GOLDEN_DEFINITION_JSON),
        JsonUtils.anyFieldMapper().readTree(serialized));
    assertEquals(definition, SemanticModelDefinitionSerDe.deserialize(GOLDEN_DEFINITION_JSON));
  }

  @Test
  public void testNullAndEmptyCollectionsRemainDistinct() throws Exception {
    Dataset baseDataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    Dataset emptyDataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withFields(new Field[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinition unset =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {baseDataset}).build();
    SemanticModelDefinition empty =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {emptyDataset})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();

    SemanticModelDefinition restoredUnset =
        SemanticModelDefinitionSerDe.deserialize(SemanticModelDefinitionSerDe.serialize(unset));
    SemanticModelDefinition restoredEmpty =
        SemanticModelDefinitionSerDe.deserialize(SemanticModelDefinitionSerDe.serialize(empty));

    assertNull(restoredUnset.relationships());
    assertNull(restoredUnset.datasets()[0].fields());
    assertArrayEquals(new Relationship[0], restoredEmpty.relationships());
    assertArrayEquals(new Field[0], restoredEmpty.datasets()[0].fields());
    assertNotEquals(restoredUnset, restoredEmpty);
  }

  private static SemanticModelDefinition completeDefinition(AIContext aiContext) {
    Field[] fields = new Field[DataType.values().length];
    for (int index = 0; index < DataType.values().length; index++) {
      DataType dataType = DataType.values()[index];
      fields[index] =
          Field.builder()
              .withName(dataType.name().toLowerCase(Locale.ROOT))
              .withExpression(expression("orders." + dataType.name().toLowerCase(Locale.ROOT)))
              .withDimension(Dimension.builder().withIsTime(dataType == DataType.DATE).build())
              .withLabel(dataType.name())
              .withDescription("Field for " + dataType.name())
              .withDatatype(dataType)
              .withAIContext(aiContext)
              .withCustomExtensions(new CustomExtension[] {extension()})
              .build();
    }
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[] {"integer"})
            .withUniqueKeys(new String[][] {{"integer"}, {"string", "date"}})
            .withDescription("Certified orders")
            .withAIContext(aiContext)
            .withFields(fields)
            .withCustomExtensions(new CustomExtension[] {extension()})
            .build();
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"integer"})
            .withToColumns(new String[] {"id"})
            .withAIContext(aiContext)
            .withCustomExtensions(new CustomExtension[] {extension()})
            .build();
    Metric metric =
        Metric.builder()
            .withName("order_count")
            .withExpression(expression("COUNT(*)"))
            .withDescription("Order count")
            .withDatatype(DataType.INTEGER)
            .withAIContext(aiContext)
            .withCustomExtensions(new CustomExtension[] {extension()})
            .build();
    return SemanticModelDefinition.builder()
        .withAIContext(aiContext)
        .withDatasets(new Dataset[] {dataset})
        .withRelationships(new Relationship[] {relationship})
        .withMetrics(new Metric[] {metric})
        .withCustomExtensions(new CustomExtension[] {extension()})
        .build();
  }

  private static AIContext structuredAIContext() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("null_value", null);
    nested.put("boolean_value", true);
    nested.put("integer_value", new BigInteger("7"));
    nested.put("decimal_value", new BigDecimal("1.50"));
    nested.put("list_value", List.of("sales", new BigInteger("3"), new BigDecimal("2.25")));
    return AIContext.of(
        AIContextObject.builder()
            .withInstructions("Use governed definitions")
            .withSynonyms(new String[] {"orders", "purchases"})
            .withExamples(new String[] {"Revenue by month"})
            .withAdditionalProperties(Map.of("nested", nested))
            .build());
  }

  private static Expression expression(String value) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder().withDialect("ansi").withExpression(value).build(),
              DialectExpression.builder()
                  .withDialect("spark")
                  .withExpression("spark_" + value)
                  .build()
            })
        .build();
  }

  private static CustomExtension extension() {
    return CustomExtension.builder()
        .withVendorName("example")
        .withData("{\"enabled\":true}")
        .build();
  }
}
