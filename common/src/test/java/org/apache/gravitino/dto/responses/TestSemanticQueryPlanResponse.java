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
package org.apache.gravitino.dto.responses;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.PlanFieldRef;
import org.apache.gravitino.semantic.PlanSelection;
import org.apache.gravitino.semantic.PlanSource;
import org.apache.gravitino.semantic.SemanticQueryPlan;
import org.junit.jupiter.api.Test;

public class TestSemanticQueryPlanResponse {

  private static final String SOURCES_JSON =
      "\"sources\":[{\"dataset\":\"orders\","
          + "\"source\":[\"catalog\",\"schema\",\"orders_table\"]}],";

  private static final String PLAN_JSON =
      "{\"semanticModel\":[\"metalake\",\"catalog\",\"schema\",\"sales_model\"],"
          + "\"semanticModelVersion\":3,"
          + SOURCES_JSON
          + "\"joins\":[],"
          + "\"selections\":[{\"name\":\"revenue\",\"role\":\"METRIC\",\"aggregate\":true,"
          + "\"field\":{\"dataset\":\"orders\","
          + "\"expression\":{\"ANSI_SQL\":\"SUM(orders.amount)\"},\"datatype\":\"DECIMAL\"}}],"
          + "\"filters\":[],\"groupBy\":[],\"limit\":10,\"warnings\":[]}";

  @Test
  public void testSerDeAndConvert() throws JsonProcessingException {
    SemanticQueryPlan plan = plan();

    SemanticQueryPlanResponse response = new SemanticQueryPlanResponse(plan);
    String json = JsonUtils.objectMapper().writeValueAsString(response);
    JsonNode root = JsonUtils.objectMapper().readTree(json);

    assertEquals(0, root.path("code").asInt());
    JsonNode planNode = root.path("plan");
    assertEquals(3, planNode.path("semanticModelVersion").asInt());
    assertEquals(
        "SUM(orders.amount)",
        planNode
            .path("selections")
            .get(0)
            .path("field")
            .path("expression")
            .path("ANSI_SQL")
            .asText());
    assertTrue(root.path("sql").isMissingNode());
    assertTrue(root.path("dialect").isMissingNode());

    SemanticQueryPlanResponse deserialized =
        JsonUtils.objectMapper().readValue(json, SemanticQueryPlanResponse.class);
    deserialized.validate();
    assertEquals(plan, deserialized.toPlan());
  }

  @Test
  public void testDeserializeWireContract() throws JsonProcessingException {
    SemanticQueryPlanResponse response = read("{\"code\":0,\"plan\":" + PLAN_JSON + "}");
    response.validate();

    assertEquals(plan(), response.toPlan());
  }

  @Test
  public void testValidation() throws JsonProcessingException {
    assertThrows(IllegalArgumentException.class, new SemanticQueryPlanResponse()::validate);
    assertThrows(IllegalArgumentException.class, () -> read("{\"code\":0}").validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> read("{\"code\":0,\"plan\":" + PLAN_JSON.replace(":3,", ":0,") + "}").validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"plan\":" + PLAN_JSON.replace(SOURCES_JSON, "\"sources\":[],") + "}")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"plan\":"
                    + PLAN_JSON.replace("\"groupBy\":[]", "\"groupBy\":[\"revenue\"]")
                    + "}")
                .toPlan());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"plan\":" + PLAN_JSON.replace("\"limit\":10", "\"limit\":-1") + "}")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"plan\":"
                    + PLAN_JSON.replace("\"warnings\":[]", "\"warnings\":[null]")
                    + "}")
                .validate());
  }

  private static SemanticQueryPlan plan() {
    PlanFieldRef revenue =
        PlanFieldRef.ofExpression("orders", expression("SUM(orders.amount)"), DataType.DECIMAL);
    return SemanticQueryPlan.builder()
        .withSemanticModel(NameIdentifier.of("metalake", "catalog", "schema", "sales_model"))
        .withSemanticModelVersion(3)
        .withSources(
            Collections.singletonList(
                PlanSource.of("orders", NameIdentifier.of("catalog", "schema", "orders_table"))))
        .withSelections(
            Collections.singletonList(
                PlanSelection.of("revenue", PlanSelection.Role.METRIC, true, revenue)))
        .withGroupBy(Collections.emptyList())
        .withLimit(10L)
        .withWarnings(Collections.emptyList())
        .build();
  }

  private static Expression expression(String text) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(text)
                  .build()
            })
        .build();
  }

  private static SemanticQueryPlanResponse read(String json) throws JsonProcessingException {
    return JsonUtils.objectMapper().readValue(json, SemanticQueryPlanResponse.class);
  }
}
