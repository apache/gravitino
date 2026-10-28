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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.SemanticQueryPlan;
import org.junit.jupiter.api.Test;

public class TestSemanticQueryPlanResponse {

  @Test
  public void testSerDeAndConvert() throws JsonProcessingException {
    SemanticQueryPlan plan =
        SemanticQueryPlan.of(
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            "trino",
            "test-planner",
            Collections.singletonList("amount treated as decimal"));

    SemanticQueryPlanResponse response = new SemanticQueryPlanResponse(plan);
    String json = JsonUtils.objectMapper().writeValueAsString(response);
    JsonNode root = JsonUtils.objectMapper().readTree(json);

    assertEquals(0, root.path("code").asInt());
    assertEquals(plan.sql(), root.path("sql").asText());
    assertEquals("trino", root.path("dialect").asText());
    assertEquals("test-planner", root.path("planner").asText());
    assertEquals(1, root.path("warnings").size());

    SemanticQueryPlanResponse deserialized =
        JsonUtils.objectMapper().readValue(json, SemanticQueryPlanResponse.class);
    deserialized.validate();
    assertEquals(plan, deserialized.toPlan());
  }

  @Test
  public void testValidation() throws JsonProcessingException {
    assertThrows(IllegalArgumentException.class, new SemanticQueryPlanResponse()::validate);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"dialect\":\"trino\",\"planner\":\"p\",\"warnings\":[]}")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"sql\":\"SELECT 1\",\"planner\":\"p\",\"warnings\":[]}").validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"sql\":\"SELECT 1\",\"dialect\":\"trino\",\"warnings\":[]}")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"sql\":\"SELECT 1\",\"dialect\":\"trino\",\"planner\":\"p\"}")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            read("{\"code\":0,\"sql\":\"SELECT 1\",\"dialect\":\"trino\",\"planner\":\"p\","
                    + "\"warnings\":[\" \"]}")
                .validate());
  }

  @Test
  public void testEmptyWarnings() throws JsonProcessingException {
    SemanticQueryPlan plan =
        SemanticQueryPlan.of("SELECT 1", "spark", "planner", Collections.emptyList());

    SemanticQueryPlanResponse deserialized =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(new SemanticQueryPlanResponse(plan)),
                SemanticQueryPlanResponse.class);
    deserialized.validate();

    assertArrayEquals(new String[0], deserialized.getWarnings());
    assertTrue(deserialized.toPlan().warnings().isEmpty());
  }

  private static SemanticQueryPlanResponse read(String json) throws JsonProcessingException {
    return JsonUtils.objectMapper().readValue(json, SemanticQueryPlanResponse.class);
  }
}
