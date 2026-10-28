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
package org.apache.gravitino.dto.requests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.dto.semantic.SemanticQueryFilterDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.SemanticQuery;
import org.apache.gravitino.semantic.SemanticQueryFilter;
import org.apache.gravitino.semantic.SemanticQueryFilter.Operator;
import org.junit.jupiter.api.Test;

public class TestPlanSemanticQueryRequest {

  @Test
  public void testDeserializeAndConvert() throws JsonProcessingException {
    String json =
        "{"
            + "\"metrics\":[\"order_amount\"],"
            + "\"dimensions\":[\"region\"],"
            + "\"filters\":[{\"field\":\"order_date\",\"operator\":\"ge\",\"value\":\"2024-01-01\"}],"
            + "\"dialect\":\"trino\""
            + "}";

    PlanSemanticQueryRequest request =
        JsonUtils.objectMapper().readValue(json, PlanSemanticQueryRequest.class);
    request.validate();
    SemanticQuery query = request.toQuery();

    assertEquals(Collections.singletonList("order_amount"), query.metrics());
    assertEquals(Collections.singletonList("region"), query.dimensions());
    assertEquals(
        Collections.singletonList(SemanticQueryFilter.of("order_date", Operator.GE, "2024-01-01")),
        query.filters());
    assertEquals("trino", query.dialect());
  }

  @Test
  public void testOmittedCollectionsAreAccepted() throws JsonProcessingException {
    PlanSemanticQueryRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"metrics\":[\"order_amount\"],\"dialect\":\"spark\"}",
                PlanSemanticQueryRequest.class);
    request.validate();

    SemanticQuery query = request.toQuery();
    assertTrue(query.dimensions().isEmpty());
    assertTrue(query.filters().isEmpty());
  }

  @Test
  public void testSerializationPreservesFieldNamesAndOperatorCase() throws JsonProcessingException {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Arrays.asList("order_amount"))
            .withDimensions(Collections.emptyList())
            .withFilters(
                Collections.singletonList(
                    SemanticQueryFilter.of("order_date", Operator.GE, "2024-01-01")))
            .withDialect("trino")
            .build();

    JsonNode serialized =
        JsonUtils.objectMapper()
            .readTree(
                JsonUtils.objectMapper()
                    .writeValueAsString(PlanSemanticQueryRequest.fromQuery(query)));

    assertEquals("order_amount", serialized.path("metrics").get(0).asText());
    assertEquals("ge", serialized.path("filters").get(0).path("operator").asText());
    assertEquals("trino", serialized.path("dialect").asText());
  }

  @Test
  public void testValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PlanSemanticQueryRequest(null, null, null, null).validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlanSemanticQueryRequest(
                    Collections.emptyList(), Collections.emptyList(), null, "trino")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlanSemanticQueryRequest(
                    Collections.singletonList(" "), Collections.emptyList(), null, "trino")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlanSemanticQueryRequest(
                    Collections.singletonList("metric"),
                    Collections.singletonList(""),
                    null,
                    "trino")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlanSemanticQueryRequest(
                    Collections.singletonList("metric"),
                    Collections.emptyList(),
                    Collections.singletonList(null),
                    "trino")
                .validate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PlanSemanticQueryRequest(
                    Collections.singletonList("metric"),
                    Collections.emptyList(),
                    Collections.singletonList(new SemanticQueryFilterDTO("field", "between", "v")),
                    "trino")
                .validate());
  }
}
