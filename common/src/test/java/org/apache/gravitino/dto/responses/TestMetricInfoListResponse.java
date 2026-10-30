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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.semantic.MetricDTO;
import org.apache.gravitino.dto.semantic.SemanticModelMetricDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.junit.jupiter.api.Test;

public class TestMetricInfoListResponse {
  @Test
  void testRoundTripPreservesCompleteMetricAndNamespace() throws Exception {
    String json =
        """
        {"name":"收入.total","expression":{"dialects":[{"dialect":"ansi","expression":"SUM(orders.amount)"}]},
         "description":"Revenue","datatype":"Float","aiContext":{"instructions":"Use for revenue","extra":[1,"two"]},
         "customExtensions":[{"vendorName":"vendor","data":"[1,2]"}]}
        """;
    MetricDTO metric = JsonUtils.objectMapper().readValue(json, MetricDTO.class);
    Namespace namespace = Namespace.of("lake", "catalog", "schema", "model");
    SemanticModelMetricDTO source = new SemanticModelMetricDTO(namespace, metric);
    SemanticModelMetricDTO converted = SemanticModelMetricDTO.fromSemanticModelMetric(source);
    assertEquals(source.metric(), converted.metric());
    SemanticModelMetricDTO[] array = {converted};
    MetricInfoListResponse response = new MetricInfoListResponse(array);
    array[0] = null;
    response.getMetrics()[0] = null;
    response.validate();
    String serialized = JsonUtils.objectMapper().writeValueAsString(response);
    JsonNode body = JsonUtils.objectMapper().readTree(serialized);
    JsonNode info = body.path("metrics").get(0);
    assertEquals(2, info.size());
    assertFalse(info.has("name"));
    assertFalse(info.has("metricId"));
    assertEquals(JsonUtils.objectMapper().readTree(json), info.path("metric"));
    assertEquals(0, body.path("code").asInt());
    MetricInfoListResponse decoded =
        JsonUtils.objectMapper().readValue(serialized, MetricInfoListResponse.class);
    decoded.validate();
    SemanticModelMetric result = decoded.getMetrics()[0];
    assertEquals(namespace, result.namespace());
    assertEquals(metric.toMetric(), result.metric());
    String[] levels = result.namespace().levels();
    levels[0] = "changed";
    assertEquals(namespace, result.namespace());
  }

  @Test
  void testEmptyAndInvalidResponses() throws Exception {
    MetricInfoListResponse empty = new MetricInfoListResponse(new SemanticModelMetricDTO[0]);
    empty.validate();
    JsonNode json = JsonUtils.objectMapper().valueToTree(empty);
    assertEquals(0, json.path("metrics").size());
    assertArrayEquals(new SemanticModelMetricDTO[0], empty.getMetrics());
    assertThrows(IllegalArgumentException.class, new MetricInfoListResponse(null)::validate);
    assertThrows(
        IllegalArgumentException.class,
        new MetricInfoListResponse(new SemanticModelMetricDTO[] {null})::validate);
    MetricInfoListResponse missing =
        JsonUtils.objectMapper().readValue("{\"code\":0}", MetricInfoListResponse.class);
    assertThrows(IllegalArgumentException.class, missing::validate);
  }
}
