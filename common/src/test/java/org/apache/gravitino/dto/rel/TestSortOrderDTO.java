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

package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.gravitino.dto.rel.expressions.FieldReferenceDTO;
import org.apache.gravitino.dto.rel.expressions.FuncExpressionDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSortOrderDTO {
  @Test
  void testJsonSerDe() throws JsonProcessingException {
    SortOrderDTO dto =
        SortOrderDTO.builder()
            .withDirection(SortDirection.ASCENDING)
            .withNullOrder(NullOrdering.NULLS_FIRST)
            .withSortTerm(FieldReferenceDTO.of("field1"))
            .build();
    String value = JsonUtils.objectMapper().writeValueAsString(dto);
    String expectedValue =
        "{\n"
            + "    \"sortTerm\": {\n"
            + "        \"type\": \"field\",\n"
            + "        \"fieldName\": [\n"
            + "            \"field1\"\n"
            + "        ]\n"
            + "    },\n"
            + "    \"direction\": \"asc\",\n"
            + "    \"nullOrdering\": \"nulls_first\"\n"
            + "}";

    JsonNode expected = JsonUtils.objectMapper().readTree(expectedValue);
    JsonNode actual = JsonUtils.objectMapper().readTree(value);
    Assertions.assertEquals(expected, actual);

    SortOrderDTO dto2 = JsonUtils.objectMapper().readValue(value, SortOrderDTO.class);
    Assertions.assertEquals(dto, dto2);

    dto = SortOrderDTO.builder().withSortTerm(FieldReferenceDTO.of("field1")).build();

    Assertions.assertEquals(
        dto,
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(dto), SortOrderDTO.class));

    dto =
        SortOrderDTO.builder()
            .withSortTerm(
                FuncExpressionDTO.builder()
                    .withFunctionName("date")
                    .withFunctionArgs(FieldReferenceDTO.of("field1"))
                    .build())
            .build();

    Assertions.assertEquals(
        dto,
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(dto), SortOrderDTO.class));
  }
}
