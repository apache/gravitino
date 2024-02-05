/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.expressions.FuncExpressionDTO;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
                new FuncExpressionDTO.Builder()
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
