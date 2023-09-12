/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.graviton.dto.rel.SortOrderDTO;
import com.datastrato.graviton.dto.rel.SortOrderDTO.Direction;
import com.datastrato.graviton.dto.rel.SortOrderDTO.NullOrder;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSortOrderDTO {

  @Test
  void test() throws JsonProcessingException {
    SortOrderDTO dto =
        new SortOrderDTO.Builder()
            .withDirection(Direction.ASC)
            .withNullOrder(NullOrder.FIRST)
            .withExpression(
                new FieldExpression.Builder().withFieldName(new String[] {"field1"}).build())
            .build();
    JsonUtils.objectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    String value = JsonUtils.objectMapper().writeValueAsString(dto);
    String expectedValue =
        "{\n"
            + "  \"expression\": {\n"
            + "    \"expressionType\": \"field\",\n"
            + "    \"fieldName\": [\n"
            + "      \"field1\"\n"
            + "    ]\n"
            + "  },\n"
            + "  \"direction\": \"asc\",\n"
            + "  \"nullOrder\": \"first\"\n"
            + "}";

    JsonNode expected = JsonUtils.objectMapper().readTree(expectedValue);
    JsonNode acutal = JsonUtils.objectMapper().readTree(value);
    Assertions.assertEquals(expected, acutal);

    SortOrderDTO dto2 = JsonUtils.objectMapper().readValue(value, SortOrderDTO.class);
    Assertions.assertEquals(dto, dto2);

    dto =
        new SortOrderDTO.Builder()
            .withExpression(
                new FieldExpression.Builder().withFieldName(new String[] {"field1"}).build())
            .build();

    Assertions.assertEquals(
        dto,
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(dto), SortOrderDTO.class));

    dto =
        new SortOrderDTO.Builder()
            .withExpression(
                new FunctionExpression.Builder()
                    .withFuncName("date")
                    .withArgs(
                        new Expression[] {
                          new FieldExpression.Builder()
                              .withFieldName(new String[] {"field1"})
                              .build()
                        })
                    .build())
            .build();

    Assertions.assertEquals(
        dto,
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(dto), SortOrderDTO.class));
  }
}
