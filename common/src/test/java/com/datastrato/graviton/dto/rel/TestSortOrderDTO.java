/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.LiteralExpression;
import com.datastrato.graviton.dto.rel.SortOrderDTO.Direction;
import com.datastrato.graviton.dto.rel.SortOrderDTO.NullOrdering;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.substrait.type.StringTypeVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSortOrderDTO {

  @Test
  void testCreator() {
    SortOrderDTO sortOrderDTO =
        SortOrderDTO.literalSortOrder("1", "i32", Direction.ASC, NullOrdering.FIRST);
    Assertions.assertEquals(Direction.ASC, sortOrderDTO.getDirection());
    Assertions.assertEquals(NullOrdering.FIRST, sortOrderDTO.getNullOrdering());
    Assertions.assertEquals("1", ((LiteralExpression) sortOrderDTO.getExpression()).getValue());
    Assertions.assertEquals(
        "i32",
        ((LiteralExpression) sortOrderDTO.getExpression())
            .getType()
            .accept(new StringTypeVisitor()));

    sortOrderDTO = SortOrderDTO.nameReferenceSortOrder(Direction.DESC, NullOrdering.LAST, "a");
    Assertions.assertEquals(Direction.DESC, sortOrderDTO.getDirection());
    Assertions.assertEquals(NullOrdering.LAST, sortOrderDTO.getNullOrdering());
    Assertions.assertEquals(
        "a", ((FieldExpression) sortOrderDTO.getExpression()).getFieldName()[0]);
  }

  @Test
  void testJsonSerDe() throws JsonProcessingException {
    SortOrderDTO dto =
        new SortOrderDTO.Builder()
            .withDirection(Direction.ASC)
            .withNullOrder(NullOrdering.FIRST)
            .withExpression(
                new FieldExpression.Builder().withFieldName(new String[] {"field1"}).build())
            .build();
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
            + "  \"nullOrdering\": \"first\"\n"
            + "}";

    JsonNode expected = JsonUtils.objectMapper().readTree(expectedValue);
    JsonNode actual = JsonUtils.objectMapper().readTree(value);
    Assertions.assertEquals(expected, actual);

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
