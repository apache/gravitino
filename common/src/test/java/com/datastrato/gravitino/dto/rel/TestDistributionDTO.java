/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.rel.DistributionDTO.Strategy;
import com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDistributionDTO {

  @Test
  void testCreator() {
    DistributionDTO distributionDTO =
        DistributionDTO.nameReferenceDistribution(Strategy.HASH, 10, "a");
    Assertions.assertEquals(Strategy.HASH, distributionDTO.getStrategy());
    Assertions.assertEquals(10, distributionDTO.getNumber());
    Assertions.assertEquals(1, distributionDTO.getExpressions().length);
    Assertions.assertEquals(
        "a", ((FieldExpression) distributionDTO.getExpressions()[0]).getFieldName()[0]);

    distributionDTO = DistributionDTO.nameReferenceDistribution(Strategy.HASH, 10, "a", "b");
    Assertions.assertEquals(Strategy.HASH, distributionDTO.getStrategy());
    Assertions.assertEquals(10, distributionDTO.getNumber());
    Assertions.assertEquals(2, distributionDTO.getExpressions().length);
    Assertions.assertEquals(
        "a", ((FieldExpression) distributionDTO.getExpressions()[0]).getFieldName()[0]);
    Assertions.assertEquals(
        "b", ((FieldExpression) distributionDTO.getExpressions()[1]).getFieldName()[0]);
  }

  @Test
  void test() throws JsonProcessingException {
    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withExpressions(
                new Expression[] {
                  new FieldExpression.Builder().withFieldName(new String[] {"a"}).build()
                })
            .build();

    String stringValue = JsonUtils.objectMapper().writeValueAsString(distributionDTO);
    String expected =
        "{\n"
            + "  \"expressions\": [\n"
            + "    {\n"
            + "      \"expressionType\": \"field\",\n"
            + "      \"fieldName\": [\n"
            + "        \"a\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"number\": 10,\n"
            + "  \"strategy\": \"hash\"\n"
            + "}";

    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(stringValue));

    distributionDTO =
        new DistributionDTO.Builder()
            .withExpressions(
                new Expression[] {
                  new FunctionExpression.Builder()
                      .withFuncName("date")
                      .withArgs(
                          new Expression[] {
                            new FieldExpression.Builder().withFieldName(new String[] {"a"}).build()
                          })
                      .build()
                })
            .build();

    Assertions.assertEquals(
        distributionDTO,
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(distributionDTO),
                DistributionDTO.class));
  }
}
