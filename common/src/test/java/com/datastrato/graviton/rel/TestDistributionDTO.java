/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.dto.rel.DistributionDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO.DistributionMethod;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDistributionDTO {
  @Test
  void test() throws JsonProcessingException {
    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withDistNum(10)
            .withDistMethod(DistributionMethod.HASH)
            .withExpressions(
                Lists.newArrayList(
                    new FieldExpression.Builder().withFieldName(new String[] {"a"}).build()))
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
            + "  \"distributionNumber\": 10,\n"
            + "  \"distributionMethod\": \"hash\"\n"
            + "}";

    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(stringValue));

    distributionDTO =
        new DistributionDTO.Builder()
            .withExpressions(
                Lists.newArrayList(
                    new FunctionExpression.Builder()
                        .withFuncName("date")
                        .withArgs(
                            new Expression[] {
                              new FieldExpression.Builder()
                                  .withFieldName(new String[] {"a"})
                                  .build()
                            })
                        .build()))
            .build();

    Assertions.assertEquals(
        distributionDTO,
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(distributionDTO),
                DistributionDTO.class));
  }
}
