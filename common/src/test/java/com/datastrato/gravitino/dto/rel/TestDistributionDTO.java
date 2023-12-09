/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.expressions.FuncExpressionDTO;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDistributionDTO {
  @Test
  void test() throws JsonProcessingException {

    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withArgs(FieldReferenceDTO.of("a"))
            .build();

    String stringValue = JsonUtils.objectMapper().writeValueAsString(distributionDTO);
    String expected =
        "{\n"
            + "    \"strategy\": \"hash\",\n"
            + "    \"number\": 10,\n"
            + "    \"funcArgs\": [\n"
            + "        {\n"
            + "            \"type\": \"field\",\n"
            + "            \"fieldName\": [\n"
            + "                \"a\"\n"
            + "            ]\n"
            + "        }\n"
            + "    ]\n"
            + "}";

    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(stringValue));

    distributionDTO =
        new DistributionDTO.Builder()
            .withArgs(
                new FuncExpressionDTO.Builder()
                    .withFunctionName("date")
                    .withFunctionArgs(FieldReferenceDTO.of("a"))
                    .build())
            .build();

    Assertions.assertEquals(
        distributionDTO,
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(distributionDTO),
                DistributionDTO.class));
  }
}
