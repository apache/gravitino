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
        DistributionDTO.builder()
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
        DistributionDTO.builder()
            .withArgs(
                FuncExpressionDTO.builder()
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

  @Test
  void testDistributionDTOEqualsReturnsTrue() {

    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withArgs(FieldReferenceDTO.of("a"))
            .build();

    DistributionDTO distributionDTOOther =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withArgs(FieldReferenceDTO.of("a"))
            .build();

    Assertions.assertEquals(distributionDTO, distributionDTOOther);
  }

  @Test
  void testDistributionDTOEqualsReturnsFalse() {

    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withArgs(FieldReferenceDTO.of("a"))
            .build();

    DistributionDTO distributionDTOOther =
        new DistributionDTO.Builder()
            .withNumber(10)
            .withStrategy(Strategy.HASH)
            .withArgs(FieldReferenceDTO.of("b"))
            .build();

    Assertions.assertNotEquals(distributionDTO, distributionDTOOther);
  }
}
