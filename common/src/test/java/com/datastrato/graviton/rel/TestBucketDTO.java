/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.rel;

import com.datastrato.graviton.dto.rel.BucketDTO;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FunctionExpression;
import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBucketDTO {
  @Test
  void test() throws JsonProcessingException {
    BucketDTO bucketDTO =
        new BucketDTO.Builder()
            .withBucketNum(10)
            .withBucketMethod(BucketDTO.BucketMethod.HASH)
            .withExpressions(
                Lists.newArrayList(
                    new FieldExpression.Builder().withFieldName(new String[] {"a"}).build()))
            .build();

    String stringValue = JsonUtils.objectMapper().writeValueAsString(bucketDTO);
    String expected =
        "{\n"
            + "  \"expressions\": [\n"
            + "    {\n"
            + "      \"expressionType\": \"FIELD\",\n"
            + "      \"fieldName\": [\n"
            + "        \"a\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"bucket_num\": 10,\n"
            + "  \"bucket_method\": \"hash\"\n"
            + "}";

    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(stringValue));

    bucketDTO =
        new BucketDTO.Builder()
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
        bucketDTO,
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(bucketDTO), BucketDTO.class));
  }
}
