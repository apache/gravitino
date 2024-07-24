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
import org.apache.gravitino.dto.rel.expressions.FieldReferenceDTO;
import org.apache.gravitino.dto.rel.expressions.FuncExpressionDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
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
}
