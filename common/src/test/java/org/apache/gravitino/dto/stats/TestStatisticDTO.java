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
package org.apache.gravitino.dto.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStatisticDTO {
  @Test
  public void testStatisticSerDe() throws JsonProcessingException {
    // case 1: StatisticDTO with long value
    StatisticDTO statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withValue(Optional.of(StatisticValues.longValue(100L)))
            .withReserved(false)
            .withModifiable(false)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    StatisticDTO deserStatisticDTO =
        JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);

    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 2: StatisticDTO with string value
    statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withValue(Optional.of(StatisticValues.stringValue("test_value")))
            .withReserved(true)
            .withModifiable(true)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    deserStatisticDTO = JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 3: StatisticDTO with null value
    statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withReserved(false)
            .withModifiable(true)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    Assertions.assertTrue(serJson.contains("null"));
    deserStatisticDTO = JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 4: StatisticDTO with list value
    statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withValue(
                Optional.of(
                    StatisticValues.listValue(
                        Lists.newArrayList(
                            StatisticValues.stringValue("value1"),
                            StatisticValues.stringValue("value2")))))
            .withReserved(false)
            .withModifiable(true)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    deserStatisticDTO = JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 5: StatisticDTO with double value
    statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withValue(Optional.of(StatisticValues.doubleValue(99.99)))
            .withReserved(true)
            .withModifiable(false)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    deserStatisticDTO = JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 6: StatisticDTO with boolean value
    statisticDTO =
        StatisticDTO.builder()
            .withName("statistic_test")
            .withValue(Optional.of(StatisticValues.booleanValue(true)))
            .withReserved(false)
            .withModifiable(true)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(statisticDTO);
    deserStatisticDTO = JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(statisticDTO, deserStatisticDTO);

    // case 7: StatisticDTO with complex object value
    Map<String, StatisticValue<?>> map = Maps.newHashMap();
    map.put("key1", StatisticValues.stringValue("value1"));
    map.put("key2", StatisticValues.longValue(200L));
    StatisticDTO complexStatisticDTO =
        StatisticDTO.builder()
            .withName("complex_statistic")
            .withValue(Optional.of(StatisticValues.objectValue(map)))
            .withReserved(false)
            .withModifiable(true)
            .withAudit(
                AuditDTO.builder()
                    .withCreator("test_user")
                    .withCreateTime(Instant.now())
                    .withLastModifier("test_user")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    serJson = JsonUtils.objectMapper().writeValueAsString(complexStatisticDTO);
    StatisticDTO deserComplexStatisticDTO =
        JsonUtils.objectMapper().readValue(serJson, StatisticDTO.class);
    Assertions.assertEquals(complexStatisticDTO, deserComplexStatisticDTO);
  }
}
