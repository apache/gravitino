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
import java.time.Instant;
import java.util.Optional;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionStatisticsDTO {

  @Test
  public void testPartitionStatisticsDTO() throws JsonProcessingException {
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

    PartitionStatisticsDTO partitionStatisticsDTO =
        PartitionStatisticsDTO.of("test_partition", new StatisticDTO[] {statisticDTO});

    String serJson = JsonUtils.objectMapper().writeValueAsString(partitionStatisticsDTO);
    PartitionStatisticsDTO deserPartitionStatisticsDTO =
        JsonUtils.objectMapper().readValue(serJson, PartitionStatisticsDTO.class);
    Assertions.assertEquals(partitionStatisticsDTO, deserPartitionStatisticsDTO);
  }
}
