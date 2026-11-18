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
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.dto.requests.PartitionStatisticsUpdateRequest;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionStatisticsUpdateDTO {

  @Test
  public void testValidateAcceptsStatistics() {
    PartitionStatisticsUpdateDTO dto =
        PartitionStatisticsUpdateDTO.of(
            "p1", ImmutableMap.of("custom-k", StatisticValues.longValue(1L)));

    Assertions.assertEquals("p1", dto.partitionName());
    Assertions.assertEquals(1, dto.statistics().size());
  }

  @Test
  public void testValidateRejectsNullStatisticValue() {
    Map<String, StatisticValue<?>> statistics = new HashMap<>();
    statistics.put("custom-k", null);

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionStatisticsUpdateDTO.of("p1", statistics));

    Assertions.assertTrue(
        e.getMessage().contains("custom-k"), () -> "Unexpected message: " + e.getMessage());
  }

  @Test
  public void testValidateRejectsBlankStatisticName() {
    Map<String, StatisticValue<?>> statistics = new HashMap<>();
    statistics.put("  ", StatisticValues.longValue(1L));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PartitionStatisticsUpdateDTO.of("p1", statistics));
  }

  @Test
  public void testRequestWithNullStatisticValueIsRejected() throws JsonProcessingException {
    // Jackson's MapDeserializer does not invoke the contentUsing deserializer for a VALUE_NULL
    // content token, it uses getNullValue(), so a top-level JSON null lands in the map and only
    // validate() can catch it. Any mapper reproduces that; no registered module is involved.
    String json = "{\"updates\":[{\"partitionName\":\"p1\",\"statistics\":{\"custom-k\":null}}]}";
    PartitionStatisticsUpdateRequest request =
        JsonUtils.objectMapper().readValue(json, PartitionStatisticsUpdateRequest.class);

    Assertions.assertNull(
        request.getUpdates().get(0).statistics().get("custom-k"),
        "the JSON null is expected to survive deserialization as a null map value");
    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }
}
