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
package org.apache.gravitino.client;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;
import static javax.servlet.http.HttpServletResponse.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.compress.utils.Lists;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.requests.PartitionStatisticsDropRequest;
import org.apache.gravitino.dto.requests.PartitionStatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.PartitionStatisticsListResponse;
import org.apache.gravitino.dto.stats.PartitionStatisticsDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsDropDTO;
import org.apache.gravitino.dto.stats.PartitionStatisticsUpdateDTO;
import org.apache.gravitino.dto.stats.StatisticDTO;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.stats.SupportsPartitionStatistics;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportsPartitionStatistics extends TestBase {

  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "catalog1";
  private static final String SCHEMA_NAME = "schema1";
  private static final String TABLE_NAME = "table1";

  private static Table relationalTable;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

    relationalTable =
        RelationalTable.from(
            Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            TableDTO.builder()
                .withName(TABLE_NAME)
                .withComment("comment1")
                .withColumns(
                    new ColumnDTO[] {
                      ColumnDTO.builder()
                          .withName("col1")
                          .withDataType(Types.IntegerType.get())
                          .build()
                    })
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient());
  }

  @Test
  public void testListStatisticsForTable() throws JsonProcessingException {
    testListStatistics(relationalTable.supportsPartitionStatistics(), getTableStatisticsPath());
  }

  @Test
  public void testUpdateStatisticsForTable() throws JsonProcessingException {
    testUpdateStatistics(relationalTable.supportsPartitionStatistics(), getTableStatisticsPath());
  }

  @Test
  public void testDropStatisticsForTable() throws JsonProcessingException {
    testDropStatistics(relationalTable.supportsPartitionStatistics(), getTableStatisticsPath());
  }

  private void testListStatistics(SupportsPartitionStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful list
    AuditDTO audit = AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build();

    StatisticDTO stat1 =
        StatisticDTO.builder()
            .withName("row_count")
            .withValue(Optional.of(StatisticValues.longValue(100L)))
            .withReserved(true)
            .withModifiable(false)
            .withAudit(audit)
            .build();

    StatisticDTO stat2 =
        StatisticDTO.builder()
            .withName("custom.user_stat")
            .withValue(Optional.of(StatisticValues.stringValue("test")))
            .withReserved(false)
            .withModifiable(true)
            .withAudit(audit)
            .build();

    Map<String, String> map = Maps.newHashMap();
    map.put("from", "p0");

    PartitionStatisticsDTO partitionStatisticsDTO =
        PartitionStatisticsDTO.of("p0", new StatisticDTO[] {stat1, stat2});
    PartitionStatisticsListResponse response =
        new PartitionStatisticsListResponse(new PartitionStatisticsDTO[] {partitionStatisticsDTO});
    buildMockResource(Method.GET, path, map, null, response, SC_OK);

    List<PartitionStatistics> statistics =
        supportsStatistics.listPartitionStatistics(
            PartitionRange.downTo("p0", PartitionRange.BoundType.CLOSED));
    Assertions.assertEquals(1, statistics.size());
    Assertions.assertEquals("row_count", statistics.get(0).statistics()[0].name());
    Assertions.assertEquals(100L, statistics.get(0).statistics()[0].value().get().value());
    Assertions.assertTrue(statistics.get(0).statistics()[0].reserved());
    Assertions.assertFalse(statistics.get(0).statistics()[0].modifiable());

    Assertions.assertEquals("custom.user_stat", statistics.get(0).statistics()[1].name());
    Assertions.assertEquals("test", statistics.get(0).statistics()[1].value().get().value());
    Assertions.assertFalse(statistics.get(0).statistics()[1].reserved());
    Assertions.assertTrue(statistics.get(0).statistics()[1].modifiable());

    // Test error handling
    ErrorResponse errorResp = ErrorResponse.internalError("Internal error");
    buildMockResource(
        Method.GET, path, Collections.emptyMap(), null, errorResp, SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            supportsStatistics.listPartitionStatistics(
                PartitionRange.upTo("p0", PartitionRange.BoundType.OPEN)));
  }

  private void testUpdateStatistics(SupportsPartitionStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful update
    Map<String, StatisticValue<?>> statisticsToUpdate = new HashMap<>();
    statisticsToUpdate.put("row_count", StatisticValues.longValue(200L));
    statisticsToUpdate.put("custom.user_stat", StatisticValues.stringValue("updated"));

    Map<String, StatisticValue<?>> expectedDTOs = new HashMap<>();
    expectedDTOs.put("row_count", StatisticValues.longValue(200L));
    expectedDTOs.put("custom.user_stat", StatisticValues.stringValue("updated"));

    List<PartitionStatisticsUpdateDTO> updateDTOS = Lists.newArrayList();
    updateDTOS.add(PartitionStatisticsUpdateDTO.of("p0", expectedDTOs));
    PartitionStatisticsUpdateRequest expectedRequest =
        new PartitionStatisticsUpdateRequest(updateDTOS);

    List<PartitionStatisticsUpdate> updates = Lists.newArrayList();
    updates.add(PartitionStatisticsModification.update("p0", statisticsToUpdate));
    BaseResponse response = new BaseResponse(0);
    buildMockResource(Method.PUT, path, Collections.emptyMap(), expectedRequest, response, SC_OK);

    supportsStatistics.updatePartitionStatistics(updates);

    // Test unmodifiable statistic exception
    String unmodifiableErrorJson =
        MAPPER.writeValueAsString(
            ImmutableMap.of(
                "code",
                ErrorConstants.UNSUPPORTED_OPERATION_CODE,
                "type",
                UnmodifiableStatisticException.class.getSimpleName(),
                "message",
                "Cannot modify reserved statistic",
                "stack",
                Collections.emptyList()));
    ErrorResponse unmodifiableError = MAPPER.readValue(unmodifiableErrorJson, ErrorResponse.class);
    buildMockResource(
        Method.PUT,
        path,
        Collections.emptyMap(),
        expectedRequest,
        unmodifiableError,
        SC_METHOD_NOT_ALLOWED);

    Assertions.assertThrows(
        UnmodifiableStatisticException.class,
        () -> supportsStatistics.updatePartitionStatistics(updates));

    // Test illegal statistic name exception
    ErrorResponse illegalNameError =
        ErrorResponse.illegalArguments(
            IllegalStatisticNameException.class.getSimpleName(), "Invalid statistic name", null);
    buildMockResource(
        Method.PUT,
        path,
        Collections.emptyMap(),
        expectedRequest,
        illegalNameError,
        SC_BAD_REQUEST);

    Assertions.assertThrows(
        IllegalStatisticNameException.class,
        () -> supportsStatistics.updatePartitionStatistics(updates));
  }

  private void testDropStatistics(SupportsPartitionStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful drop
    List<String> statisticsToDrop = Arrays.asList("custom.user_stat1", "custom.user_stat2");

    List<PartitionStatisticsDropDTO> requestDrops = Lists.newArrayList();
    requestDrops.add(PartitionStatisticsDropDTO.of("p0", statisticsToDrop));
    PartitionStatisticsDropRequest request = new PartitionStatisticsDropRequest(requestDrops);
    DropResponse response = new DropResponse(true);
    buildMockResource(Method.POST, path, Collections.emptyMap(), request, response, SC_OK);

    List<PartitionStatisticsDrop> drops = Lists.newArrayList();
    drops.add(PartitionStatisticsModification.drop("p0", statisticsToDrop));
    boolean dropped = supportsStatistics.dropPartitionStatistics(drops);
    Assertions.assertTrue(dropped);

    // Test drop non-existing statistics
    response = new DropResponse(false);
    buildMockResource(Method.POST, path, Collections.emptyMap(), request, response, SC_OK);

    dropped = supportsStatistics.dropPartitionStatistics(drops);
    Assertions.assertFalse(dropped);

    // Test unmodifiable statistic exception
    String unmodifiableErrorJson =
        MAPPER.writeValueAsString(
            ImmutableMap.of(
                "code",
                ErrorConstants.UNSUPPORTED_OPERATION_CODE,
                "type",
                UnmodifiableStatisticException.class.getSimpleName(),
                "message",
                "Cannot drop reserved statistic",
                "stack",
                Collections.emptyList()));
    ErrorResponse unmodifiableError = MAPPER.readValue(unmodifiableErrorJson, ErrorResponse.class);
    buildMockResource(
        Method.POST, path, Collections.emptyMap(), null, unmodifiableError, SC_METHOD_NOT_ALLOWED);

    Assertions.assertThrows(
        UnmodifiableStatisticException.class,
        () -> supportsStatistics.dropPartitionStatistics(drops));
  }

  private String getTableStatisticsPath() {
    return String.format(
        "/api/metalakes/%s/objects/table/%s.%s.%s/statistics/partitions",
        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
  }
}
