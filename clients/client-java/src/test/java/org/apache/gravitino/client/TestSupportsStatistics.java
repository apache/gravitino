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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.hc.core5.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.requests.StatisticsUpdateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.StatisticListResponse;
import org.apache.gravitino.dto.stats.StatisticDTO;
import org.apache.gravitino.dto.stats.StatisticValueDTO;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.stats.SupportsStatistics;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportsStatistics extends TestBase {

  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "catalog1";
  private static final String SCHEMA_NAME = "schema1";
  private static final String TABLE_NAME = "table1";
  private static final String FILESET_NAME = "fileset1";

  private static Table relationalTable;
  private static Fileset genericFileset;

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

    genericFileset =
        new GenericFileset(
            FilesetDTO.builder()
                .name(FILESET_NAME)
                .comment("comment1")
                .type(Fileset.Type.MANAGED)
                .storageLocations(
                    Collections.singletonMap(
                        LOCATION_NAME_UNKNOWN, "hdfs://localhost:9000/user/test"))
                .properties(Collections.emptyMap())
                .audit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME));
  }

  @Test
  public void testListStatisticsForTable() throws JsonProcessingException {
    testListStatistics((SupportsStatistics) relationalTable, getTableStatisticsPath());
  }

  @Test
  public void testListStatisticsForFileset() throws JsonProcessingException {
    testListStatistics((SupportsStatistics) genericFileset, getFilesetStatisticsPath());
  }

  @Test
  public void testUpdateStatisticsForTable() throws JsonProcessingException {
    testUpdateStatistics((SupportsStatistics) relationalTable, getTableStatisticsPath());
  }

  @Test
  public void testUpdateStatisticsForFileset() throws JsonProcessingException {
    testUpdateStatistics((SupportsStatistics) genericFileset, getFilesetStatisticsPath());
  }

  @Test
  public void testDropStatisticsForTable() throws JsonProcessingException {
    testDropStatistics((SupportsStatistics) relationalTable, getTableStatisticsPath());
  }

  @Test
  public void testDropStatisticsForFileset() throws JsonProcessingException {
    testDropStatistics((SupportsStatistics) genericFileset, getFilesetStatisticsPath());
  }

  private void testListStatistics(SupportsStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful list
    StatisticDTO stat1 =
        StatisticDTO.builder()
            .withName("row_count")
            .withValue(new StatisticValueDTO.LongValueDTO(100L))
            .withReserved(true)
            .withModifiable(false)
            .build();

    StatisticDTO stat2 =
        StatisticDTO.builder()
            .withName("custom.user_stat")
            .withValue(new StatisticValueDTO.StringValueDTO("test"))
            .withReserved(false)
            .withModifiable(true)
            .build();

    StatisticListResponse response = new StatisticListResponse(new StatisticDTO[] {stat1, stat2});
    buildMockResource(Method.GET, path, Collections.emptyMap(), null, response, SC_OK);

    List<Statistic> statistics = supportsStatistics.listStatistics();
    Assertions.assertEquals(2, statistics.size());
    Assertions.assertEquals("row_count", statistics.get(0).name());
    Assertions.assertEquals(100L, statistics.get(0).value().get().value());
    Assertions.assertTrue(statistics.get(0).reserved());
    Assertions.assertFalse(statistics.get(0).modifiable());

    Assertions.assertEquals("custom.user_stat", statistics.get(1).name());
    Assertions.assertEquals("test", statistics.get(1).value().get().value());
    Assertions.assertFalse(statistics.get(1).reserved());
    Assertions.assertTrue(statistics.get(1).modifiable());

    // Test error handling
    ErrorResponse errorResp = ErrorResponse.internalError("Internal error");
    buildMockResource(
        Method.GET, path, Collections.emptyMap(), null, errorResp, SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(RuntimeException.class, () -> supportsStatistics.listStatistics());
  }

  private void testUpdateStatistics(SupportsStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful update
    Map<String, StatisticValue<?>> statisticsToUpdate = new HashMap<>();
    statisticsToUpdate.put("row_count", StatisticValues.longValue(200L));
    statisticsToUpdate.put("custom.user_stat", StatisticValues.stringValue("updated"));

    Map<String, StatisticValueDTO<?>> expectedDTOs = new HashMap<>();
    expectedDTOs.put("row_count", new StatisticValueDTO.LongValueDTO(200L));
    expectedDTOs.put("custom.user_stat", new StatisticValueDTO.StringValueDTO("updated"));

    StatisticsUpdateRequest expectedRequest =
        StatisticsUpdateRequest.builder().withStatistics(expectedDTOs).build();

    StatisticDTO updatedStat1 =
        StatisticDTO.builder()
            .withName("row_count")
            .withValue(new StatisticValueDTO.LongValueDTO(200L))
            .withReserved(true)
            .withModifiable(false)
            .build();

    StatisticDTO updatedStat2 =
        StatisticDTO.builder()
            .withName("custom.user_stat")
            .withValue(new StatisticValueDTO.StringValueDTO("updated"))
            .withReserved(false)
            .withModifiable(true)
            .build();

    StatisticListResponse response =
        new StatisticListResponse(new StatisticDTO[] {updatedStat1, updatedStat2});
    buildMockResource(Method.POST, path, Collections.emptyMap(), expectedRequest, response, SC_OK);

    List<Statistic> updatedStatistics = supportsStatistics.updateStatistics(statisticsToUpdate);
    Assertions.assertEquals(2, updatedStatistics.size());
    Assertions.assertEquals(200L, updatedStatistics.get(0).value().get().value());
    Assertions.assertEquals("updated", updatedStatistics.get(1).value().get().value());

    // Test unmodifiable statistic exception
    // Need to create ErrorResponse with proper type for UnmodifiableStatisticException
    String unmodifiableErrorJson =
        MAPPER.writeValueAsString(
            ImmutableMap.of(
                "code",
                ErrorConstants.FORBIDDEN_CODE,
                "type",
                UnmodifiableStatisticException.class.getSimpleName(),
                "message",
                "Cannot modify reserved statistic",
                "stack",
                Collections.emptyList()));
    ErrorResponse unmodifiableError = MAPPER.readValue(unmodifiableErrorJson, ErrorResponse.class);
    buildMockResource(
        Method.POST,
        path,
        Collections.emptyMap(),
        expectedRequest,
        unmodifiableError,
        SC_FORBIDDEN);

    Assertions.assertThrows(
        UnmodifiableStatisticException.class,
        () -> supportsStatistics.updateStatistics(statisticsToUpdate));

    // Test illegal statistic name exception
    ErrorResponse illegalNameError =
        ErrorResponse.illegalArguments(
            IllegalStatisticNameException.class.getSimpleName(), "Invalid statistic name", null);
    buildMockResource(
        Method.POST, path, Collections.emptyMap(), expectedRequest, illegalNameError, SC_FORBIDDEN);

    Assertions.assertThrows(
        IllegalStatisticNameException.class,
        () -> supportsStatistics.updateStatistics(statisticsToUpdate));
  }

  private void testDropStatistics(SupportsStatistics supportsStatistics, String path)
      throws JsonProcessingException {
    // Test successful drop
    List<String> statisticsToDrop = Arrays.asList("custom.user_stat1", "custom.user_stat2");
    // For DELETE with query params, the path should not include query params
    // buildMockResource will handle query params separately
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("statistics", "custom.user_stat1,custom.user_stat2");

    DropResponse response = new DropResponse(true);
    buildMockResource(Method.DELETE, path, queryParams, null, response, SC_OK);

    boolean dropped = supportsStatistics.dropStatistics(statisticsToDrop);
    Assertions.assertTrue(dropped);

    // Test drop non-existing statistics
    response = new DropResponse(false);
    buildMockResource(Method.DELETE, path, queryParams, null, response, SC_OK);

    dropped = supportsStatistics.dropStatistics(statisticsToDrop);
    Assertions.assertFalse(dropped);

    // Test unmodifiable statistic exception
    // Need to create ErrorResponse with proper type for UnmodifiableStatisticException
    String unmodifiableErrorJson =
        MAPPER.writeValueAsString(
            ImmutableMap.of(
                "code",
                ErrorConstants.FORBIDDEN_CODE,
                "type",
                UnmodifiableStatisticException.class.getSimpleName(),
                "message",
                "Cannot drop reserved statistic",
                "stack",
                Collections.emptyList()));
    ErrorResponse unmodifiableError = MAPPER.readValue(unmodifiableErrorJson, ErrorResponse.class);
    buildMockResource(Method.DELETE, path, queryParams, null, unmodifiableError, SC_FORBIDDEN);

    Assertions.assertThrows(
        UnmodifiableStatisticException.class,
        () -> supportsStatistics.dropStatistics(statisticsToDrop));
  }

  private String getTableStatisticsPath() {
    return String.format(
        "/api/metalakes/%s/catalogs/%s/schemas/%s/tables/%s/statistics",
        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
  }

  private String getFilesetStatisticsPath() {
    return String.format(
        "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s/statistics",
        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, FILESET_NAME);
  }
}
