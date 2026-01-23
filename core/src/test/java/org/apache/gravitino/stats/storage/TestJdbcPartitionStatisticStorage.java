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
package org.apache.gravitino.stats.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link JdbcPartitionStatisticStorage} using mocked JDBC components. */
public class TestJdbcPartitionStatisticStorage {

  private JdbcPartitionStatisticStorage storage;
  private DataSource mockDataSource;
  private Connection mockConnection;
  private PreparedStatement mockPreparedStatement;
  private ResultSet mockResultSet;
  private EntityStore mockEntityStore;

  private static final String METALAKE = "test_metalake";
  private static final MetadataObject TEST_TABLE =
      MetadataObjects.of(
          Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  private static final Long TABLE_ID = 100L;

  @BeforeEach
  public void setUp() throws Exception {
    // Mock JDBC components
    mockDataSource = mock(DataSource.class);
    mockConnection = mock(Connection.class);
    mockPreparedStatement = mock(PreparedStatement.class);
    mockResultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    // Mock EntityStore
    mockEntityStore = mock(EntityStore.class);
    TableEntity mockTableEntity = mock(TableEntity.class);
    when(mockEntityStore.get(any(), any(), any())).thenReturn(mockTableEntity);
    when(mockTableEntity.id()).thenReturn(TABLE_ID);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", mockEntityStore, true);

    // Create storage with mocked DataSource
    storage = new JdbcPartitionStatisticStorage(mockDataSource);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (storage != null) {
      storage.close();
    }
  }

  /** Helper method to serialize StatisticValue to JSON for mocking ResultSet. */
  private String toJson(StatisticValue<?> value) throws JsonProcessingException {
    return JsonUtils.anyFieldMapper().writeValueAsString(value);
  }

  @Test
  public void testUpdateStatistics() throws Exception {
    // Prepare test data
    Map<String, StatisticValue<?>> stats = new HashMap<>();
    stats.put("custom-rowCount", StatisticValues.longValue(1000L));
    stats.put("custom-sizeBytes", StatisticValues.longValue(5242880L));

    PartitionStatisticsUpdate update =
        PartitionStatisticsModification.update("partition_2024_01", stats);

    List<MetadataObjectStatisticsUpdate> objectUpdates =
        Lists.newArrayList(
            MetadataObjectStatisticsUpdate.of(TEST_TABLE, Lists.newArrayList(update)));

    // Mock successful execution
    when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

    // Execute
    storage.updateStatistics(METALAKE, objectUpdates);

    // Verify connection and commit were called
    verify(mockConnection, times(1)).commit();
    verify(mockConnection, times(1)).close();
  }

  @Test
  public void testListStatisticsAllPartitions() throws Exception {
    // Create actual JSON from StatisticValue objects
    String rowCountJson = toJson(StatisticValues.longValue(1000L));
    String sizeBytesJson = toJson(StatisticValues.longValue(5242880L));

    // Mock ResultSet to return test data
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getLong("table_id")).thenReturn(TABLE_ID);
    when(mockResultSet.getString("partition_name"))
        .thenReturn("partition_2024_01", "partition_2024_01");
    when(mockResultSet.getString("statistic_name"))
        .thenReturn("custom-rowCount", "custom-sizeBytes");
    when(mockResultSet.getString("statistic_value")).thenReturn(rowCountJson, sizeBytesJson);
    when(mockResultSet.getString("audit_info"))
        .thenReturn(
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}",
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}");

    // Execute
    List<PersistedPartitionStatistics> results =
        storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

    // Verify
    assertNotNull(results);
    assertEquals(1, results.size());
    assertEquals("partition_2024_01", results.get(0).partitionName());
    assertEquals(2, results.get(0).statistics().size());

    // Verify statistics values
    List<PersistedStatistic> statistics = results.get(0).statistics();
    boolean hasRowCount = statistics.stream().anyMatch(s -> s.name().equals("custom-rowCount"));
    boolean hasSizeBytes = statistics.stream().anyMatch(s -> s.name().equals("custom-sizeBytes"));
    assertTrue(hasRowCount);
    assertTrue(hasSizeBytes);
  }

  @Test
  public void testListStatisticsWithClosedRange() throws Exception {
    // Create actual JSON from StatisticValue object
    String rowCountJson = toJson(StatisticValues.longValue(2000L));

    // Mock ResultSet
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getLong("table_id")).thenReturn(TABLE_ID);
    when(mockResultSet.getString("partition_name")).thenReturn("partition_2024_02");
    when(mockResultSet.getString("statistic_name")).thenReturn("custom-rowCount");
    when(mockResultSet.getString("statistic_value")).thenReturn(rowCountJson);
    when(mockResultSet.getString("audit_info"))
        .thenReturn("{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}");

    // Execute with range query
    storage.listStatistics(
        METALAKE,
        TEST_TABLE,
        PartitionRange.between(
            "partition_2024_01",
            PartitionRange.BoundType.CLOSED,
            "partition_2024_03",
            PartitionRange.BoundType.CLOSED));

    // Verify query was executed with WHERE clause
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockConnection).prepareStatement(sqlCaptor.capture());
    String executedSql = sqlCaptor.getValue();
    assertTrue(executedSql.contains("WHERE"));
    assertTrue(executedSql.contains("table_id"));
  }

  @Test
  public void testDropStatistics() throws Exception {
    // Prepare drop request
    List<MetadataObjectStatisticsDrop> drops =
        Lists.newArrayList(
            MetadataObjectStatisticsDrop.of(
                TEST_TABLE,
                Lists.newArrayList(
                    PartitionStatisticsModification.drop(
                        "partition_2024_01", Lists.newArrayList("custom-rowCount")))));

    // Mock successful batch execution
    when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1});

    // Execute
    int dropped = storage.dropStatistics(METALAKE, drops);

    // Verify
    assertEquals(1, dropped);
    verify(mockPreparedStatement, times(1)).executeBatch();
  }

  @Test
  public void testTransactionRollback() throws Exception {
    // Prepare test data
    Map<String, StatisticValue<?>> stats = new HashMap<>();
    stats.put("custom-rowCount", StatisticValues.longValue(1000L));

    PartitionStatisticsUpdate update =
        PartitionStatisticsModification.update("partition_2024_01", stats);

    List<MetadataObjectStatisticsUpdate> objectUpdates =
        Lists.newArrayList(
            MetadataObjectStatisticsUpdate.of(TEST_TABLE, Lists.newArrayList(update)));

    // Mock SQLException during batch execution
    when(mockPreparedStatement.executeBatch()).thenThrow(new SQLException("Test error"));

    // Execute and expect exception
    assertThrows(Exception.class, () -> storage.updateStatistics(METALAKE, objectUpdates));

    // Verify rollback was called
    verify(mockConnection, times(1)).rollback();
  }

  @Test
  public void testMultipleStatisticTypes() throws Exception {
    // Create actual StatisticValue objects and serialize them to JSON
    String longValueJson = toJson(StatisticValues.longValue(1000L));
    String doubleValueJson = toJson(StatisticValues.doubleValue(256.5));
    String stringValueJson = toJson(StatisticValues.stringValue("parquet"));
    String booleanValueJson = toJson(StatisticValues.booleanValue(true));

    // Mock ResultSet with different statistic types
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getLong("table_id")).thenReturn(TABLE_ID);
    when(mockResultSet.getString("partition_name"))
        .thenReturn(
            "partition_2024_01", "partition_2024_01", "partition_2024_01", "partition_2024_01");
    when(mockResultSet.getString("statistic_name"))
        .thenReturn("custom-rowCount", "custom-avgSize", "custom-format", "custom-isCompressed");
    when(mockResultSet.getString("statistic_value"))
        .thenReturn(longValueJson, doubleValueJson, stringValueJson, booleanValueJson);
    when(mockResultSet.getString("audit_info"))
        .thenReturn(
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}",
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}",
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}",
            "{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}");

    // Execute
    List<PersistedPartitionStatistics> results =
        storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

    // Verify all statistic types
    assertEquals(1, results.size());
    assertEquals(4, results.get(0).statistics().size());

    List<PersistedStatistic> statistics = results.get(0).statistics();
    Map<String, Object> valueMap = new HashMap<>();
    for (PersistedStatistic stat : statistics) {
      valueMap.put(stat.name(), stat.value().value());
    }

    assertEquals(1000L, valueMap.get("custom-rowCount"));
    assertEquals(256.5, valueMap.get("custom-avgSize"));
    assertEquals("parquet", valueMap.get("custom-format"));
    assertEquals(true, valueMap.get("custom-isCompressed"));
  }

  @Test
  public void testBatchUpdate() throws Exception {
    // Prepare multiple partition updates
    Map<String, StatisticValue<?>> stats1 = new HashMap<>();
    stats1.put("custom-rowCount", StatisticValues.longValue(1000L));

    Map<String, StatisticValue<?>> stats2 = new HashMap<>();
    stats2.put("custom-rowCount", StatisticValues.longValue(2000L));

    Map<String, StatisticValue<?>> stats3 = new HashMap<>();
    stats3.put("custom-rowCount", StatisticValues.longValue(3000L));

    List<PartitionStatisticsUpdate> updates =
        Lists.newArrayList(
            PartitionStatisticsModification.update("partition_2024_01", stats1),
            PartitionStatisticsModification.update("partition_2024_02", stats2),
            PartitionStatisticsModification.update("partition_2024_03", stats3));

    List<MetadataObjectStatisticsUpdate> objectUpdates =
        Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

    // Mock successful batch execution
    when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1, 1, 1});

    // Execute
    storage.updateStatistics(METALAKE, objectUpdates);

    // Verify batch was executed
    verify(mockPreparedStatement, times(1)).executeBatch();
    verify(mockConnection, times(1)).commit();
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    // Mock empty ResultSet
    when(mockResultSet.next()).thenReturn(false);

    // Execute
    List<PersistedPartitionStatistics> results =
        storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

    // Verify empty list returned
    assertNotNull(results);
    assertTrue(results.isEmpty());
  }
}
