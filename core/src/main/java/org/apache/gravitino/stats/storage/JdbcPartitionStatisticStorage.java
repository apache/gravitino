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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-based implementation of {@link PartitionStatisticStorage}.
 *
 * <p>This implementation stores partition statistics in a JDBC-compatible database table, using
 * Apache Commons DBCP2 for connection pooling. It supports multiple database backends (MySQL,
 * PostgreSQL, H2). Statistics are stored as JSON-serialized values along with audit information.
 */
public class JdbcPartitionStatisticStorage implements PartitionStatisticStorage {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPartitionStatisticStorage.class);

  private final DataSource dataSource;
  private final EntityStore entityStore;

  // SQL statements
  private static final String INSERT_OR_UPDATE_SQL =
      "INSERT INTO partition_statistic_meta "
          + "(table_id, partition_name, statistic_name, statistic_value, audit_info, created_at, updated_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?) "
          + "ON DUPLICATE KEY UPDATE "
          + "statistic_value = VALUES(statistic_value), "
          + "audit_info = VALUES(audit_info), "
          + "updated_at = VALUES(updated_at)";

  private static final String SELECT_STATISTICS_SQL =
      "SELECT partition_name, statistic_name, statistic_value, audit_info "
          + "FROM partition_statistic_meta "
          + "WHERE table_id = ? ";

  private static final String DELETE_STATISTICS_SQL =
      "DELETE FROM partition_statistic_meta "
          + "WHERE table_id = ? AND partition_name = ? AND statistic_name = ?";

  /**
   * Constructs a new JdbcPartitionStatisticStorage.
   *
   * @param dataSource the JDBC DataSource for database connections
   */
  public JdbcPartitionStatisticStorage(DataSource dataSource) {
    this.dataSource = dataSource;
    this.entityStore = GravitinoEnv.getInstance().entityStore();
  }

  @Override
  public List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange partitionRange)
      throws IOException {
    LOG.debug(
        "Listing statistics for metalake: {}, object: {}, range: {}",
        metalake,
        metadataObject.fullName(),
        partitionRange);

    Long tableId = resolveTableId(metalake, metadataObject);
    String rangeFilter = buildPartitionRangeFilter(partitionRange);

    String sql = SELECT_STATISTICS_SQL + rangeFilter + " ORDER BY partition_name, statistic_name";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {

      stmt.setLong(1, tableId);

      // Set partition range parameters
      setPartitionRangeParameters(stmt, partitionRange, 2);

      try (ResultSet rs = stmt.executeQuery()) {
        return parseResultSet(rs);
      }

    } catch (SQLException e) {
      throw new IOException("Failed to list statistics for " + metadataObject.fullName(), e);
    }
  }

  @Override
  public List<PersistedPartitionStatistics> listStatistics(
      String metalake, MetadataObject metadataObject, List<String> partitionNames)
      throws IOException {
    LOG.debug(
        "Listing statistics for metalake: {}, object: {}, partitions: {}",
        metalake,
        metadataObject.fullName(),
        partitionNames);

    if (partitionNames.isEmpty()) {
      return Lists.newArrayList();
    }

    Long tableId = resolveTableId(metalake, metadataObject);

    // Build IN clause
    String inClause =
        partitionNames.stream().map(p -> "?").collect(Collectors.joining(", ", "(", ")"));
    String sql =
        SELECT_STATISTICS_SQL
            + "AND partition_name IN "
            + inClause
            + " ORDER BY partition_name, statistic_name";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {

      stmt.setLong(1, tableId);

      int paramIndex = 2;
      for (String partitionName : partitionNames) {
        stmt.setString(paramIndex++, partitionName);
      }

      try (ResultSet rs = stmt.executeQuery()) {
        return parseResultSet(rs);
      }

    } catch (SQLException e) {
      throw new IOException(
          "Failed to list statistics for partitions "
              + partitionNames
              + " in "
              + metadataObject.fullName(),
          e);
    }
  }

  @Override
  public int dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> statisticsToDrop)
      throws IOException {
    LOG.debug(
        "Dropping statistics for metalake: {}, {} objects", metalake, statisticsToDrop.size());

    int totalDropped = 0;

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try (PreparedStatement stmt = conn.prepareStatement(DELETE_STATISTICS_SQL)) {
        for (MetadataObjectStatisticsDrop objectDrop : statisticsToDrop) {
          Long tableId = resolveTableId(metalake, objectDrop.metadataObject());

          for (PartitionStatisticsDrop drop : objectDrop.drops()) {
            String partitionName = drop.partitionName();
            for (String statisticName : drop.statisticNames()) {
              stmt.setLong(1, tableId);
              stmt.setString(2, partitionName);
              stmt.setString(3, statisticName);
              stmt.addBatch();
            }
          }
        }

        int[] results = stmt.executeBatch();
        for (int result : results) {
          // Count successful deletions. Per JDBC spec, executeBatch() returns:
          // - Positive number: actual update count
          // - SUCCESS_NO_INFO (-2): operation succeeded but driver doesn't know row count
          // - EXECUTE_FAILED (-3): operation failed (we don't count this)
          if (result > 0 || result == PreparedStatement.SUCCESS_NO_INFO) {
            totalDropped++;
          }
        }

        conn.commit();
        LOG.debug("Successfully dropped {} statistics", totalDropped);

      } catch (Exception e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }

    } catch (SQLException e) {
      throw new IOException("Failed to drop statistics", e);
    }

    return totalDropped;
  }

  @Override
  public void updateStatistics(
      String metalake, List<MetadataObjectStatisticsUpdate> statisticsToUpdate) throws IOException {
    LOG.debug(
        "Updating statistics for metalake: {}, {} objects", metalake, statisticsToUpdate.size());

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);

      try (PreparedStatement stmt = conn.prepareStatement(INSERT_OR_UPDATE_SQL)) {
        for (MetadataObjectStatisticsUpdate objectUpdate : statisticsToUpdate) {
          Long tableId = resolveTableId(metalake, objectUpdate.metadataObject());

          for (PartitionStatisticsUpdate update : objectUpdate.partitionUpdates()) {
            String partitionName = update.partitionName();

            for (Map.Entry<String, StatisticValue<?>> stat : update.statistics().entrySet()) {
              String statisticName = stat.getKey();
              StatisticValue<?> statisticValue = stat.getValue();

              // Create audit info
              String currentUser = PrincipalUtils.getCurrentUserName();
              Instant now = Instant.now();
              AuditInfo auditInfo =
                  AuditInfo.builder()
                      .withCreator(currentUser)
                      .withCreateTime(now)
                      .withLastModifier(currentUser)
                      .withLastModifiedTime(now)
                      .build();

              // Serialize to JSON
              String statisticValueJson =
                  JsonUtils.anyFieldMapper().writeValueAsString(statisticValue);
              String auditInfoJson = JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);

              long timestamp = now.toEpochMilli();

              stmt.setLong(1, tableId);
              stmt.setString(2, partitionName);
              stmt.setString(3, statisticName);
              stmt.setString(4, statisticValueJson);
              stmt.setString(5, auditInfoJson);
              stmt.setLong(6, timestamp);
              stmt.setLong(7, timestamp);

              stmt.addBatch();
            }
          }
        }

        stmt.executeBatch();
        conn.commit();
        LOG.debug("Successfully updated statistics");

      } catch (Exception e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }

    } catch (SQLException | JsonProcessingException e) {
      throw new IOException("Failed to update statistics", e);
    }
  }

  @Override
  public void close() throws IOException {
    // DataSource lifecycle is managed externally by the factory
    LOG.debug("Closing JdbcPartitionStatisticStorage");
  }

  /**
   * Resolves the table ID for a given metadata object.
   *
   * @param metalake the metalake name
   * @param metadataObject the metadata object
   * @return the table ID
   * @throws IOException if unable to resolve the table ID
   */
  private Long resolveTableId(String metalake, MetadataObject metadataObject) throws IOException {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType type = MetadataObjectUtil.toEntityType(metadataObject);

    TableEntity tableEntity = entityStore.get(identifier, type, TableEntity.class);
    return tableEntity.id();
  }

  /**
   * Builds the SQL filter clause for partition range.
   *
   * @param range the partition range
   * @return the SQL filter clause
   */
  private String buildPartitionRangeFilter(PartitionRange range) {
    StringBuilder filter = new StringBuilder();

    range
        .lowerPartitionName()
        .ifPresent(
            name -> {
              String op =
                  range
                      .lowerBoundType()
                      .map(t -> t == PartitionRange.BoundType.CLOSED ? ">=" : ">")
                      .orElse(">=");
              filter.append("AND partition_name ").append(op).append(" ? ");
            });

    range
        .upperPartitionName()
        .ifPresent(
            name -> {
              String op =
                  range
                      .upperBoundType()
                      .map(t -> t == PartitionRange.BoundType.CLOSED ? "<=" : "<")
                      .orElse("<=");
              filter.append("AND partition_name ").append(op).append(" ? ");
            });

    return filter.toString();
  }

  /**
   * Sets partition range parameters in the prepared statement.
   *
   * @param stmt the prepared statement
   * @param range the partition range
   * @param startIndex the starting parameter index
   * @throws SQLException if setting parameters fails
   */
  private void setPartitionRangeParameters(
      PreparedStatement stmt, PartitionRange range, int startIndex) throws SQLException {
    final int[] paramIndex = {startIndex};

    range
        .lowerPartitionName()
        .ifPresent(
            name -> {
              try {
                stmt.setString(paramIndex[0]++, name);
              } catch (SQLException e) {
                throw new RuntimeException("Failed to set lower partition name parameter", e);
              }
            });

    range
        .upperPartitionName()
        .ifPresent(
            name -> {
              try {
                stmt.setString(paramIndex[0]++, name);
              } catch (SQLException e) {
                throw new RuntimeException("Failed to set upper partition name parameter", e);
              }
            });
  }

  /**
   * Parses a ResultSet into a list of PersistedPartitionStatistics.
   *
   * @param rs the result set
   * @return list of persisted partition statistics
   * @throws SQLException if reading from result set fails
   */
  private List<PersistedPartitionStatistics> parseResultSet(ResultSet rs) throws SQLException {
    Map<String, List<PersistedStatistic>> partitionMap = new HashMap<>();

    while (rs.next()) {
      String partitionName = rs.getString("partition_name");
      String statisticName = rs.getString("statistic_name");
      String statisticValueJson = rs.getString("statistic_value");
      String auditInfoJson = rs.getString("audit_info");

      try {
        StatisticValue<?> statisticValue =
            JsonUtils.anyFieldMapper().readValue(statisticValueJson, StatisticValue.class);
        AuditInfo auditInfo = JsonUtils.anyFieldMapper().readValue(auditInfoJson, AuditInfo.class);

        PersistedStatistic persistedStatistic =
            PersistedStatistic.of(statisticName, statisticValue, auditInfo);

        partitionMap.computeIfAbsent(partitionName, k -> new ArrayList<>()).add(persistedStatistic);

      } catch (JsonProcessingException e) {
        LOG.error(
            "Failed to parse statistic JSON for partition: {}, statistic: {}",
            partitionName,
            statisticName,
            e);
        throw new SQLException("Failed to parse statistic JSON", e);
      }
    }

    return partitionMap.entrySet().stream()
        .map(entry -> PersistedPartitionStatistics.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }
}
