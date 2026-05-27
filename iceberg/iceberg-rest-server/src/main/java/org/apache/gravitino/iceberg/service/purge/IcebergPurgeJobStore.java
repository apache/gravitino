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

package org.apache.gravitino.iceberg.service.purge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;

/** JDBC persistence for {@code iceberg_cleanup_job}. */
public class IcebergPurgeJobStore {

  private static final String INSERT_SQL =
      "INSERT INTO iceberg_cleanup_job (metalake_name, catalog_name, namespace, table_name,"
          + " metadata_location, file_io_impl, file_io_props, state, attempts, created_by,"
          + " updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)";

  private static final String SCAN_SQL =
      "SELECT id FROM iceberg_cleanup_job WHERE state = 'PENDING'"
          + " OR (state = 'RUNNING' AND (heartbeat_at IS NULL OR heartbeat_at < ?))"
          + " ORDER BY updated_at LIMIT ?";

  private static final String CLAIM_SQL =
      "UPDATE iceberg_cleanup_job SET state = 'RUNNING', heartbeat_at = ?, updated_at = ?"
          + " WHERE id = ? AND (state = 'PENDING'"
          + " OR (state = 'RUNNING' AND (heartbeat_at IS NULL OR heartbeat_at < ?)))";

  private static final String SELECT_SQL = "SELECT * FROM iceberg_cleanup_job WHERE id = ?";

  private final JdbcClientPool connections;

  /**
   * Creates a purge job store.
   *
   * @param connections JDBC pool over the Gravitino backend database
   */
  public IcebergPurgeJobStore(JdbcClientPool connections) {
    this.connections = connections;
  }

  /**
   * Persists a new PENDING job.
   *
   * @param job job to persist
   * @return generated id
   */
  public long enqueue(IcebergPurgeJob job) {
    long now = System.currentTimeMillis();
    String props = writeProps(job.fileIoProperties());
    return run(
        conn -> {
          try (PreparedStatement ps =
              conn.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, job.metalakeName());
            ps.setString(2, job.catalogName());
            ps.setString(3, job.namespace());
            ps.setString(4, job.tableName());
            ps.setString(5, job.metadataLocation());
            ps.setString(6, job.fileIoImpl());
            ps.setString(7, props);
            ps.setString(8, job.createdBy());
            ps.setLong(9, now);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
              keys.next();
              return keys.getLong(1);
            }
          }
        });
  }

  /**
   * Scans a small candidate window and claims the first winnable row via compare-and-swap.
   *
   * @param now current epoch millis, written as the claim heartbeat
   * @param heartbeatTimeoutMs age past which a RUNNING heartbeat is stale
   * @param window max candidates to consider
   * @return the claimed job, or {@code null} if nothing was claimable
   */
  public IcebergPurgeJob claimNext(long now, long heartbeatTimeoutMs, int window) {
    long staleBefore = now - heartbeatTimeoutMs;
    return run(
        conn -> {
          List<Long> ids = new ArrayList<>();
          try (PreparedStatement ps = conn.prepareStatement(SCAN_SQL)) {
            ps.setLong(1, staleBefore);
            ps.setInt(2, window);
            try (ResultSet rs = ps.executeQuery()) {
              while (rs.next()) {
                ids.add(rs.getLong(1));
              }
            }
          }

          for (long id : ids) {
            try (PreparedStatement ps = conn.prepareStatement(CLAIM_SQL)) {
              ps.setLong(1, now);
              ps.setLong(2, now);
              ps.setLong(3, id);
              ps.setLong(4, staleBefore);
              if (ps.executeUpdate() == 1) {
                return readJob(conn, id);
              }
            }
          }
          return null;
        });
  }

  /**
   * Reads a job state for tests.
   *
   * @param id job id
   * @return its current state
   * @throws IllegalStateException if the row is gone
   */
  @VisibleForTesting
  public IcebergPurgeJob.State stateOf(long id) {
    return run(
        conn -> {
          try (PreparedStatement ps =
              conn.prepareStatement("SELECT state FROM iceberg_cleanup_job WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
              if (!rs.next()) {
                throw new IllegalStateException("No purge job " + id);
              }
              return IcebergPurgeJob.State.valueOf(rs.getString(1));
            }
          }
        });
  }

  private static IcebergPurgeJob readJob(Connection conn, long id) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
      ps.setLong(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return new IcebergPurgeJob(
            rs.getLong("id"),
            rs.getString("metalake_name"),
            rs.getString("catalog_name"),
            rs.getString("namespace"),
            rs.getString("table_name"),
            rs.getString("metadata_location"),
            rs.getString("file_io_impl"),
            readProps(rs.getString("file_io_props")),
            rs.getString("created_by"));
      }
    }
  }

  private static String writeProps(Map<String, String> props) {
    try {
      return JsonUtils.objectMapper().writeValueAsString(props);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize fileIoProperties", e);
    }
  }

  private static Map<String, String> readProps(String json) {
    try {
      return JsonUtils.objectMapper().readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize fileIoProperties", e);
    }
  }

  <R> R run(Call<R> call) {
    try {
      return connections.run(call::apply);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed purge-store SQL");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted in purge-store SQL");
    }
  }

  /** Matches {@code JdbcClientPool.Action}. */
  @FunctionalInterface
  interface Call<R> {
    R apply(Connection conn) throws SQLException;
  }
}
