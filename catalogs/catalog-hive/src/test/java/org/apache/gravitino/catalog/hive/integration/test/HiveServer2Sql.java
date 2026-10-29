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
package org.apache.gravitino.catalog.hive.integration.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.integration.test.container.HiveContainer;

/**
 * Runs HiveQL through the HiveServer2 that already runs in the Hive container, over one JDBC
 * connection opened on first use.
 *
 * <p>Each {@code hive -e} call through {@code docker exec} boots a CLI JVM with its own embedded
 * metastore, which costs seconds per statement; a statement over JDBC does not.
 */
final class HiveServer2Sql implements AutoCloseable {

  private final HiveContainer hive;
  private Connection connection;

  HiveServer2Sql(HiveContainer hive) {
    this.hive = hive;
  }

  /**
   * Runs one statement and returns its rows the way {@code hive -S -e} prints them: one row per
   * line, columns separated by tabs, nulls as empty strings. Statements without a result set return
   * an empty string. Any error fails the call.
   */
  String run(String sql) {
    try (Statement statement = connection().createStatement()) {
      if (!statement.execute(sql)) {
        return "";
      }
      try (ResultSet resultSet = statement.getResultSet()) {
        return format(resultSet);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to run Hive SQL: " + sql, e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  private Connection connection() throws SQLException {
    if (connection == null) {
      // HiveContainer waits for HiveServer2's port and lets it impersonate the container's Hadoop
      // user, the user `docker exec hive -e` ran as, so HDFS ownership stays the same.
      String url =
          String.format(
              "jdbc:hive2://%s:%d/default",
              hive.getContainerIpAddress(), HiveContainer.HIVE_SERVICE_PORT);
      connection = DriverManager.getConnection(url, hive.getHadoopUser(), "");
    }
    return connection;
  }

  private static String format(ResultSet resultSet) throws SQLException {
    int columns = resultSet.getMetaData().getColumnCount();
    List<String> rows = new ArrayList<>();
    while (resultSet.next()) {
      List<String> values = new ArrayList<>(columns);
      for (int i = 1; i <= columns; i++) {
        String value = resultSet.getString(i);
        values.add(value == null ? "" : value);
      }
      rows.add(String.join("\t", values));
    }
    return String.join("\n", rows);
  }
}
