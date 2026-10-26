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

package org.apache.gravitino.storage.relational;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;

/**
 * Empties the live backend's tables before a test, as its {@link ResetStrategy} asks. {@link
 * ResetStrategy#FRESH_DATABASE} has nothing to empty, so it is rejected.
 */
final class DatabaseResetter {

  private static final int RESET_TIMEOUT_SECONDS = 30;

  private DatabaseResetter() {}

  static void reset(String backendType, ResetStrategy resetStrategy) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = sqlSession.getConnection().createStatement()) {
      // A lock left by an earlier test would block the reset forever (PostgreSQL has no default
      // lock timeout); fail this test instead.
      statement.setQueryTimeout(RESET_TIMEOUT_SECONDS);
      try {
        List<String> allTables = tables(backendType, statement);
        switch (resetStrategy) {
          case DELETE_ROWS:
            deleteRows(sqlSession.getConnection(), statement, nonEmptyTables(statement, allTables));
            break;
          case TRUNCATE_TABLES:
            // Every table, not only non-empty ones: an earlier DELETE_ROWS test may have emptied a
            // table but left its counter running.
            truncate(backendType, statement, allTables);
            break;
          default:
            throw new IllegalArgumentException("Nothing to reset for " + resetStrategy);
        }
      } finally {
        // H2 applies a statement's timeout to its whole session, and the pool keeps the
        // connection, so the test that runs next would inherit it.
        statement.setQueryTimeout(0);
      }
    }
  }

  private static void deleteRows(Connection connection, Statement statement, List<String> tables)
      throws SQLException {
    if (tables.isEmpty()) {
      return;
    }
    // One transaction, so the deletes share one commit instead of one fsync per table.
    connection.setAutoCommit(false);
    try {
      for (String table : tables) {
        statement.execute("DELETE FROM " + table);
      }
      connection.commit();
    } catch (SQLException e) {
      connection.rollback();
      throw e;
    } finally {
      connection.setAutoCommit(true);
    }
  }

  private static void truncate(String backendType, Statement statement, List<String> tables)
      throws SQLException {
    if (tables.isEmpty()) {
      return;
    }
    if ("postgresql".equalsIgnoreCase(backendType)) {
      statement.execute(
          tables.stream()
              .map(table -> "TRUNCATE TABLE " + table + " RESTART IDENTITY CASCADE;")
              .collect(Collectors.joining("\n", "DO $$ BEGIN\n", "\nEND $$;")));
      return;
    }
    // H2 keeps identity counters on TRUNCATE unless asked; MySQL always restarts them.
    String restart = "h2".equalsIgnoreCase(backendType) ? " RESTART IDENTITY" : "";
    for (String table : tables) {
      statement.execute("TRUNCATE TABLE " + table + restart);
    }
  }

  private static List<String> tables(String backendType, Statement statement) throws SQLException {
    String query =
        "postgresql".equalsIgnoreCase(backendType)
            ? "SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
            : "SHOW TABLES";
    List<String> tables = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        tables.add(rs.getString(1));
      }
    }
    return tables;
  }

  // One query finds the tables a test wrote to, so DELETE_ROWS issues one DELETE per dirty table
  // rather than one per table.
  private static List<String> nonEmptyTables(Statement statement, List<String> tables)
      throws SQLException {
    List<String> dirtyTables = new ArrayList<>();
    if (tables.isEmpty()) {
      return dirtyTables;
    }
    // Table names come from the database's own catalog, not from input, so formatting them in is
    // safe.
    String query =
        tables.stream()
            .map(table -> String.format("(SELECT '%s' FROM %s LIMIT 1)", table, table))
            .collect(Collectors.joining(" UNION ALL "));
    try (ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        dirtyTables.add(rs.getString(1));
      }
    }
    return dirtyTables;
  }
}
