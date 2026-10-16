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

/** Empties the live backend's tables before a test, as its {@link ResetStrategy} asks. */
final class DatabaseResetter {

  private static final int RESET_TIMEOUT_SECONDS = 30;

  private DatabaseResetter() {}

  static void reset(String backendType, ResetStrategy resetStrategy) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      // A lock left by an earlier test would block the reset forever (PostgreSQL has no default
      // lock timeout); fail this test instead.
      statement.setQueryTimeout(RESET_TIMEOUT_SECONDS);
      List<String> tables = nonEmptyTables(statement, tables(backendType, statement));
      if (tables.isEmpty()) {
        return;
      }
      if (resetStrategy != ResetStrategy.TRUNCATE_TABLES) {
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
      } else if ("postgresql".equalsIgnoreCase(backendType)) {
        statement.execute(
            tables.stream()
                .map(table -> "TRUNCATE TABLE " + table + " RESTART IDENTITY CASCADE;")
                .collect(Collectors.joining("\n", "DO $$ BEGIN\n", "\nEND $$;")));
      } else {
        // H2 keeps identity counters on TRUNCATE unless asked; MySQL always restarts them.
        String restart = "h2".equalsIgnoreCase(backendType) ? " RESTART IDENTITY" : "";
        for (String table : tables) {
          statement.execute("TRUNCATE TABLE " + table + restart);
        }
      }
    }
  }

  private static List<String> tables(String backendType, Statement statement) throws SQLException {
    String query =
        "postgresql".equalsIgnoreCase(backendType)
            ? "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
            : "SHOW TABLES";
    List<String> tables = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        tables.add(rs.getString(1));
      }
    }
    return tables;
  }

  // Resetting every table before every test dominated the database lanes: TRUNCATE recreates each
  // table's files and syncs them to disk, even when the table is empty. One query finds the few
  // tables a test wrote to, so only those are reset.
  private static List<String> nonEmptyTables(Statement statement, List<String> tables)
      throws SQLException {
    List<String> nonEmpty = new ArrayList<>();
    if (tables.isEmpty()) {
      return nonEmpty;
    }
    String query =
        tables.stream()
            .map(table -> String.format("(SELECT '%s' FROM %s LIMIT 1)", table, table))
            .collect(Collectors.joining(" UNION ALL "));
    try (ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        nonEmpty.add(rs.getString(1));
      }
    }
    return nonEmpty;
  }
}
