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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Checks that {@link ResetStrategy#DELETE_ROWS} and {@link ResetStrategy#TRUNCATE_TABLES} empty the
 * tables a test wrote to and leave counters as their Javadoc says; {@code TestBackendTestExtension}
 * covers {@link ResetStrategy#FRESH_DATABASE}.
 */
public class TestResetStrategy extends TestJDBCBackend {

  @TestTemplate
  void testDeleteRowsEmptiesEveryWrittenTable() throws SQLException {
    insertChangeLogRow();
    execute(
        "INSERT INTO table_metrics (table_identifier, metric_name, metric_ts, metric_value)"
            + " VALUES ('table', 'metric', 0, 'value')");
    DatabaseResetter.reset(backendType, ResetStrategy.DELETE_ROWS);
    Assertions.assertEquals(0L, rowCount("entity_change_log"));
    Assertions.assertEquals(0L, rowCount("table_metrics"));
  }

  @TestTemplate
  void testDeleteRowsKeepsCounting() throws SQLException {
    long before = insertChangeLogRow();
    DatabaseResetter.reset(backendType, ResetStrategy.DELETE_ROWS);
    Assertions.assertTrue(insertChangeLogRow() > before);
  }

  @TestTemplate
  void testTruncateTablesRestartsCounters() throws SQLException {
    insertChangeLogRow();
    DatabaseResetter.reset(backendType, ResetStrategy.TRUNCATE_TABLES);
    Assertions.assertEquals(1L, insertChangeLogRow());
  }

  private static void execute(String sql) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = sqlSession.getConnection().createStatement()) {
      statement.executeUpdate(sql);
    }
  }

  private static long rowCount(String table) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = sqlSession.getConnection().createStatement();
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
      Assertions.assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  private static long insertChangeLogRow() throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Statement statement = sqlSession.getConnection().createStatement()) {
      statement.executeUpdate(
          "INSERT INTO entity_change_log"
              + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
              + " VALUES ('metalake', 'TABLE', 'table', 3, 0)",
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = statement.getGeneratedKeys()) {
        Assertions.assertTrue(keys.next());
        return keys.getLong(1);
      }
    }
  }
}
