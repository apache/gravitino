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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.apache.gravitino.integration.test.util.PrintFuncNameExtension;
import org.apache.gravitino.storage.relational.mapper.it.BackendTestExtension;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ResourceLock("idp-basic-sql-session-factory")
@ExtendWith({
  BackendTestExtension.class,
  PrintFuncNameExtension.class,
  CloseContainerExtension.class
})
public abstract class TestJDBCBackend {
  protected String backendType;
  protected JDBCBackend backend;

  public void setBackendType(String backendType) {
    this.backendType = backendType;
  }

  public void setBackend(JDBCBackend backend) {
    this.backend = backend;
  }

  @BeforeEach
  public void init() throws SQLException {
    truncateAllTables();
  }

  protected long queryLongValue(String table, String column, String idColumn, long idValue) {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        String query = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
          statement.setLong(1, idValue);
          try (ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            return resultSet.getLong(1);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Query " + column + " from " + table + " failed", e);
    }
  }

  protected int countRows(String table, String idColumn, long idValue) {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        String query = "SELECT COUNT(1) FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
          statement.setLong(1, idValue);
          try (ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Count rows from " + table + " failed", e);
    }
  }

  protected String currentJdbcUrl() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getURL();
      }
    } catch (SQLException e) {
      throw new RuntimeException("Get current JDBC URL failed", e);
    }
  }

  private void truncateAllTables() throws SQLException {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection();
          Statement statement = connection.createStatement()) {
        if ("postgresql".equalsIgnoreCase(backendType)) {
          truncateAllTablesForPostgreSQL(connection);
        } else {
          List<String> tableList = new ArrayList<>();
          try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
              tableList.add(rs.getString(1));
            }
          }
          for (String table : tableList) {
            statement.execute("TRUNCATE TABLE " + table);
          }
        }
      }
    }
  }

  private void truncateAllTablesForPostgreSQL(Connection connection) throws SQLException {
    List<String> tableList = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      String query =
          "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()";
      try (ResultSet rs = statement.executeQuery(query)) {
        while (rs.next()) {
          tableList.add(rs.getString(1));
        }
      }

      if (tableList.isEmpty()) {
        return;
      }

      String truncateCommand =
          String.format("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", String.join(", ", tableList));
      statement.execute(truncateCommand);
    }
  }
}
