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

package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.apache.gravitino.integration.test.util.PrintFuncNameExtension;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({PrintFuncNameExtension.class, CloseContainerExtension.class})
abstract class IdpMapperTestBase {
  private final BaseIT baseIT = new BaseIT();
  private Path h2Path;

  protected String backendType;
  protected JDBCBackend backend;
  protected SqlSession sharedSession;
  protected IdpUserMetaMapper idpUserMetaMapper;

  @BeforeAll
  void startBackend() throws SQLException {
    backendType = backendType();
    backend = createBackend(backendType);
  }

  @BeforeEach
  void openSession() {
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    truncateTables();
  }

  @AfterEach
  void closeSession() {
    if (sharedSession != null) {
      sharedSession.close();
      sharedSession = null;
    }
  }

  @AfterAll
  void stopBackend() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
    if (backend != null) {
      backend.close();
      backend = null;
    }

    if (h2Path != null && Files.exists(h2Path)) {
      deleteDirectory(h2Path);
      h2Path = null;
    }
  }

  void truncateTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection();
          Statement statement = connection.createStatement()) {
        if ("postgresql".equalsIgnoreCase(backendType)) {
          statement.execute("TRUNCATE TABLE idp_user_meta RESTART IDENTITY CASCADE");
        } else {
          statement.execute("TRUNCATE TABLE idp_user_meta");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Truncate table failed", e);
    }
  }

  protected IdpUserPO insertUser(
      long userId,
      String userName,
      String passwordHash,
      long currentVersion,
      long lastVersion,
      long deletedAt) {
    IdpUserPO userPO =
        IdpUserPO.builder()
            .withUserId(userId)
            .withUserName(userName)
            .withPasswordHash(passwordHash)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpUserMetaMapper.insertIdpUser(userPO);
    return userPO;
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

  protected abstract String backendType();

  private JDBCBackend createBackend(String backendType) throws SQLException {
    Config config = new Config(false) {};
    config.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    config.set(Configs.ENTITY_RELATIONAL_STORE, backendType);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 20);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

    if ("mysql".equals(backendType)) {
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitMySQLBackend());
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "com.mysql.cj.jdbc.Driver");
    } else if ("postgresql".equals(backendType)) {
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitPGBackend());
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.postgresql.Driver");
    } else {
      String jdbcStorePath =
          "/tmp/gravitino_jdbc_idpMappers_" + UUID.randomUUID().toString().replace("-", "");
      h2Path = Path.of(jdbcStorePath);
      try {
        Files.createDirectories(h2Path);
      } catch (IOException e) {
        throw new RuntimeException("Create H2 test directory failed: " + h2Path, e);
      }

      config.set(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
          String.format("jdbc:h2:file:%s/testdb;DB_CLOSE_DELAY=-1;MODE=MYSQL", jdbcStorePath));
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    }

    JDBCBackend jdbcBackend = new JDBCBackend();
    jdbcBackend.initialize(config);
    return jdbcBackend;
  }

  private void deleteDirectory(Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new RuntimeException("Delete path failed: " + path, e);
                }
              });
    }
  }
}
