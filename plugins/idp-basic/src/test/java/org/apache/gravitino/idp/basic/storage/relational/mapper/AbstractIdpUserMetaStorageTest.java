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

package org.apache.gravitino.idp.basic.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.idp.basic.storage.relational.po.IdpUserPO;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;

abstract class AbstractIdpUserMetaStorageTest {
  protected String backendType;
  protected JDBCBackend backend;
  protected SqlSession sharedSession;
  protected IdpUserMetaMapper idpUserMetaMapper;

  private Config config;
  private Path h2Path;

  static Stream<String> storageProvider() {
    return Stream.of("h2", "mysql", "postgresql");
  }

  @AfterEach
  void closeSuite() throws IOException {
    closeSession();
    if (backend != null) {
      backend.close();
      backend = null;
    }

    SqlSessionFactoryHelper.getInstance().close();
    ContainerSuite.getInstance().close();

    if (h2Path != null && Files.exists(h2Path)) {
      deleteDirectory(h2Path);
      h2Path = null;
    }
  }

  protected void init(String type) throws IOException {
    backendType = type;
    config = createBackendConfig(type);
    backend = new JDBCBackend();
    backend.close();
    backend.initialize(config);
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
  }

  protected void restartBackend() throws IOException {
    closeSession();
    backend.close();
    backend = new JDBCBackend();
    backend.initialize(config);
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
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

  protected void closeSession() {
    if (sharedSession != null) {
      sharedSession.close();
      sharedSession = null;
    }
  }

  private Config createBackendConfig(String type) throws IOException {
    BaseIT baseIT = new BaseIT();
    Config backendConfig = new Config(false) {};
    backendConfig.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    backendConfig.set(Configs.ENTITY_RELATIONAL_STORE, type);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 20);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

    if ("mysql".equals(type)) {
      backendConfig.set(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitMySQLBackend());
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "com.mysql.cj.jdbc.Driver");
    } else if ("postgresql".equals(type)) {
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, baseIT.startAndInitPGBackend());
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "root");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.postgresql.Driver");
    } else {
      h2Path = Files.createTempDirectory("gravitino_idp_basic_h2_");
      backendConfig.set(
          Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
          String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
      backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    }

    return backendConfig;
  }

  private void deleteDirectory(Path directory) throws IOException {
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(this::deletePath);
    }
  }

  private void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Delete path failed: " + path, e);
    }
  }
}
