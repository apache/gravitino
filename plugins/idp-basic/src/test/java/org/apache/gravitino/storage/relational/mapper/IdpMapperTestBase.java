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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

abstract class IdpMapperTestBase {
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_idpMappers_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";

  protected static JDBCBackend backend;
  protected static SqlSession sharedSession;
  protected static IdpUserMetaMapper idpUserMetaMapper;
  protected static IdpGroupMetaMapper idpGroupMetaMapper;
  protected static IdpGroupUserRelMapper idpGroupUserRelMapper;

  @BeforeAll
  static void setup() throws Exception {
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      deleteDirectory(dir.toPath());
    }
    dir.mkdirs();

    Config config = new Config(false) {};
    config.set(Configs.ENTITY_STORE, "relational");
    config.set(Configs.ENTITY_RELATIONAL_STORE, "h2");
    config.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "gravitino");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "gravitino");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 20);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

    backend = new JDBCBackend();
    backend.initialize(config);

    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    idpGroupUserRelMapper = sharedSession.getMapper(IdpGroupUserRelMapper.class);
  }

  @AfterAll
  static void tearDown() throws IOException {
    if (sharedSession != null) {
      sharedSession.close();
    }
    if (backend != null) {
      backend.close();
    }
    File dir = new File(JDBC_STORE_PATH);
    if (dir.exists()) {
      deleteDirectory(dir.toPath());
    }
  }

  @BeforeEach
  void truncateTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("TRUNCATE TABLE idp_group_user_rel");
        statement.execute("TRUNCATE TABLE idp_group_meta");
        statement.execute("TRUNCATE TABLE idp_user_meta");
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

  protected IdpGroupPO insertGroup(
      long groupId, String groupName, long currentVersion, long lastVersion, long deletedAt) {
    IdpGroupPO groupPO =
        IdpGroupPO.builder()
            .withGroupId(groupId)
            .withGroupName(groupName)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpGroupMetaMapper.insertIdpGroup(groupPO);
    return groupPO;
  }

  protected IdpGroupUserRelPO insertRelation(
      long id, long groupId, long userId, long currentVersion, long lastVersion, long deletedAt) {
    IdpGroupUserRelPO relationPO =
        IdpGroupUserRelPO.builder()
            .withId(id)
            .withGroupId(groupId)
            .withUserId(userId)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpGroupUserRelMapper.batchInsertIdpGroupUsers(List.of(relationPO));
    return relationPO;
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

  private static void deleteDirectory(Path dir) throws IOException {
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
