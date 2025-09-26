/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.relational.mapper;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestUserRoleRelMapper {

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static JDBCBackend backend;
  private static UserRoleRelMapper userRoleRelMapper;
  private static UserMetaMapper userMetaMapper;
  private static RoleMetaMapper roleMetaMapper;
  private static MetalakeMetaMapper metalakeMetaMapper;

  @BeforeAll
  public static void setup() throws Exception {
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn("relational");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE)).thenReturn("h2");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD))
        .thenReturn("gravitino");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("org.h2.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(20);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
        .thenReturn(1000L);

    backend = new JDBCBackend();
    backend.initialize(config);

    SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    userRoleRelMapper = sqlSession.getMapper(UserRoleRelMapper.class);
    userMetaMapper = sqlSession.getMapper(UserMetaMapper.class);
    roleMetaMapper = sqlSession.getMapper(RoleMetaMapper.class);
    metalakeMetaMapper = sqlSession.getMapper(MetalakeMetaMapper.class);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (backend != null) {
      backend.close();
    }
    File dir = new File(JDBC_STORE_PATH);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
  }

  @BeforeEach
  public void init() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("TRUNCATE TABLE user_role_rel");
          statement.execute("TRUNCATE TABLE user_meta");
          statement.execute("TRUNCATE TABLE role_meta");
          statement.execute("TRUNCATE TABLE metalake_meta");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Truncate table failed", e);
    }
  }

  private int countUserRoleRelsByUserId(Long userId) {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          String query = "SELECT count(*) FROM user_role_rel WHERE user_id = " + userId;
          try (ResultSet rs = statement.executeQuery(query)) {
            if (rs.next()) {
              return rs.getInt(1);
            }
            return 0;
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  @Test
  void testBatchInsertUserRoleRel() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    MetalakePO metalakePO =
        MetalakePO.builder()
            .withMetalakeId(1L)
            .withMetalakeName("test_metalake")
            .withAuditInfo(auditInfo.toString())
            .withSchemaVersion("1.0")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    metalakeMetaMapper.insertMetalakeMeta(metalakePO);

    UserPO userPO1 =
        UserPO.builder()
            .withUserId(1L)
            .withUserName("user1")
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    userMetaMapper.insertUserMeta(userPO1);

    RolePO rolePO1 =
        RolePO.builder()
            .withRoleId(1L)
            .withRoleName("role1")
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    RolePO rolePO2 =
        RolePO.builder()
            .withRoleId(2L)
            .withRoleName("role2")
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    roleMetaMapper.insertRoleMeta(rolePO1);
    roleMetaMapper.insertRoleMeta(rolePO2);

    UserRoleRelPO userRoleRel1 =
        UserRoleRelPO.builder()
            .withUserId(userPO1.getUserId())
            .withRoleId(rolePO1.getRoleId())
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    UserRoleRelPO userRoleRel2 =
        UserRoleRelPO.builder()
            .withUserId(userPO1.getUserId())
            .withRoleId(rolePO2.getRoleId())
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    List<UserRoleRelPO> userRoleRels = Lists.newArrayList(userRoleRel1, userRoleRel2);

    userRoleRelMapper.batchInsertUserRoleRel(userRoleRels);

    Assertions.assertEquals(2, countUserRoleRelsByUserId(userPO1.getUserId()));
  }
}
