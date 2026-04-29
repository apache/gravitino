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

package org.apache.gravitino.storage.relational.mapper.provider.base;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.auth.ChangedOwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.GroupAuthInfo;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserAuthInfo;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestAuthMappers {

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_authMappers_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static JDBCBackend backend;
  private static RoleMetaMapper roleMetaMapper;
  private static UserMetaMapper userMetaMapper;
  private static GroupMetaMapper groupMetaMapper;
  private static OwnerMetaMapper ownerMetaMapper;
  private static EntityChangeLogMapper entityChangeLogMapper;
  private static MetalakeMetaMapper metalakeMetaMapper;
  private static SqlSession sharedSession;

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

    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    roleMetaMapper = sharedSession.getMapper(RoleMetaMapper.class);
    userMetaMapper = sharedSession.getMapper(UserMetaMapper.class);
    groupMetaMapper = sharedSession.getMapper(GroupMetaMapper.class);
    ownerMetaMapper = sharedSession.getMapper(OwnerMetaMapper.class);
    entityChangeLogMapper = sharedSession.getMapper(EntityChangeLogMapper.class);
    metalakeMetaMapper = sharedSession.getMapper(MetalakeMetaMapper.class);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (sharedSession != null) {
      sharedSession.close();
    }
    if (backend != null) {
      backend.close();
    }
    File dir = new File(JDBC_STORE_PATH);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
  }

  @BeforeEach
  public void truncateTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("TRUNCATE TABLE entity_change_log");
          statement.execute("TRUNCATE TABLE owner_meta");
          statement.execute("TRUNCATE TABLE user_role_rel");
          statement.execute("TRUNCATE TABLE group_role_rel");
          statement.execute("TRUNCATE TABLE role_meta_securable_object");
          statement.execute("TRUNCATE TABLE user_meta");
          statement.execute("TRUNCATE TABLE group_meta");
          statement.execute("TRUNCATE TABLE role_meta");
          statement.execute("TRUNCATE TABLE metalake_meta");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Truncate table failed", e);
    }
  }

  private AuditInfo buildAuditInfo() {
    return AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }

  private MetalakePO insertMetalake(long metalakeId, String metalakeName) {
    AuditInfo auditInfo = buildAuditInfo();
    MetalakePO metalakePO =
        MetalakePO.builder()
            .withMetalakeId(metalakeId)
            .withMetalakeName(metalakeName)
            .withAuditInfo(auditInfo.toString())
            .withSchemaVersion("1.0")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    metalakeMetaMapper.insertMetalakeMeta(metalakePO);
    return metalakePO;
  }

  private UserPO insertUser(long userId, String userName, long metalakeId) {
    AuditInfo auditInfo = buildAuditInfo();
    UserPO userPO =
        UserPO.builder()
            .withUserId(userId)
            .withUserName(userName)
            .withMetalakeId(metalakeId)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    userMetaMapper.insertUserMeta(userPO);
    return userPO;
  }

  private RolePO insertRole(long roleId, String roleName, long metalakeId) {
    AuditInfo auditInfo = buildAuditInfo();
    RolePO rolePO =
        RolePO.builder()
            .withRoleId(roleId)
            .withRoleName(roleName)
            .withMetalakeId(metalakeId)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    roleMetaMapper.insertRoleMeta(rolePO);
    return rolePO;
  }

  private GroupPO insertGroup(long groupId, String groupName, long metalakeId) {
    AuditInfo auditInfo = buildAuditInfo();
    GroupPO groupPO =
        GroupPO.builder()
            .withGroupId(groupId)
            .withGroupName(groupName)
            .withMetalakeId(metalakeId)
            .withAuditInfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    groupMetaMapper.insertGroupMeta(groupPO);
    return groupPO;
  }

  private long queryUpdatedAt(String table, String idColumn, long idValue) {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        String query = "SELECT updated_at FROM " + table + " WHERE " + idColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
          statement.setLong(1, idValue);
          try (ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
              return rs.getLong(1);
            }
            return -1;
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  @Test
  void testRoleMetaTouchUpdatedAt() {
    insertMetalake(1L, "metalake1");
    insertRole(10L, "role10", 1L);

    long before = queryUpdatedAt("role_meta", "role_id", 10L);
    Assertions.assertEquals(0L, before);

    long now = System.currentTimeMillis();
    roleMetaMapper.touchUpdatedAt(10L, now);

    long after = queryUpdatedAt("role_meta", "role_id", 10L);
    Assertions.assertEquals(now, after);
  }

  @Test
  void testRoleMetaBatchGetUpdatedAt() {
    insertMetalake(1L, "metalake1");
    insertRole(11L, "role11", 1L);
    insertRole(12L, "role12", 1L);

    roleMetaMapper.touchUpdatedAt(11L, 1000L);
    roleMetaMapper.touchUpdatedAt(12L, 2000L);

    List<Long> roleIds = Lists.newArrayList(11L, 12L);
    List<RoleUpdatedAt> results = roleMetaMapper.batchGetUpdatedAt(roleIds);

    Assertions.assertEquals(2, results.size());
    results.sort((a, b) -> Long.compare(a.getRoleId(), b.getRoleId()));
    Assertions.assertEquals(11L, results.get(0).getRoleId());
    Assertions.assertEquals(1000L, results.get(0).getUpdatedAt());
    Assertions.assertEquals(12L, results.get(1).getRoleId());
    Assertions.assertEquals(2000L, results.get(1).getUpdatedAt());
  }

  @Test
  void testUserMetaTouchUpdatedAt() {
    insertMetalake(1L, "metalake1");
    insertUser(20L, "user20", 1L);

    long before = queryUpdatedAt("user_meta", "user_id", 20L);
    Assertions.assertEquals(0L, before);

    long now = System.currentTimeMillis();
    userMetaMapper.touchUpdatedAt(20L, now);

    long after = queryUpdatedAt("user_meta", "user_id", 20L);
    Assertions.assertEquals(now, after);
  }

  @Test
  void testUserMetaGetUserInfo() {
    insertMetalake(1L, "metalake1");
    insertUser(21L, "user21", 1L);

    long now = System.currentTimeMillis();
    userMetaMapper.touchUpdatedAt(21L, now);

    UserAuthInfo info = userMetaMapper.getUserInfo("metalake1", "user21");
    Assertions.assertNotNull(info);
    Assertions.assertEquals(21L, info.getUserId());
    Assertions.assertEquals(now, info.getUpdatedAt());
  }

  @Test
  void testGroupMetaTouchUpdatedAt() {
    insertMetalake(1L, "metalake1");
    insertGroup(30L, "group30", 1L);

    long before = queryUpdatedAt("group_meta", "group_id", 30L);
    Assertions.assertEquals(0L, before);

    long now = System.currentTimeMillis();
    groupMetaMapper.modifyUpdatedAt(30L, now);

    long after = queryUpdatedAt("group_meta", "group_id", 30L);
    Assertions.assertEquals(now, after);
  }

  @Test
  void testGroupMetaGetGroupInfoByUserId() {
    insertMetalake(1L, "metalake1");
    insertUser(40L, "user40", 1L);
    insertGroup(41L, "group41", 1L);

    // Insert group_user_rel
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(
              "INSERT INTO group_user_rel (group_id, user_id, deleted_at) VALUES (41, 40, 0)");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Insert group_user_rel failed", e);
    }

    List<GroupAuthInfo> groups = groupMetaMapper.getGroupInfoByUserId(40L);
    Assertions.assertEquals(1, groups.size());
    Assertions.assertEquals(41L, groups.get(0).getGroupId());
  }

  @Test
  void testOwnerMetaSelectOwnerByMetadataObjectId() {
    insertMetalake(1L, "metalake1");
    insertUser(50L, "user50", 1L);
    AuditInfo auditInfo = buildAuditInfo();

    OwnerRelPO ownerRelPO =
        OwnerRelPO.builder()
            .withMetalakeId(1L)
            .withOwnerId(50L)
            .withOwnerType("USER")
            .withMetadataObjectId(100L)
            .withMetadataObjectType("TABLE")
            .withAuditIfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeleteAt(0L)
            .withUpdatedAt(0L)
            .build();
    ownerMetaMapper.insertOwnerRel(ownerRelPO);

    OwnerInfo info = ownerMetaMapper.selectOwnerByMetadataObjectId(100L);
    Assertions.assertNotNull(info);
    Assertions.assertEquals(50L, info.getOwnerId());
    Assertions.assertEquals("USER", info.getOwnerType());
  }

  @Test
  void testOwnerMetaSelectChangedOwners() {
    insertMetalake(1L, "metalake1");
    insertUser(60L, "user60", 1L);
    AuditInfo auditInfo = buildAuditInfo();

    OwnerRelPO ownerRelPO =
        OwnerRelPO.builder()
            .withMetalakeId(1L)
            .withOwnerId(60L)
            .withOwnerType("USER")
            .withMetadataObjectId(200L)
            .withMetadataObjectType("SCHEMA")
            .withAuditIfo(auditInfo.toString())
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeleteAt(0L)
            .withUpdatedAt(0L)
            .build();
    ownerMetaMapper.insertOwnerRel(ownerRelPO);

    // Set updated_at = 100 via direct SQL
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(
              "UPDATE owner_meta SET updated_at = 100 WHERE metadata_object_id = 200");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Update failed", e);
    }

    List<ChangedOwnerInfo> changed = ownerMetaMapper.selectChangedOwners(50L);
    Assertions.assertEquals(1, changed.size());
    Assertions.assertEquals(200L, changed.get(0).getMetadataObjectId());
    Assertions.assertEquals(100L, changed.get(0).getUpdatedAt());

    // With a higher threshold, should find nothing
    List<ChangedOwnerInfo> empty = ownerMetaMapper.selectChangedOwners(100L);
    Assertions.assertTrue(empty.isEmpty());
  }

  @Test
  void testEntityChangeLogInsertAndSelect() {
    long now = System.currentTimeMillis();
    entityChangeLogMapper.insertChange(
        "metalake1", "TABLE", "cat.schema.tbl", OperateType.RENAME, now);

    List<EntityChangeRecord> records = entityChangeLogMapper.selectChanges(now - 1, 10);
    Assertions.assertEquals(1, records.size());
    EntityChangeRecord r = records.get(0);
    Assertions.assertEquals("metalake1", r.getMetalakeName());
    Assertions.assertEquals("TABLE", r.getEntityType());
    Assertions.assertEquals("cat.schema.tbl", r.getFullName());
    Assertions.assertEquals(OperateType.RENAME, r.getOperateType());
    Assertions.assertEquals(now, r.getCreatedAt());
  }

  @Test
  void testEntityChangeLogPruneOldEntries() {
    long old = 1000L;
    long recent = System.currentTimeMillis();
    entityChangeLogMapper.insertChange(
        "metalake1", "SCHEMA", "cat.schema", OperateType.INSERT, old);
    entityChangeLogMapper.insertChange(
        "metalake1", "TABLE", "cat.schema.tbl", OperateType.DROP, recent);

    // Prune entries before (old + 1)
    entityChangeLogMapper.pruneOldEntries(old + 1);

    List<EntityChangeRecord> after = entityChangeLogMapper.selectChanges(0L, 100);
    Assertions.assertEquals(1, after.size());
    Assertions.assertEquals(recent, after.get(0).getCreatedAt());
  }
}
