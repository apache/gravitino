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
package org.apache.gravitino.idp.storage.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.idp.storage.mapper.AbstractIdpMetaStorageTest;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;

/** Base class for IdP meta service tests. */
public abstract class AbstractIdpMetaServiceTest extends AbstractIdpMetaStorageTest {
  protected static final long LEGACY_TIMELINE = Instant.now().toEpochMilli() + 1000;

  protected IdpUserMetaMapper idpUserMetaMapper;
  protected IdpGroupMetaMapper idpGroupMetaMapper;
  protected IdpUserGroupRelMapper idpUserGroupRelMapper;

  @Override
  protected void initializeMappers() {
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    idpUserGroupRelMapper = sharedSession.getMapper(IdpUserGroupRelMapper.class);
  }

  /** Reopens the shared session after service-layer commits or direct SQL updates. */
  protected void refreshSession() {
    closeSession();
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    initializeMappers();
  }

  /**
   * Runs a service call outside the shared session (services use their own sessions), then
   * refreshes mapper state for assertions.
   */
  protected void runServiceCall(ServiceCall serviceCall) throws IOException {
    closeSession();
    serviceCall.run();
    refreshSession();
  }

  @FunctionalInterface
  protected interface ServiceCall {
    void run() throws IOException;
  }

  /**
   * Asserts the service throws {@code EntityAlreadyExistsException} without compiling against the
   * {@code :api} module.
   */
  protected void assertThrowsEntityAlreadyExists(ServiceCall insertCall) throws IOException {
    closeSession();
    try {
      Exception exception = assertThrows(Exception.class, insertCall::run);
      assertEquals(
          "org.apache.gravitino.EntityAlreadyExistsException", exception.getClass().getName());
    } finally {
      refreshSession();
    }
  }

  protected void insertGroups(long count) {
    for (long index = 1L; index <= count; index++) {
      idpGroupMetaMapper.insertIdpGroup(
          IdpGroupPO.builder()
              .withGroupId(index)
              .withGroupName("group" + index)
              .withCurrentVersion(1L)
              .withLastVersion(0L)
              .withDeletedAt(0L)
              .build());
    }
  }

  protected void insertUsers(long count) {
    for (long index = 1L; index <= count; index++) {
      idpUserMetaMapper.insertIdpUser(
          IdpUserPO.builder()
              .withUserId(index)
              .withUsername("user" + index)
              .withPasswordHash("hash-" + index)
              .withCurrentVersion(1L)
              .withLastVersion(0L)
              .withDeletedAt(0L)
              .build());
    }
  }

  protected void insertDefaultUserGroupRelations() {
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            userGroupRel(100L, "user1", "group1"),
            userGroupRel(101L, "user2", "group1"),
            userGroupRel(102L, "user3", "group1"),
            userGroupRel(103L, "user4", "group1"),
            userGroupRel(104L, "user1", "group2"),
            userGroupRel(105L, "user2", "group2"),
            userGroupRel(106L, "user3", "group2"),
            userGroupRel(107L, "user4", "group2")));
  }

  protected void insertGroupUserGroupRelations() {
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            userGroupRel(100L, "user1", "group1"),
            userGroupRel(101L, "user2", "group1"),
            userGroupRel(102L, "user1", "group2"),
            userGroupRel(103L, "user2", "group2"),
            userGroupRel(104L, "user3", "group3"),
            userGroupRel(105L, "user4", "group3"),
            userGroupRel(106L, "user3", "group4"),
            userGroupRel(107L, "user4", "group4")));
  }

  protected IdpUserGroupRelPO userGroupRel(
      long relationOrdinal, String username, String groupName) {
    IdpUserPO user = idpUserMetaMapper.selectIdpUser(username);
    IdpGroupPO group = idpGroupMetaMapper.selectIdpGroup(groupName);
    return IdpUserGroupRelPO.builder()
        .withId(relationOrdinal)
        .withUserId(user.getUserId())
        .withGroupId(group.getGroupId())
        .withCurrentVersion(1L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  protected void softDeleteAllUsersAndRelations() throws SQLException {
    executeUpdate(
        "UPDATE idp_user_meta SET deleted_at = 1 WHERE deleted_at = 0",
        "UPDATE idp_user_group_rel SET deleted_at = 1 WHERE deleted_at = 0");
  }

  protected void softDeleteAllGroups() throws SQLException {
    executeUpdate(
        "UPDATE idp_group_meta SET deleted_at = 1 WHERE deleted_at = 0",
        "UPDATE idp_user_group_rel SET deleted_at = 1 WHERE deleted_at = 0");
  }

  protected int countUsers() {
    return countTableRows("idp_user_meta");
  }

  protected int countGroups() {
    return countTableRows("idp_group_meta");
  }

  protected int countUserGroupRels() {
    return countTableRows("idp_user_group_rel");
  }

  private void executeUpdate(String... sqlStatements) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqlStatements) {
        statement.execute(sql);
      }
    }
  }

  private int countTableRows(String tableName) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + tableName)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new IllegalStateException("Count query returned no rows for table: " + tableName);
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }
}
