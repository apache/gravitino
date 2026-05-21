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
    for (long groupId = 1L; groupId <= count; groupId++) {
      idpGroupMetaMapper.insertIdpGroup(
          IdpGroupPO.builder()
              .withGroupId(groupId)
              .withGroupName("group" + groupId)
              .withCurrentVersion(1L)
              .withLastVersion(0L)
              .withDeletedAt(0L)
              .build());
    }
  }

  protected void insertUsers(long count) {
    for (long userId = 1L; userId <= count; userId++) {
      idpUserMetaMapper.insertIdpUser(
          IdpUserPO.builder()
              .withUserId(userId)
              .withUsername("user" + userId)
              .withPasswordHash("hash-" + userId)
              .withCurrentVersion(1L)
              .withLastVersion(0L)
              .withDeletedAt(0L)
              .build());
    }
  }

  protected void insertDefaultUserGroupRelations() {
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            userGroupRel(100L, 1L, 1L),
            userGroupRel(101L, 2L, 1L),
            userGroupRel(102L, 3L, 1L),
            userGroupRel(103L, 4L, 1L),
            userGroupRel(104L, 1L, 2L),
            userGroupRel(105L, 2L, 2L),
            userGroupRel(106L, 3L, 2L),
            userGroupRel(107L, 4L, 2L)));
  }

  protected void insertGroupUserGroupRelations() {
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            userGroupRel(100L, 1L, 1L),
            userGroupRel(101L, 2L, 1L),
            userGroupRel(102L, 1L, 2L),
            userGroupRel(103L, 2L, 2L),
            userGroupRel(104L, 3L, 3L),
            userGroupRel(105L, 4L, 3L),
            userGroupRel(106L, 3L, 4L),
            userGroupRel(107L, 4L, 4L)));
  }

  protected IdpUserGroupRelPO userGroupRel(long id, long userId, long groupId) {
    return IdpUserGroupRelPO.builder()
        .withId(id)
        .withUserId(userId)
        .withGroupId(groupId)
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
    executeUpdate("UPDATE idp_group_meta SET deleted_at = 1 WHERE deleted_at = 0");
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
