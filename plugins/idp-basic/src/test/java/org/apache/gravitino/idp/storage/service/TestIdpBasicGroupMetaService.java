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
import static org.junit.jupiter.api.Assertions.assertNull;

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpBasicGroupMetaService extends AbstractIdpMetaStorageTest {
  private IdpUserMetaMapper idpUserMetaMapper;
  private IdpGroupMetaMapper idpGroupMetaMapper;
  private IdpUserGroupRelMapper idpUserGroupRelMapper;

  @Override
  protected void initializeMappers() {
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    idpUserGroupRelMapper = sharedSession.getMapper(IdpUserGroupRelMapper.class);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void deleteGroupMetasByLegacyTimeline(String type) throws Exception {
    init(type);
    IdpBasicGroupMetaService groupMetaService = IdpBasicGroupMetaService.getInstance();

    insertUsers();
    insertGroups();
    insertUserGroupRelations();

    // hard delete before soft delete
    int deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 4);
    Assertions.assertEquals(0, deletedCount);
    assertEquals("group1", idpGroupMetaMapper.selectIdpGroup("group1").getGroupName());
    assertEquals("group2", idpGroupMetaMapper.selectIdpGroup("group2").getGroupName());
    assertEquals("group3", idpGroupMetaMapper.selectIdpGroup("group3").getGroupName());
    assertEquals("group4", idpGroupMetaMapper.selectIdpGroup("group4").getGroupName());
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());

    // soft delete groups and their relations
    markGroupsAndRelationsSoftDeleted();
    reopenSession();
    assertNull(idpGroupMetaMapper.selectIdpGroup("group1"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group2"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group3"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group4"));
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());

    // hard delete after soft delete
    closeSession();
    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 3);
    reopenSession();
    Assertions.assertEquals(6, deletedCount); // delete 3 group + 3 userGroupRel
    Assertions.assertEquals(1, countGroups()); // 4 - 3
    Assertions.assertEquals(5, countUserGroupRels()); // 8 - 3

    closeSession();
    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 3);
    reopenSession();
    Assertions.assertEquals(4, deletedCount); // delete 1 group + 3 userGroupRel
    Assertions.assertEquals(0, countGroups());
    Assertions.assertEquals(2, countUserGroupRels()); // 5 - 3

    closeSession();
    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 3);
    reopenSession();
    Assertions.assertEquals(2, deletedCount);
    Assertions.assertEquals(0, countGroups());
    Assertions.assertEquals(0, countUserGroupRels());

    closeSession();
    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 3);
    reopenSession();
    Assertions.assertEquals(0, deletedCount); // no more to delete
  }

  private void reopenSession() {
    closeSession();
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    initializeMappers();
  }

  private void insertUsers() {
    for (long userId = 1L; userId <= 4L; userId++) {
      idpUserMetaMapper.insertIdpUser(
          IdpUserPO.builder()
              .withUserId(userId)
              .withUserName("user" + userId)
              .withPasswordHash("hash-" + userId)
              .withCurrentVersion(1L)
              .withLastVersion(0L)
              .withDeletedAt(0L)
              .build());
    }
  }

  private void insertGroups() {
    for (long groupId = 1L; groupId <= 4L; groupId++) {
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

  private void insertUserGroupRelations() {
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

  private IdpUserGroupRelPO userGroupRel(long id, long userId, long groupId) {
    return IdpUserGroupRelPO.builder()
        .withId(id)
        .withUserId(userId)
        .withGroupId(groupId)
        .withCurrentVersion(1L)
        .withLastVersion(0L)
        .withDeletedAt(0L)
        .build();
  }

  private void markGroupsAndRelationsSoftDeleted() throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE idp_group_meta SET deleted_at = 1 WHERE deleted_at = 0");
      statement.execute("UPDATE idp_user_group_rel SET deleted_at = 1 WHERE deleted_at = 0");
    }
  }

  private Integer countGroups() {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM idp_group_meta")) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }

  private Integer countUserGroupRels() {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM idp_user_group_rel")) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }
}
