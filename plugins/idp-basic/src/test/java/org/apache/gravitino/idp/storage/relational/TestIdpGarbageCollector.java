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
package org.apache.gravitino.idp.storage.relational;

import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpGarbageCollector extends AbstractIdpMetaStorageTest {
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
  void testCollectAndClean(String type) throws Exception {
    init(type);
    insertGroups();
    insertUsers();
    insertUserGroupRelations();

    markAllSoftDeleted();
    reopenSession();
    assertNull(idpUserMetaMapper.selectIdpUser("user1"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group1"));
    assertEquals(4, countUsers());
    assertEquals(2, countGroups());
    assertEquals(8, countUserGroupRels());

    getConfig().set(STORE_DELETE_AFTER_TIME, 600000L);

    closeSession();
    IdpGarbageCollector garbageCollector = new IdpGarbageCollector(getConfig());
    try {
      garbageCollector.collectAndClean();
    } finally {
      garbageCollector.close();
    }
    reinitializeBackend();
    reopenSession();

    assertEquals(0, countUsers());
    assertEquals(0, countGroups());
    assertEquals(0, countUserGroupRels());
  }

  private void insertGroups() {
    for (long index = 1L; index <= 2L; index++) {
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

  private void insertUsers() {
    for (long index = 1L; index <= 4L; index++) {
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

  private void insertUserGroupRelations() {
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

  private IdpUserGroupRelPO userGroupRel(long relationOrdinal, String username, String groupName) {
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

  private void markAllSoftDeleted() throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("UPDATE idp_user_meta SET deleted_at = 1 WHERE deleted_at = 0");
      statement.execute("UPDATE idp_group_meta SET deleted_at = 1 WHERE deleted_at = 0");
      statement.execute("UPDATE idp_user_group_rel SET deleted_at = 1 WHERE deleted_at = 0");
    }
  }

  private Integer countUsers() {
    return countRows("idp_user_meta");
  }

  private Integer countGroups() {
    return countRows("idp_group_meta");
  }

  private Integer countUserGroupRels() {
    return countRows("idp_user_group_rel");
  }

  private Integer countRows(String tableName) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + tableName)) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }
}
