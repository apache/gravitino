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

package org.apache.gravitino.idp.storage.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpGroupUserRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpGroupUserRelStorage extends AbstractIdpMetaStorageTest {
  private IdpUserMetaMapper idpUserMetaMapper;
  private IdpGroupMetaMapper idpGroupMetaMapper;
  private IdpGroupUserRelMapper idpGroupUserRelMapper;

  @Override
  protected void initializeMappers() {
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    idpGroupUserRelMapper = sharedSession.getMapper(IdpGroupUserRelMapper.class);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testBatchInsertIdpGroupUsersAndSelectGroupNamesByUserId(String type) throws IOException {
    init(type);
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertGroup(20L, "ops", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 20L, 1L, 1L, 0L, 0L);

    assertIterableEquals(List.of("dev", "ops"), idpGroupUserRelMapper.selectGroupNamesByUserId(1L));
    assertTrue(idpGroupUserRelMapper.selectGroupNamesByUserId(999L).isEmpty());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectUserNamesByGroupId(String type) throws IOException {
    init(type);
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    assertIterableEquals(
        List.of("alice", "bob"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(idpGroupUserRelMapper.selectUserNamesByGroupId(999L).isEmpty());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectRelatedUserIds(String type) throws IOException {
    init(type);
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertUser(3L, "carol", "hash-c", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    List<Long> relatedUserIds =
        idpGroupUserRelMapper.selectRelatedUserIds(10L, List.of(2L, 3L, 1L));
    relatedUserIds.sort(Long::compareTo);
    assertIterableEquals(List.of(1L, 2L), relatedUserIds);
    assertTrue(idpGroupUserRelMapper.selectRelatedUserIds(10L, List.of()).isEmpty());
    assertTrue(idpGroupUserRelMapper.selectRelatedUserIds(10L, null).isEmpty());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpGroupUsers(String type) throws IOException {
    init(type);
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    IdpGroupUserRelPO firstRelation = insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteIdpGroupUsers(10L, List.of(1L));
    assertIterableEquals(List.of("bob"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", firstRelation.getId()) > 0L);
    assertEquals(
        2L, queryLongValue("idp_group_user_rel", "current_version", "id", firstRelation.getId()));
    assertEquals(
        1L, queryLongValue("idp_group_user_rel", "last_version", "id", firstRelation.getId()));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteGroupUsersByUserId(String type) throws IOException {
    init(type);
    insertGroup(10L, "dev", 1L, 0L, 0L);
    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    insertUser(2L, "bob", "hash-b", 1L, 0L, 0L);
    insertRelation(100L, 10L, 1L, 1L, 0L, 0L);
    IdpGroupUserRelPO secondRelation = insertRelation(101L, 10L, 2L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteGroupUsersByUserId(2L);
    assertIterableEquals(List.of("alice"), idpGroupUserRelMapper.selectUserNamesByGroupId(10L));
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", secondRelation.getId()) > 0L);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteGroupUsersByGroupId(String type) throws IOException {
    init(type);
    insertGroup(20L, "ops", 1L, 0L, 0L);
    insertUser(3L, "carol", "hash-c", 1L, 0L, 0L);
    IdpGroupUserRelPO thirdRelation = insertRelation(102L, 20L, 3L, 1L, 0L, 0L);

    idpGroupUserRelMapper.softDeleteGroupUsersByGroupId(20L);
    assertTrue(idpGroupUserRelMapper.selectUserNamesByGroupId(20L).isEmpty());
    assertTrue(
        queryLongValue("idp_group_user_rel", "deleted_at", "id", thirdRelation.getId()) > 0L);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroupUserRelMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    insertRelation(100L, 10L, 1L, 1L, 0L, 10L);
    insertRelation(101L, 20L, 2L, 1L, 0L, 30L);
    insertRelation(102L, 30L, 3L, 1L, 0L, 0L);

    assertEquals(1, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(20L, 10));

    assertEquals(0, countRows("idp_group_user_rel", "id", 100L));
    assertEquals(1, countRows("idp_group_user_rel", "id", 101L));
    assertEquals(1, countRows("idp_group_user_rel", "id", 102L));
  }

  private void insertUser(
      long userId,
      String userName,
      String passwordHash,
      long currentVersion,
      long lastVersion,
      long deletedAt) {
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(userId)
            .withUserName(userName)
            .withPasswordHash(passwordHash)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build());
  }

  private void insertGroup(
      long groupId, String groupName, long currentVersion, long lastVersion, long deletedAt) {
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(groupId)
            .withGroupName(groupName)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build());
  }

  private IdpGroupUserRelPO insertRelation(
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

  private long queryLongValue(String table, String column, String idColumn, long idValue) {
    try (Connection connection = sharedSession.getConnection()) {
      String query = "SELECT " + column + " FROM " + table + " WHERE " + idColumn + " = ?";
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setLong(1, idValue);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          return resultSet.getLong(1);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Query " + column + " from " + table + " failed", e);
    }
  }

  private int countRows(String table, String idColumn, long idValue) {
    try (Connection connection = sharedSession.getConnection()) {
      String query = "SELECT COUNT(*) FROM " + table + " WHERE " + idColumn + " = ?";
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setLong(1, idValue);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          return resultSet.getInt(1);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Count rows from " + table + " failed", e);
    }
  }
}
