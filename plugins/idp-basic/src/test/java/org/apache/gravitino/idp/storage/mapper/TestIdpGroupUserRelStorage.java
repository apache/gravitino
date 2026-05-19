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
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpGroupUserRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
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
  void testBatchInsertIdpGroupUsersAndSelectGroupNamesByUsername(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(2L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertIterableEquals(
        List.of("dev", "ops"), idpGroupUserRelMapper.selectGroupNamesByUsername("alice"));
    assertIterableEquals(
        List.of(), idpGroupUserRelMapper.selectGroupNamesByUsername("missing-user"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectUsernamesByGroupName(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUserName("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(1L)
                .withUserId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertIterableEquals(
        List.of("alice", "bob"), idpGroupUserRelMapper.selectUsernamesByGroupName("dev"));
    assertIterableEquals(
        List.of(), idpGroupUserRelMapper.selectUsernamesByGroupName("missing-group"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpGroupUsers(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUserName("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(1L)
                .withUserId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpGroupUserRelMapper.softDeleteRelations(1L, List.of(1L)));
    assertIterableEquals(List.of("bob"), idpGroupUserRelMapper.selectUsernamesByGroupName("dev"));
    assertEquals(0, idpGroupUserRelMapper.softDeleteRelations(1L, List.of(1L)));
    assertEquals(
        1, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(Long.MAX_VALUE, 10));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteRelationsByUsername(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUserName("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(1L)
                .withUserId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpGroupUserRelMapper.softDeleteRelationsByUsername("bob"));
    assertIterableEquals(List.of("alice"), idpGroupUserRelMapper.selectUsernamesByGroupName("dev"));
    assertEquals(0, idpGroupUserRelMapper.softDeleteRelationsByUsername("bob"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteRelationsByGroupName(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(3L)
            .withUserName("carol")
            .withPasswordHash("hash-c")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(102L)
                .withGroupId(2L)
                .withUserId(3L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpGroupUserRelMapper.softDeleteRelationsByGroupName("ops"));
    assertIterableEquals(List.of(), idpGroupUserRelMapper.selectUsernamesByGroupName("ops"));
    assertEquals(0, idpGroupUserRelMapper.softDeleteRelationsByGroupName("ops"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroupUserRelMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(3L)
            .withGroupName("qa")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("legacy-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUserName("new-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(3L)
            .withUserName("active-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(10L)
                .build()));
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(2L)
                .withUserId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(30L)
                .build()));
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(102L)
                .withGroupId(3L)
                .withUserId(3L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(20L, 10));
    assertEquals(0, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(20L, 10));
    assertEquals(1, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(40L, 10));
    assertEquals(
        0, idpGroupUserRelMapper.deleteIdpGroupUserRelMetasByLegacyTimeline(Long.MAX_VALUE, 10));
    assertIterableEquals(
        List.of("active-user"), idpGroupUserRelMapper.selectUsernamesByGroupName("qa"));
    assertIterableEquals(List.of(), idpGroupUserRelMapper.selectUsernamesByGroupName("dev"));
    assertIterableEquals(List.of(), idpGroupUserRelMapper.selectUsernamesByGroupName("ops"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRestart(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(3L)
            .withLastVersion(2L)
            .withDeletedAt(0L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(3L)
            .withLastVersion(2L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUserName("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(100L)
                .withGroupId(1L)
                .withUserId(1L)
                .withCurrentVersion(3L)
                .withLastVersion(2L)
                .withDeletedAt(0L)
                .build()));
    idpGroupUserRelMapper.batchInsertRelations(
        List.of(
            IdpGroupUserRelPO.builder()
                .withId(101L)
                .withGroupId(2L)
                .withUserId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(10L)
                .build()));

    assertPersistedRelations();

    restartBackend();

    assertPersistedRelations();
  }

  private void assertPersistedRelations() {
    assertTrue(
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .hasMapper(IdpGroupUserRelMapper.class));

    assertIterableEquals(List.of("dev"), idpGroupUserRelMapper.selectGroupNamesByUsername("alice"));
    assertIterableEquals(List.of("alice"), idpGroupUserRelMapper.selectUsernamesByGroupName("dev"));
    assertIterableEquals(List.of(), idpGroupUserRelMapper.selectGroupNamesByUsername("bob"));
    assertIterableEquals(List.of(), idpGroupUserRelMapper.selectUsernamesByGroupName("ops"));
  }
}
