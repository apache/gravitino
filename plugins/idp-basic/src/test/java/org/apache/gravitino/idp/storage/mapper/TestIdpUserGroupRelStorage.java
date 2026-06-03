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

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpUserGroupRelStorage extends AbstractIdpMetaStorageTest {
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
  void testBatchInsertIdpUserGroupsAndSelectGroupNamesByUsername(String type) throws IOException {
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
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(100L)
                .withUserId(1L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpUserGroupRelPO.builder()
                .withId(101L)
                .withUserId(1L)
                .withGroupId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertIterableEquals(
        List.of("dev", "ops"), idpUserGroupRelMapper.selectGroupNamesByUsername("alice"));
    assertIterableEquals(
        List.of(), idpUserGroupRelMapper.selectGroupNamesByUsername("missing-user"));
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
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(100L)
                .withUserId(1L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpUserGroupRelPO.builder()
                .withId(101L)
                .withUserId(2L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertIterableEquals(
        List.of("alice", "bob"), idpUserGroupRelMapper.selectUsernamesByGroupName("dev"));
    assertIterableEquals(
        List.of(), idpUserGroupRelMapper.selectUsernamesByGroupName("missing-group"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpUserGroups(String type) throws IOException {
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
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(100L)
                .withUserId(1L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpUserGroupRelPO.builder()
                .withId(101L)
                .withUserId(2L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpUserGroupRelMapper.softDeleteRelations("dev", List.of("alice")));
    assertIterableEquals(List.of("bob"), idpUserGroupRelMapper.selectUsernamesByGroupName("dev"));
    assertEquals(0, idpUserGroupRelMapper.softDeleteRelations("dev", List.of("alice")));
    assertEquals(1, idpUserGroupRelMapper.softDeleteRelations("dev", List.of()));
    assertIterableEquals(List.of(), idpUserGroupRelMapper.selectUsernamesByGroupName("dev"));
    assertEquals(
        2, idpUserGroupRelMapper.deleteIdpUserGroupRelMetasByLegacyTimeline(Long.MAX_VALUE, 10));
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
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("bob")
            .withPasswordHash("hash-b")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(100L)
                .withUserId(1L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build(),
            IdpUserGroupRelPO.builder()
                .withId(101L)
                .withUserId(2L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpUserGroupRelMapper.softDeleteRelationsByUsername("bob"));
    assertIterableEquals(List.of("alice"), idpUserGroupRelMapper.selectUsernamesByGroupName("dev"));
    assertEquals(0, idpUserGroupRelMapper.softDeleteRelationsByUsername("bob"));
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
            .withUsername("carol")
            .withPasswordHash("hash-c")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(102L)
                .withUserId(3L)
                .withGroupId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpUserGroupRelMapper.softDeleteRelationsByGroupName("ops"));
    assertIterableEquals(List.of(), idpUserGroupRelMapper.selectUsernamesByGroupName("ops"));
    assertEquals(0, idpUserGroupRelMapper.softDeleteRelationsByGroupName("ops"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpUserGroupRelMetasByLegacyTimeline(String type) throws IOException {
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
            .withUsername("legacy-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("new-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(3L)
            .withUsername("active-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(100L)
                .withUserId(1L)
                .withGroupId(1L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(10L)
                .build()));
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(101L)
                .withUserId(2L)
                .withGroupId(2L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(30L)
                .build()));
    idpUserGroupRelMapper.batchInsertRelations(
        List.of(
            IdpUserGroupRelPO.builder()
                .withId(102L)
                .withUserId(3L)
                .withGroupId(3L)
                .withCurrentVersion(1L)
                .withLastVersion(0L)
                .withDeletedAt(0L)
                .build()));

    assertEquals(1, idpUserGroupRelMapper.deleteIdpUserGroupRelMetasByLegacyTimeline(20L, 10));
    assertEquals(0, idpUserGroupRelMapper.deleteIdpUserGroupRelMetasByLegacyTimeline(20L, 10));
    assertEquals(1, idpUserGroupRelMapper.deleteIdpUserGroupRelMetasByLegacyTimeline(40L, 10));
    assertEquals(
        0, idpUserGroupRelMapper.deleteIdpUserGroupRelMetasByLegacyTimeline(Long.MAX_VALUE, 10));
    assertIterableEquals(
        List.of("active-user"), idpUserGroupRelMapper.selectUsernamesByGroupName("qa"));
    assertIterableEquals(List.of(), idpUserGroupRelMapper.selectUsernamesByGroupName("dev"));
    assertIterableEquals(List.of(), idpUserGroupRelMapper.selectUsernamesByGroupName("ops"));
  }
}
