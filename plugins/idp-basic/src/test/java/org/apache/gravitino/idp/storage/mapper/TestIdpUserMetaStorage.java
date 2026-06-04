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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpUserMetaStorage extends AbstractIdpMetaStorageTest {
  private IdpUserMetaMapper idpUserMetaMapper;

  @Override
  protected void initializeMappers() {
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpUserAndSelectIdpUser(String type) throws IOException {
    init(type);
    IdpUserPO firstUser =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpUserMetaMapper.insertIdpUser(firstUser);

    assertEquals(firstUser, idpUserMetaMapper.selectIdpUser("alice"));
    assertNull(idpUserMetaMapper.selectIdpUser("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpUserWithGroups(String type) throws IOException {
    init(type);
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    IdpGroupMetaMapper idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    IdpUserGroupRelMapper idpUserGroupRelMapper =
        sharedSession.getMapper(IdpUserGroupRelMapper.class);
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

    var userWithGroups = idpUserMetaMapper.selectIdpUserWithGroups("alice");
    assertEquals("alice", userWithGroups.getName());
    assertEquals("hash-a", userWithGroups.getPasswordHash());
    assertTrue(userWithGroups.getGroupNames().contains("dev"));
    assertTrue(userWithGroups.getGroupNames().contains("ops"));
    assertNull(idpUserMetaMapper.selectIdpUserWithGroups("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpUsersByUsernames(String type) throws IOException {
    init(type);
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

    assertEquals(
        List.of("alice", "bob"),
        idpUserMetaMapper.selectIdpUsersByUsernames(List.of("alice", "bob", "alice")).stream()
            .map(IdpUserPO::getUsername)
            .sorted()
            .toList());
    assertEquals(List.of(), idpUserMetaMapper.selectIdpUsersByUsernames(List.of("unknown")));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUpdateIdpUserPassword(String type) throws IOException {
    init(type);
    IdpUserPO oldUserPO =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpUserMetaMapper.insertIdpUser(oldUserPO);
    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword("alice", "hash-a-2"));
    assertEquals("hash-a-2", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(1L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(0L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUpdateIdpUserPasswordKeepsVersionsUnchanged(String type) throws IOException {
    init(type);
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(3L)
            .withLastVersion(2L)
            .withDeletedAt(0L)
            .build());

    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword("alice", "hash-a-2"));
    assertEquals("hash-a-2", idpUserMetaMapper.selectIdpUser("alice").getPasswordHash());
    assertEquals(3L, idpUserMetaMapper.selectIdpUser("alice").getCurrentVersion());
    assertEquals(2L, idpUserMetaMapper.selectIdpUser("alice").getLastVersion());
    assertEquals(1, idpUserMetaMapper.updateIdpUserPassword("alice", "hash-a-2"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpUser(String type) throws IOException {
    init(type);
    IdpUserPO oldUserPO =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("alice")
            .withPasswordHash("hash-a")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpUserMetaMapper.insertIdpUser(oldUserPO);

    assertEquals(1, idpUserMetaMapper.softDeleteIdpUser("alice"));
    assertNull(idpUserMetaMapper.selectIdpUser("alice"));
    assertEquals(0, idpUserMetaMapper.softDeleteIdpUser("alice"));
    assertEquals(0, idpUserMetaMapper.updateIdpUserPassword("alice", "hash-a-2"));
    assertEquals(1, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(Long.MAX_VALUE, 10));
    assertEquals(0, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(Long.MAX_VALUE, 10));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpUserMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("legacy-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());
    idpUserMetaMapper.insertIdpUser(
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("new-user")
            .withPasswordHash("hash")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(30L)
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

    assertEquals(1, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(20L, 10));
    assertEquals(0, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(20L, 10));
    assertEquals(1, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(40L, 10));
    assertEquals(0, idpUserMetaMapper.deleteIdpUserMetasByLegacyTimeline(Long.MAX_VALUE, 10));
    assertEquals("active-user", idpUserMetaMapper.selectIdpUser("active-user").getUsername());
    assertNull(idpUserMetaMapper.selectIdpUser("legacy-user"));
    assertNull(idpUserMetaMapper.selectIdpUser("new-user"));
  }
}
