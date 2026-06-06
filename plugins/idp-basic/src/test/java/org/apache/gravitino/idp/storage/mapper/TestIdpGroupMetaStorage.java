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
class TestIdpGroupMetaStorage extends AbstractIdpMetaStorageTest {
  private IdpGroupMetaMapper idpGroupMetaMapper;

  @Override
  protected void initializeMappers() {
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpGroupAndSelectIdpGroup(String type) throws IOException {
    init(type);
    IdpGroupPO firstGroup =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpGroupMetaMapper.insertIdpGroup(firstGroup);

    assertEquals(firstGroup, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpGroupWithUsers(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());
    IdpUserMetaMapper idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    IdpUserGroupRelMapper idpUserGroupRelMapper =
        sharedSession.getMapper(IdpUserGroupRelMapper.class);
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

    var groupWithUsers = idpGroupMetaMapper.selectIdpGroupWithUsers("dev");
    assertEquals("dev", groupWithUsers.getName());
    assertTrue(groupWithUsers.getUsernames().contains("alice"));
    assertTrue(groupWithUsers.getUsernames().contains("bob"));
    assertNull(idpGroupMetaMapper.selectIdpGroupWithUsers("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpGroupIgnoresDeletedGroups(String type) throws IOException {
    init(type);
    IdpGroupPO activeGroup =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpGroupMetaMapper.insertIdpGroup(activeGroup);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());

    assertEquals(activeGroup, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("ops"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpGroup(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());

    assertEquals(1, idpGroupMetaMapper.softDeleteIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("dev"));
    assertEquals(0, idpGroupMetaMapper.softDeleteIdpGroup("dev"));
    assertEquals(1, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(Long.MAX_VALUE, 10));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroupMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("legacy-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("new-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(30L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(3L)
            .withGroupName("active-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());

    assertEquals(1, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(20L, 10));
    assertEquals(0, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(20L, 10));
    assertEquals(1, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(40L, 10));
    assertEquals(0, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(Long.MAX_VALUE, 10));
    assertEquals("active-group", idpGroupMetaMapper.selectIdpGroup("active-group").getGroupName());
    assertNull(idpGroupMetaMapper.selectIdpGroup("legacy-group"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("new-group"));
  }
}
