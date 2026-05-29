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
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpGroupMetaService extends AbstractIdpMetaServiceTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGetIdpGroupByName(String type) throws IOException {
    init(type);
    insertGroups(1);
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    assertThrows(NotFoundException.class, () -> groupMetaService.getIdpGroupByName("missing"));
    assertEquals("group1", groupMetaService.getIdpGroupByName("group1").getGroupName());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGetIdpGroup(String type) throws IOException {
    init(type);
    insertUsers(2);
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();
    runServiceCall(
        () ->
            groupMetaService.insertIdpGroup(
                IdpGroupPO.builder()
                    .withGroupId(1L)
                    .withGroupName("engineering")
                    .withCurrentVersion(1L)
                    .withLastVersion(0L)
                    .withDeletedAt(0L)
                    .build()));
    runServiceCall(
        () ->
            groupMetaService.changeGroupMembership(
                "engineering", List.of("user1", "user2"), List.of()));

    assertThrows(NotFoundException.class, () -> groupMetaService.getIdpGroup("missing"));
    assertEquals("engineering", groupMetaService.getIdpGroup("engineering").name());
    assertEquals(
        Set.of("user1", "user2"),
        Set.copyOf(groupMetaService.getIdpGroup("engineering").usernames()));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpGroup(String type) throws IOException {
    init(type);
    insertUsers(4);
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    IdpGroupPO group1 =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("group1")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();

    assertThrows(NotFoundException.class, () -> groupMetaService.getIdpGroupByName("group1"));
    runServiceCall(() -> groupMetaService.insertIdpGroup(group1));
    runServiceCall(
        () -> groupMetaService.changeGroupMembership("group1", List.of("user1"), List.of()));
    assertEquals("group1", groupMetaService.getIdpGroupByName("group1").getGroupName());
    assertIterableEquals(List.of("user1"), groupMetaService.listUsernamesByGroupName("group1"));

    IdpGroupPO duplicateGroup =
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("group1")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    assertThrows(
        AlreadyExistsException.class, () -> groupMetaService.insertIdpGroup(duplicateGroup));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroupForceRemovesMemberships(String type) throws IOException {
    init(type);
    insertUsers(4);
    insertGroups(4);
    insertGroupUserGroupRelations();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    assertThrows(
        IllegalStateException.class, () -> groupMetaService.deleteIdpGroup("group1", false));

    runServiceCall(() -> assertTrue(groupMetaService.deleteIdpGroup("group1", true)));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group1"));
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());
    assertIterableEquals(List.of(), groupMetaService.listUsernamesByGroupName("group1"));
    assertEquals("group2", idpGroupMetaMapper.selectIdpGroup("group2").getGroupName());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testAddAndRemoveUsersFromGroup(String type) throws IOException {
    init(type);
    insertUsers(4);
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    IdpGroupPO group1 =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    runServiceCall(() -> groupMetaService.insertIdpGroup(group1));

    runServiceCall(
        () ->
            groupMetaService.changeGroupMembership(
                "engineering", List.of("user1", "user2"), List.of()));
    assertIterableEquals(
        List.of("user1", "user2"), groupMetaService.listUsernamesByGroupName("engineering"));

    runServiceCall(
        () -> groupMetaService.changeGroupMembership("engineering", List.of(), List.of("user1")));
    assertIterableEquals(
        List.of("user2"), groupMetaService.listUsernamesByGroupName("engineering"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testChangeGroupMembershipThrowsWhenUserMissing(String type) throws IOException {
    init(type);
    insertUsers(1);
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();
    runServiceCall(
        () ->
            groupMetaService.insertIdpGroup(
                IdpGroupPO.builder()
                    .withGroupId(1L)
                    .withGroupName("engineering")
                    .withCurrentVersion(1L)
                    .withLastVersion(0L)
                    .withDeletedAt(0L)
                    .build()));

    assertThrows(
        NotFoundException.class,
        () ->
            runServiceCall(
                () ->
                    groupMetaService.changeGroupMembership(
                        "engineering", List.of("missing-user"), List.of())));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteGroupMetasByLegacyTimeline(String type) throws Exception {
    init(type);
    insertUsers(4);
    insertGroups(4);
    insertGroupUserGroupRelations();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    assertEquals(0, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 4));
    assertEquals("group1", idpGroupMetaMapper.selectIdpGroup("group1").getGroupName());
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());

    softDeleteAllGroups();
    refreshSession();
    assertNull(idpGroupMetaMapper.selectIdpGroup("group1"));
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());

    assertEquals(6, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(1, countGroups());
    assertEquals(5, countUserGroupRels());

    assertEquals(4, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(0, countGroups());
    assertEquals(2, countUserGroupRels());

    assertEquals(2, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(0, countGroups());
    assertEquals(0, countUserGroupRels());

    assertEquals(0, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
  }
}
