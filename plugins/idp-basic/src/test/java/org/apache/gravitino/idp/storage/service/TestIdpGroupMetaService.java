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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.idp.exception.NoSuchEntityException;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpGroupMetaService extends AbstractIdpMetaServiceTest {

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

    assertThrows(NoSuchEntityException.class, () -> groupMetaService.getIdpGroupByName("group1"));
    runServiceCall(
        () -> groupMetaService.insertIdpGroup(group1, List.of(userGroupRel(100L, 1L, 1L)), false));
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
    assertThrowsEntityAlreadyExists(
        () -> groupMetaService.insertIdpGroup(duplicateGroup, Lists.newArrayList(), false));

    IdpGroupPO overwriteGroup =
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupName("group1")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    List<IdpUserGroupRelPO> overwriteRelations =
        List.of(userGroupRel(101L, 2L, 2L), userGroupRel(102L, 3L, 2L));
    runServiceCall(() -> groupMetaService.insertIdpGroup(overwriteGroup, overwriteRelations, true));
    assertEquals(2L, groupMetaService.getIdpGroupByName("group1").getGroupId());
    assertIterableEquals(
        List.of("user2", "user3"), groupMetaService.listUsernamesByGroupName("group1"));
    assertEquals(2, countGroups());
    assertEquals(3, countUserGroupRels());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroup(String type) throws IOException {
    init(type);
    insertUsers(4);
    insertGroups(4);
    insertGroupUserGroupRelations();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    runServiceCall(() -> assertTrue(groupMetaService.deleteIdpGroup("group1")));
    assertNull(idpGroupMetaMapper.selectIdpGroup("group1"));
    assertEquals(4, countGroups());
    assertEquals(8, countUserGroupRels());
    assertIterableEquals(List.of(), groupMetaService.listUsernamesByGroupName("group1"));
    assertEquals("group2", idpGroupMetaMapper.selectIdpGroup("group2").getGroupName());
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

    assertEquals(3, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(1, countGroups());
    assertEquals(8, countUserGroupRels());

    assertEquals(1, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(0, countGroups());
    assertEquals(8, countUserGroupRels());

    assertEquals(0, groupMetaService.deleteGroupMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
  }
}
