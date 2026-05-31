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
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpUserMetaService extends AbstractIdpMetaServiceTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGetIdpUserByUsername(String type) throws IOException {
    init(type);
    insertUsers(1);
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();

    assertThrows(NotFoundException.class, () -> userMetaService.getIdpUserByUsername("missing"));
    assertEquals("user1", userMetaService.getIdpUserByUsername("user1").getUsername());
    assertEquals("user1", userMetaService.getIdpUser("user1").name());
    assertThrows(NotFoundException.class, () -> userMetaService.getIdpUser("missing"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpUser(String type) throws IOException {
    init(type);
    insertGroups(2);
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();
    IdpGroupMetaService groupMetaService = IdpGroupMetaService.getInstance();

    IdpUserPO user1 =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUsername("user1")
            .withPasswordHash("hash-1")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();

    assertThrows(NotFoundException.class, () -> userMetaService.getIdpUserByUsername("user1"));
    runServiceCall(() -> userMetaService.insertIdpUser(user1));
    runServiceCall(
        () -> groupMetaService.changeGroupMembership("group1", List.of("user1"), List.of()));
    assertEquals("user1", userMetaService.getIdpUserByUsername("user1").getUsername());
    assertIterableEquals(List.of("group1"), userMetaService.listGroupNamesByUsername("user1"));

    IdpUserPO duplicateUser =
        IdpUserPO.builder()
            .withUserId(2L)
            .withUsername("user1")
            .withPasswordHash("hash-2")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    assertThrows(AlreadyExistsException.class, () -> userMetaService.insertIdpUser(duplicateUser));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUpdateIdpUserPassword(String type) throws IOException {
    init(type);
    insertUsers(1);
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();

    closeSession();
    assertThrows(
        NotFoundException.class, () -> userMetaService.updateIdpUserPassword("missing", "hash-2"));
    refreshSession();

    runServiceCall(() -> assertTrue(userMetaService.updateIdpUserPassword("user1", "hash-2")));
    assertEquals("hash-2", userMetaService.getIdpUserByUsername("user1").getPasswordHash());
    assertEquals(1L, userMetaService.getIdpUserByUsername("user1").getCurrentVersion());

    runServiceCall(() -> assertTrue(userMetaService.updateIdpUserPassword("user1", "hash-2")));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpUser(String type) throws IOException {
    init(type);
    insertGroups(2);
    insertUsers(4);
    insertDefaultUserGroupRelations();
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();

    runServiceCall(() -> assertTrue(userMetaService.deleteIdpUser("user1")));
    assertNull(idpUserMetaMapper.selectIdpUser("user1"));
    assertEquals(4, countUsers());
    assertEquals(8, countUserGroupRels());
    assertIterableEquals(List.of(), userMetaService.listGroupNamesByUsername("user1"));
    assertEquals("user2", idpUserMetaMapper.selectIdpUser("user2").getUsername());
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteUserMetasByLegacyTimeline(String type) throws Exception {
    init(type);
    insertGroups(2);
    insertUsers(4);
    insertDefaultUserGroupRelations();
    IdpUserMetaService userMetaService = IdpUserMetaService.getInstance();

    assertEquals(0, userMetaService.deleteUserMetasByLegacyTimeline(LEGACY_TIMELINE, 4));
    assertEquals("user1", idpUserMetaMapper.selectIdpUser("user1").getUsername());
    assertEquals(4, countUsers());
    assertEquals(8, countUserGroupRels());

    softDeleteAllUsersAndRelations();
    refreshSession();
    assertNull(idpUserMetaMapper.selectIdpUser("user1"));
    assertEquals(4, countUsers());
    assertEquals(8, countUserGroupRels());

    assertEquals(6, userMetaService.deleteUserMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(1, countUsers());
    assertEquals(5, countUserGroupRels());

    assertEquals(4, userMetaService.deleteUserMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(0, countUsers());
    assertEquals(2, countUserGroupRels());

    assertEquals(2, userMetaService.deleteUserMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
    refreshSession();
    assertEquals(0, countUsers());
    assertEquals(0, countUserGroupRels());

    assertEquals(0, userMetaService.deleteUserMetasByLegacyTimeline(LEGACY_TIMELINE, 3));
  }
}
