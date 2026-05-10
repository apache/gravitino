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
package org.apache.gravitino.idp.basic.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.idp.basic.dto.IdpGroupDTO;
import org.apache.gravitino.idp.basic.dto.IdpUserDTO;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestBasicIdpManager {
  private IdpUserMetaService<IdpUserPO> userMetaService;
  private IdpGroupMetaService<IdpGroupPO, IdpGroupUserRelPO> groupMetaService;
  private PasswordHasher passwordHasher;
  private BasicIdpManager manager;

  @BeforeEach
  void setUp() {
    userMetaService = mock(IdpUserMetaService.class);
    groupMetaService = mock(IdpGroupMetaService.class);
    passwordHasher = mock(PasswordHasher.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    when(idGenerator.nextId()).thenReturn(1L, 2L, 3L, 4L);
    manager = new BasicIdpManager(idGenerator, userMetaService, groupMetaService, passwordHasher);
  }

  @Test
  void testCreateIdpUser() {
    IdpUserPO userPO = user("alice", "hash-1");
    when(userMetaService.findUser("alice")).thenReturn(Optional.empty(), Optional.of(userPO));
    when(userMetaService.listGroupNames("alice")).thenReturn(Collections.emptyList());
    when(passwordHasher.hash("Passw0rd-For-Alice")).thenReturn("hash-1");

    IdpUserDTO user = manager.createIdpUser("alice", "Passw0rd-For-Alice");

    assertEquals("alice", user.name());
    verify(userMetaService).createUser(any(IdpUserPO.class));
  }

  @Test
  void testCreateUserRejectsInvalidUserName() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> manager.createIdpUser("alice:1", "123456789012"));

    assertEquals("User name cannot contain ':'", exception.getMessage());
  }

  @Test
  void testCreateUserRejectsShortPassword() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> manager.createIdpUser("alice", "short"));

    assertEquals("Password length must be at least 12 characters", exception.getMessage());
  }

  @Test
  void testResetPasswordThrowsWhenPasswordUnchanged() {
    IdpUserPO userPO = user("alice", "hash-1");
    when(userMetaService.findUser("alice")).thenReturn(Optional.of(userPO));
    when(passwordHasher.verify("Passw0rd-For-Alice", "hash-1")).thenReturn(true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> manager.resetIdpUserPassword("alice", "Passw0rd-For-Alice"));

    assertEquals(
        "The new password must be different from the old password", exception.getMessage());
  }

  @Test
  void testDeleteUserReturnsFalseWhenUserDoesNotExist() {
    when(userMetaService.findUser("alice")).thenReturn(Optional.empty());

    boolean deleted = manager.deleteUser("alice");

    assertFalse(deleted);
  }

  @Test
  void testGetIdpUser() {
    IdpUserPO userPO = user("alice", "hash-1");
    when(userMetaService.findUser("alice")).thenReturn(Optional.of(userPO));
    when(userMetaService.listGroupNames("alice"))
        .thenReturn(Collections.singletonList("engineering"));

    IdpUserDTO user = manager.getIdpUser("alice");

    assertEquals("alice", user.name());
    assertEquals(Collections.singletonList("engineering"), user.groups());
  }

  @Test
  void testCreateIdpGroup() {
    IdpGroupPO groupPO = group("engineering");
    when(groupMetaService.findGroup("engineering"))
        .thenReturn(Optional.empty(), Optional.of(groupPO));
    when(groupMetaService.listUserNames("engineering")).thenReturn(Collections.emptyList());

    IdpGroupDTO group = manager.createIdpGroup("engineering");

    assertEquals("engineering", group.name());
    verify(groupMetaService).createGroup(any(IdpGroupPO.class));
  }

  @Test
  void testGetIdpGroup() {
    IdpGroupPO groupPO = group("engineering");
    when(groupMetaService.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(groupMetaService.listUserNames("engineering"))
        .thenReturn(Collections.singletonList("alice"));

    IdpGroupDTO group = manager.getIdpGroup("engineering");

    assertEquals("engineering", group.name());
    assertEquals(Collections.singletonList("alice"), group.users());
  }

  @Test
  void testDeleteGroupRequiresForceWhenGroupHasUsers() {
    IdpGroupPO groupPO = group("engineering");
    when(groupMetaService.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(groupMetaService.listUserNames("engineering"))
        .thenReturn(Collections.singletonList("alice"));

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> manager.deleteGroup("engineering", false));

    assertEquals(
        "Removing built-in IdP group engineering is dangerous while it still has users, retry with"
            + " force=true if this is intended",
        exception.getMessage());
  }

  @Test
  void testAddUsersToIdpGroupSkipsExistingUsers() {
    IdpGroupPO groupPO = group("engineering");
    IdpUserPO alice = user("alice", "hash-1");
    IdpUserPO bob = user("bob", "hash-2", 2L);
    when(groupMetaService.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(userMetaService.findUsers(Arrays.asList("alice", "bob")))
        .thenReturn(Arrays.asList(alice, bob));
    when(groupMetaService.selectRelatedUserIds(groupPO.getGroupId(), Arrays.asList(1L, 2L)))
        .thenReturn(Collections.singletonList(1L));
    when(groupMetaService.listUserNames("engineering")).thenReturn(Arrays.asList("alice", "bob"));

    IdpGroupDTO group = manager.addUsersToIdpGroup("engineering", Arrays.asList("alice", "bob"));

    assertEquals("engineering", group.name());
    verify(groupMetaService).addUsersToGroup(anyList());
  }

  @Test
  void testRemoveUsersFromIdpGroupAllowsRemovingAllMembers() {
    IdpGroupPO groupPO = group("engineering");
    IdpUserPO userPO = user("alice", "hash");
    when(groupMetaService.findGroup("engineering"))
        .thenReturn(Optional.of(groupPO), Optional.of(groupPO));
    when(userMetaService.findUsers(Collections.singletonList("alice")))
        .thenReturn(Collections.singletonList(userPO));
    when(groupMetaService.listUserNames("engineering")).thenReturn(Collections.emptyList());

    IdpGroupDTO group =
        manager.removeUsersFromIdpGroup("engineering", Collections.singletonList("alice"));

    assertEquals("engineering", group.name());
    assertEquals(Collections.emptyList(), group.users());
    verify(groupMetaService)
        .removeUsersFromGroup(
            eq(groupPO.getGroupId()), eq(Collections.singletonList(userPO.getUserId())), anyLong());
  }

  private IdpUserPO user(String userName, String passwordHash) {
    return user(userName, passwordHash, 1L);
  }

  private IdpUserPO user(String userName, String passwordHash, long userId) {
    return IdpUserPO.builder()
        .withUserId(userId)
        .withUserName(userName)
        .withPasswordHash(passwordHash)
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private IdpGroupPO group(String groupName) {
    return IdpGroupPO.builder()
        .withGroupId(1L)
        .withGroupName(groupName)
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
