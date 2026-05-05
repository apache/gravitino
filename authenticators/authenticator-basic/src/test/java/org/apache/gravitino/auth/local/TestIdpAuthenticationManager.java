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

package org.apache.gravitino.auth.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.local.dto.IdpGroupDTO;
import org.apache.gravitino.auth.local.dto.IdpUserDTO;
import org.apache.gravitino.auth.local.store.IdpAuthenticationStore;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdpAuthenticationManager {

  private Config config;
  private IdpAuthenticationStore store;
  private PasswordHasher passwordHasher;
  private IdpAuthenticationManager manager;

  @BeforeEach
  public void setUp() {
    config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Collections.singletonList("basic"));
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    store = mock(IdpAuthenticationStore.class);
    passwordHasher = mock(PasswordHasher.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    when(idGenerator.nextId()).thenReturn(1L);
    manager = new IdpAuthenticationManager(config, idGenerator, store, passwordHasher);
  }

  @Test
  public void testCreateUser() throws Exception {
    IdpUserPO userPO = user("alice", "hash-1");
    when(store.findUser("alice")).thenReturn(Optional.empty(), Optional.of(userPO));
    when(store.listGroupNames("alice")).thenReturn(Collections.emptyList());
    when(passwordHasher.hash("Passw0rd-For-Alice")).thenReturn("hash-1");

    IdpUserDTO user =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"), () -> manager.createUser("alice", "Passw0rd-For-Alice"));

    assertEquals("alice", user.name());
    verify(store).createUser(anyLong(), eq("alice"), eq("hash-1"), eq("admin"));
  }

  @Test
  public void testResetPasswordThrowsWhenPasswordUnchanged() throws Exception {
    IdpUserPO userPO = user("alice", "hash-1");
    when(store.findUser("alice")).thenReturn(Optional.of(userPO));
    when(passwordHasher.verify("Passw0rd-For-Alice", "hash-1")).thenReturn(true);

    PasswordUnchangedException exception =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () ->
                assertThrows(
                    PasswordUnchangedException.class,
                    () -> manager.resetPassword("alice", "Passw0rd-For-Alice")));

    assertEquals(
        "The new password must be different from the old password", exception.getMessage());
  }

  @Test
  public void testDeleteGroupRequiresForceWhenGroupHasUsers() throws Exception {
    IdpGroupPO groupPO = group("engineering");
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.listUserNames("engineering")).thenReturn(Collections.singletonList("alice"));

    UnsupportedOperationException exception =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () ->
                assertThrows(
                    UnsupportedOperationException.class,
                    () -> manager.deleteGroup("engineering", false)));

    assertEquals(
        "Removing local group engineering is dangerous while it still has users, retry with"
            + " force=true if this is intended",
        exception.getMessage());
  }

  @Test
  public void testRemoveUsersFromGroupAllowsForceToRemoveAllMembers() throws Exception {
    IdpGroupPO groupPO = group("engineering");
    IdpUserPO userPO = user("alice", "hash-1");
    when(store.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(store.findUsers(Collections.singletonList("alice")))
        .thenReturn(Collections.singletonList(userPO));
    when(store.listUserNames("engineering"))
        .thenReturn(Collections.singletonList("alice"), Collections.emptyList());

    IdpGroupDTO group =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () ->
                manager.removeUsersFromGroup(
                    "engineering", Collections.singletonList("alice"), true));

    assertEquals("engineering", group.name());
    assertEquals(Collections.emptyList(), group.users());
    verify(store).removeUsersFromGroup(groupPO, Collections.singletonList(userPO), "admin");
  }

  private IdpUserPO user(String userName, String passwordHash) throws Exception {
    IdpUserPO userPO = new IdpUserPO();
    userPO.setUserId(1L);
    userPO.setUserName(userName);
    userPO.setPasswordHash(passwordHash);
    userPO.setAuditInfo(toAuditJson());
    userPO.setCurrentVersion(1L);
    userPO.setLastVersion(1L);
    userPO.setDeletedAt(0L);
    return userPO;
  }

  private IdpGroupPO group(String groupName) throws Exception {
    IdpGroupPO groupPO = new IdpGroupPO();
    groupPO.setGroupId(1L);
    groupPO.setGroupName(groupName);
    groupPO.setAuditInfo(toAuditJson());
    groupPO.setCurrentVersion(1L);
    groupPO.setLastVersion(1L);
    groupPO.setDeletedAt(0L);
    return groupPO;
  }

  private String toAuditJson() throws Exception {
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator("admin")
            .withCreateTime(Instant.now())
            .withLastModifier("admin")
            .withLastModifiedTime(Instant.now())
            .build();
    return JsonUtils.objectMapper().writeValueAsString(auditInfo);
  }
}
