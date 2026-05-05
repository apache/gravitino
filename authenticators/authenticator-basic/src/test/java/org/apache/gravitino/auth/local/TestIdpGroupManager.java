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
import static org.mockito.ArgumentMatchers.anyString;
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
import org.apache.gravitino.auth.local.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.auth.local.storage.relational.po.IdpUserPO;
import org.apache.gravitino.auth.local.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.auth.local.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdpGroupManager {

  private IdpUserMetaService userMetaService;
  private IdpGroupMetaService groupMetaService;
  private IdpGroupManager manager;

  @BeforeEach
  public void setUp() {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Collections.singletonList("basic"));
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    userMetaService = mock(IdpUserMetaService.class);
    groupMetaService = mock(IdpGroupMetaService.class);
    manager =
        new IdpGroupManager(
            config,
            mock(IdGenerator.class),
            userMetaService,
            groupMetaService,
            mock(PasswordHasher.class));
  }

  @Test
  public void testDeleteGroupRequiresForceWhenGroupHasUsers() throws Exception {
    IdpGroupPO groupPO = group("engineering");
    when(groupMetaService.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(groupMetaService.listUserNames("engineering"))
        .thenReturn(Collections.singletonList("alice"));

    UnsupportedOperationException exception =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () ->
                assertThrows(
                    UnsupportedOperationException.class,
                    () -> manager.deleteGroup("engineering", false)));

    assertEquals(
        "Removing built-in IdP group engineering is dangerous while it still has users, retry with"
            + " force=true if this is intended",
        exception.getMessage());
  }

  @Test
  public void testRemoveUsersFromGroupAllowsRemovingAllMembers() throws Exception {
    IdpGroupPO groupPO = group("engineering");
    IdpUserPO userPO = user("alice");
    when(groupMetaService.findGroup("engineering")).thenReturn(Optional.of(groupPO));
    when(userMetaService.findUsers(Collections.singletonList("alice")))
        .thenReturn(Collections.singletonList(userPO));
    when(groupMetaService.listUserNames("engineering")).thenReturn(Collections.emptyList());

    IdpGroupDTO group =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () -> manager.removeUsersFromGroup("engineering", Collections.singletonList("alice")));

    assertEquals("engineering", group.name());
    assertEquals(Collections.emptyList(), group.users());
    verify(groupMetaService)
        .removeUsersFromGroup(
            eq(groupPO.getGroupId()),
            eq(Collections.singletonList(userPO.getUserId())),
            anyLong(),
            anyString());
  }

  private IdpUserPO user(String userName) throws Exception {
    return IdpUserPO.builder()
        .withUserId(1L)
        .withUserName(userName)
        .withPasswordHash("hash")
        .withAuditInfo(toAuditJson())
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private IdpGroupPO group(String groupName) throws Exception {
    return IdpGroupPO.builder()
        .withGroupId(1L)
        .withGroupName(groupName)
        .withAuditInfo(toAuditJson())
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
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
