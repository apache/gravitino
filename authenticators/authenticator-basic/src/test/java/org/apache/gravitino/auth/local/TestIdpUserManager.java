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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdpUserManager {

  private IdpUserMetaService userMetaService;
  private PasswordHasher passwordHasher;
  private IdpUserManager manager;

  @BeforeEach
  public void setUp() {
    Config config = new Config(false) {};
    config.set(Configs.AUTHENTICATORS, Collections.singletonList("basic"));
    config.set(Configs.SERVICE_ADMINS, Collections.singletonList("admin"));
    userMetaService = mock(IdpUserMetaService.class);
    passwordHasher = mock(PasswordHasher.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    when(idGenerator.nextId()).thenReturn(1L);
    manager =
        new IdpUserManager(
            config, idGenerator, userMetaService, mock(IdpGroupMetaService.class), passwordHasher);
  }

  @Test
  public void testCreateUser() throws Exception {
    IdpUserPO userPO = user("alice", "hash-1");
    when(userMetaService.findUser("alice")).thenReturn(Optional.empty(), Optional.of(userPO));
    when(userMetaService.listGroupNames("alice")).thenReturn(Collections.emptyList());
    when(passwordHasher.hash("Passw0rd-For-Alice")).thenReturn("hash-1");

    IdpUserDTO user =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"), () -> manager.createUser("alice", "Passw0rd-For-Alice"));

    assertEquals("alice", user.name());
    assertEquals("admin", user.auditInfo().creator());
    verify(userMetaService).createUser(any(IdpUserPO.class));
  }

  @Test
  public void testResetPasswordThrowsWhenPasswordUnchanged() throws Exception {
    IdpUserPO userPO = user("alice", "hash-1");
    when(userMetaService.findUser("alice")).thenReturn(Optional.of(userPO));
    when(passwordHasher.verify("Passw0rd-For-Alice", "hash-1")).thenReturn(true);

    IllegalArgumentException exception =
        PrincipalUtils.doAs(
            new UserPrincipal("admin"),
            () ->
                assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.resetPassword("alice", "Passw0rd-For-Alice")));

    assertEquals(
        "The new password must be different from the old password", exception.getMessage());
  }

  private IdpUserPO user(String userName, String passwordHash) throws Exception {
    return IdpUserPO.builder()
        .withUserId(1L)
        .withUserName(userName)
        .withPasswordHash(passwordHash)
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
    return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
  }
}
