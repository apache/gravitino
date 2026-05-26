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

package org.apache.gravitino.idp.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestServiceAdminInitializer {
  private Config config;
  private IdpUserMetaService userMetaService;
  private PasswordHasher passwordHasher;
  private IdGenerator idGenerator;

  @BeforeEach
  void setUp() {
    config = new Config(false) {};
    userMetaService = Mockito.mock(IdpUserMetaService.class);
    passwordHasher = Mockito.mock(PasswordHasher.class);
    idGenerator = Mockito.mock(IdGenerator.class);
  }

  @Test
  void testInitializeCreatesMissingServiceAdmin() throws IOException {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1,admin2"),
        t -> true);
    when(userMetaService.idpUserExists("admin1")).thenReturn(false);
    when(userMetaService.idpUserExists("admin2")).thenReturn(true);
    when(passwordHasher.hash("Passw0rd-For-Admin1")).thenReturn("hashed-password");
    when(idGenerator.nextId()).thenReturn(42L);

    initialize("[\"admin1:Passw0rd-For-Admin1\",\"admin2:Passw0rd-For-Admin2\"]");

    ArgumentCaptor<IdpUserPO> userCaptor = ArgumentCaptor.forClass(IdpUserPO.class);
    verify(userMetaService).insertIdpUser(userCaptor.capture());
    IdpUserPO userPO = userCaptor.getValue();
    assertEquals("admin1", userPO.getUsername());
    assertEquals("hashed-password", userPO.getPasswordHash());
    assertEquals(42L, userPO.getUserId());
    assertEquals(POConverters.INIT_VERSION, userPO.getCurrentVersion());
    verify(userMetaService, times(1)).insertIdpUser(any());
    verify(passwordHasher).hash("Passw0rd-For-Admin1");
  }

  @Test
  void testInitializeSkipsWhenBasicAuthenticatorDisabledEvenIfPayloadInvalid() throws IOException {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "simple",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    initialize("not-json");

    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeSkipsWhenNoServiceAdminsConfigured() throws IOException {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", ""),
        t -> true);

    initialize("[\"admin1:Passw0rd-For-Admin1\"]");

    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeSkipsWhenAllServiceAdminsAlreadyExist() throws IOException {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1,admin2"),
        t -> true);
    when(userMetaService.idpUserExists("admin1")).thenReturn(true);
    when(userMetaService.idpUserExists("admin2")).thenReturn(true);

    initialize(null);

    verify(userMetaService).idpUserExists("admin1");
    verify(userMetaService).idpUserExists("admin2");
    verify(userMetaService, never()).insertIdpUser(any());
    verifyNoInteractions(passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenRequiredPasswordMissing() throws IOException {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);
    when(userMetaService.idpUserExists("admin1")).thenReturn(false);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(null));

    assertEquals(
        "Missing initial password for configured service admin admin1; declare"
            + " GRAVITINO_INITIAL_ADMIN_PASSWORD",
        exception.getMessage());
    verify(userMetaService, never()).insertIdpUser(any());
  }

  @Test
  void testInitializeFailsWhenPasswordPayloadIsInvalidJson() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize("not-json"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD must be a JSON array of 'username:password' strings",
        exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenPasswordPayloadEntryFormatInvalid() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize("[\"admin1\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'admin1' must use the format username:password",
        exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenPasswordPayloadContainsUnknownAdmin() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> initialize("[\"other:Passw0rd-For-Other\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'other' is not a configured service admin",
        exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenPasswordPayloadContainsDuplicateAdmin() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                initialize(
                    "[\"admin1:Passw0rd-For-Admin1\",\"admin1:Passw0rd-For-Admin1-Another\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD contains duplicate entries for service admin admin1",
        exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenPasswordDoesNotSatisfyPolicy() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize("[\"admin1:short\"]"));

    assertEquals("Password length must be at least 12 characters", exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  private void initialize(@Nullable String initialAdminPasswords) throws IOException {
    ServiceAdminInitializer.getInstance()
        .initialize(config, userMetaService, passwordHasher, idGenerator, initialAdminPasswords);
  }
}
