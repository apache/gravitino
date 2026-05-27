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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
    loadConfig("basic", "admin1,admin2");
    when(userMetaService.getIdpUserByUsername("admin1"))
        .thenThrow(new NotFoundException("IdP user not found: %s", "admin1"));
    when(userMetaService.getIdpUserByUsername("admin2")).thenReturn(existingUser("admin2"));
    when(passwordHasher.hash("Passw0rd-For-Admin1")).thenReturn("hashed-password");
    when(idGenerator.nextId()).thenReturn(42L);

    initializeWithPayload("[\"admin1:Passw0rd-For-Admin1\",\"admin2:Passw0rd-For-Admin2\"]");

    ArgumentCaptor<IdpUserPO> userCaptor = ArgumentCaptor.forClass(IdpUserPO.class);
    verify(userMetaService).insertIdpUser(userCaptor.capture());
    IdpUserPO userPO = userCaptor.getValue();
    assertEquals("admin1", userPO.getUsername());
    assertEquals("hashed-password", userPO.getPasswordHash());
    assertEquals(42L, userPO.getUserId());
    assertEquals(POConverters.INIT_VERSION, userPO.getCurrentVersion());
    verify(passwordHasher).hash("Passw0rd-For-Admin1");
  }

  @Test
  void testInitializeSkipsWhenBasicAuthenticatorDisabledEvenIfPayloadInvalid() throws IOException {
    loadConfig("simple", "admin1");
    initializeWithPayload("not-json");
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeSkipsWhenNoServiceAdminsConfigured() throws IOException {
    loadConfig("basic", "");
    initializeWithPayload("[\"admin1:Passw0rd-For-Admin1\"]");
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  @Test
  void testInitializeSkipsWhenAllServiceAdminsAlreadyExist() throws IOException {
    loadConfig("basic", "admin1,admin2");
    when(userMetaService.getIdpUserByUsername("admin1")).thenReturn(existingUser("admin1"));
    when(userMetaService.getIdpUserByUsername("admin2")).thenReturn(existingUser("admin2"));

    initializeWithoutPayload();

    verify(userMetaService).getIdpUserByUsername("admin1");
    verify(userMetaService).getIdpUserByUsername("admin2");
    verify(userMetaService, never()).insertIdpUser(any());
    verifyNoInteractions(passwordHasher, idGenerator);
  }

  @Test
  void testInitializeFailsWhenRequiredPasswordMissing() throws IOException {
    loadConfig("basic", "admin1");
    when(userMetaService.getIdpUserByUsername("admin1"))
        .thenThrow(new NotFoundException("IdP user not found: %s", "admin1"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, this::initializeWithoutPayload);

    assertEquals(
        "Missing initial password for configured service admin admin1; declare"
            + " GRAVITINO_INITIAL_ADMIN_PASSWORD",
        exception.getMessage());
    verify(userMetaService, never()).insertIdpUser(any());
  }

  @ParameterizedTest
  @MethodSource("invalidPasswordPayloads")
  void testInitializeFailsOnInvalidPasswordPayload(String payload, String expectedMessage) {
    loadConfig("basic", "admin1");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initializeWithPayload(payload));

    assertEquals(expectedMessage, exception.getMessage());
    verifyNoInteractions(userMetaService, passwordHasher, idGenerator);
  }

  private static Stream<Arguments> invalidPasswordPayloads() {
    return Stream.of(
        Arguments.of(
            "not-json",
            "GRAVITINO_INITIAL_ADMIN_PASSWORD must be a JSON array of 'username:password' strings"),
        Arguments.of(
            "[\"admin1\"]",
            "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'admin1' must use the format username:password"),
        Arguments.of(
            "[\"other:Passw0rd-For-Other\"]",
            "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'other' is not a configured service admin"),
        Arguments.of(
            "[\"admin1:Passw0rd-For-Admin1\",\"admin1:Passw0rd-For-Admin1-Another\"]",
            "GRAVITINO_INITIAL_ADMIN_PASSWORD contains duplicate entries for service admin admin1"),
        Arguments.of(
            "[\"admin1:short\"]",
            "Password must be at least 12 characters long and at most 64 characters long"));
  }

  private static IdpUserPO existingUser(String username) {
    return IdpUserPO.builder()
        .withUserId(1L)
        .withUsername(username)
        .withPasswordHash("hash")
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private void loadConfig(String authenticators, String serviceAdmins) {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", authenticators,
            "gravitino.authorization.serviceAdmins", serviceAdmins),
        t -> true);
  }

  private void initializeWithoutPayload() throws IOException {
    ServiceAdminInitializer.initialize(config, userMetaService, passwordHasher, idGenerator, null);
  }

  private void initializeWithPayload(String initialAdminPasswords) throws IOException {
    ServiceAdminInitializer.initialize(
        config, userMetaService, passwordHasher, idGenerator, initialAdminPasswords);
  }
}
