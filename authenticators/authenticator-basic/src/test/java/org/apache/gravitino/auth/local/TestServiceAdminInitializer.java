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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestServiceAdminInitializer {
  private Config config;
  private ServiceAdminManager serviceAdminManager;

  @BeforeEach
  public void setUp() {
    config = new Config(false) {};
    serviceAdminManager = Mockito.mock(ServiceAdminManager.class);
  }

  @Test
  public void testInitializeCreatesMissingServiceAdmin() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1,admin2"),
        t -> true);
    when(serviceAdminManager.serviceAdminExists("admin1")).thenReturn(false);
    when(serviceAdminManager.serviceAdminExists("admin2")).thenReturn(true);

    ServiceAdminInitializer.getInstance()
        .initialize(
            config,
            serviceAdminManager,
            "[\"admin1:Passw0rd-For-Admin1\",\"admin2:Passw0rd-For-Admin2\"]");

    verify(serviceAdminManager).initializeServiceAdmin("admin1", "Passw0rd-For-Admin1");
    verify(serviceAdminManager, never()).initializeServiceAdmin("admin2", "Passw0rd-For-Admin2");
  }

  @Test
  public void testInitializeSkipsWhenBasicAuthenticatorDisabledEvenIfPayloadInvalid() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "simple",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    ServiceAdminInitializer.getInstance().initialize(config, serviceAdminManager, "not-json");

    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeSkipsWhenNoServiceAdminsConfigured() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", ""),
        t -> true);

    ServiceAdminInitializer.getInstance()
        .initialize(config, serviceAdminManager, "[\"admin1:Passw0rd-For-Admin1\"]");

    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeSkipsWhenAllServiceAdminsAlreadyExist() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1,admin2"),
        t -> true);
    when(serviceAdminManager.serviceAdminExists("admin1")).thenReturn(true);
    when(serviceAdminManager.serviceAdminExists("admin2")).thenReturn(true);

    ServiceAdminInitializer.getInstance().initialize(config, serviceAdminManager, null);

    verify(serviceAdminManager).serviceAdminExists("admin1");
    verify(serviceAdminManager).serviceAdminExists("admin2");
    verify(serviceAdminManager, never()).initializeServiceAdmin("admin1", "Passw0rd-For-Admin1");
    verify(serviceAdminManager, never()).initializeServiceAdmin("admin2", "Passw0rd-For-Admin2");
  }

  @Test
  public void testInitializeFailsWhenRequiredPasswordMissing() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);
    when(serviceAdminManager.serviceAdminExists("admin1")).thenReturn(false);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(config, serviceAdminManager, null));

    assertEquals(
        "Missing initial password for configured service admin admin1; declare"
            + " GRAVITINO_INITIAL_ADMIN_PASSWORD",
        exception.getMessage());
  }

  @Test
  public void testInitializeFailsWhenPasswordPayloadIsInvalidJson() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(config, serviceAdminManager, "not-json"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD must be a JSON array of 'username:password' strings",
        exception.getMessage());
    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeFailsWhenPasswordPayloadEntryFormatInvalid() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(config, serviceAdminManager, "[\"admin1\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'admin1' must use the format username:password",
        exception.getMessage());
    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeFailsWhenPasswordPayloadContainsUnknownAdmin() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(config, serviceAdminManager, "[\"other:Passw0rd-For-Other\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD entry 'other' is not a configured service admin",
        exception.getMessage());
    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeFailsWhenPasswordPayloadContainsDuplicateAdmin() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(
                        config,
                        serviceAdminManager,
                        "[\"admin1:Passw0rd-For-Admin1\",\"admin1:Passw0rd-For-Admin1-Another\"]"));

    assertEquals(
        "GRAVITINO_INITIAL_ADMIN_PASSWORD contains duplicate entries for service admin admin1",
        exception.getMessage());
    verifyNoInteractions(serviceAdminManager);
  }

  @Test
  public void testInitializeFailsWhenPasswordDoesNotSatisfyPolicy() {
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.authenticators", "basic",
            "gravitino.authorization.serviceAdmins", "admin1"),
        t -> true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ServiceAdminInitializer.getInstance()
                    .initialize(config, serviceAdminManager, "[\"admin1:short\"]"));

    assertEquals("Password length must be at least 12 characters", exception.getMessage());
    verifyNoInteractions(serviceAdminManager);
  }
}
