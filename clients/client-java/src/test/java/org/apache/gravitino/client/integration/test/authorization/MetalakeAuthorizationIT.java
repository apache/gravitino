/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String SERVICE_ADMIN = "serviceAdmin";

  private static final String SERVICE_ADMIN_BUT_NOT_OWNER = "serviceAdmin2";

  private GravitinoAdminClient serviceAdminClient;

  private GravitinoAdminClient serviceAdminButNotOwnerClient;

  private static String testMetalake2 = "testMetalake2";

  private static String testMetalake3 = "testMetalake3";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    client.loadMetalake(METALAKE).addUser(SERVICE_ADMIN);
    client.loadMetalake(METALAKE).addUser(SERVICE_ADMIN_BUT_NOT_OWNER);
    serviceAdminClient =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(SERVICE_ADMIN).build();
    serviceAdminButNotOwnerClient =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(SERVICE_ADMIN_BUT_NOT_OWNER).build();
  }

  @Override
  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), String.join(",", USER, SERVICE_ADMIN));
  }

  @Test
  @Order(1)
  public void testCreateMetalake() {
    assertThrows(
        "Only service admins can create metalakes, current user can't create the metalake,  you should configure it in the server configuration first",
        ForbiddenException.class,
        () -> {
          normalUserClient.createMetalake(testMetalake2, "", new HashMap<>());
        });
    serviceAdminClient.createMetalake(testMetalake2, "", new HashMap<>());
    serviceAdminClient.createMetalake(testMetalake3, "", new HashMap<>());
  }

  @Test
  @Order(2)
  public void testListMetalake() {
    assertMetalakeEquals(
        new String[] {METALAKE, testMetalake2, testMetalake3}, serviceAdminClient.listMetalakes());
    serviceAdminClient.disableMetalake(testMetalake2);
    assertMetalakeEquals(new String[] {METALAKE}, normalUserClient.listMetalakes());
    assertMetalakeEquals(
        new String[] {METALAKE, testMetalake2, testMetalake3}, serviceAdminClient.listMetalakes());
    serviceAdminClient.enableMetalake(testMetalake2);
    assertMetalakeEquals(new String[] {METALAKE}, normalUserClient.listMetalakes());
    assertMetalakeEquals(new String[] {METALAKE}, serviceAdminButNotOwnerClient.listMetalakes());
    serviceAdminClient.loadMetalake(testMetalake2).addUser(SERVICE_ADMIN_BUT_NOT_OWNER);
    assertMetalakeEquals(
        new String[] {METALAKE, testMetalake2}, serviceAdminButNotOwnerClient.listMetalakes());
    GravitinoAdminClient tempClient =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth("tempUse").build();
    assertMetalakeEquals(new String[] {}, tempClient.listMetalakes());
  }

  @Test
  @Order(3)
  public void testLoadMetalake() {
    serviceAdminClient.loadMetalake(METALAKE);
    serviceAdminClient.loadMetalake(testMetalake2);
    serviceAdminClient.loadMetalake(testMetalake3);
    normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user access metadata {testMetalake2}",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(testMetalake2);
        });
    assertThrows(
        "Current user access metadata {testMetalake3}",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(testMetalake3);
        });
    serviceAdminButNotOwnerClient.loadMetalake(METALAKE);
    serviceAdminButNotOwnerClient.loadMetalake(testMetalake2);
    assertThrows(
        "Current user access metadata {testMetalake3}",
        ForbiddenException.class,
        () -> {
          serviceAdminButNotOwnerClient.loadMetalake(testMetalake3);
        });
  }

  @Test
  @Order(4)
  public void testSetMetalake() {
    assertThrows(
        "Current user access metadata {testMetalake2}",
        ForbiddenException.class,
        () -> {
          serviceAdminButNotOwnerClient.alterMetalake(
              testMetalake2, MetalakeChange.setProperty("key1", "value1"));
        });
    assertThrows(
        "Current user access metadata {testMetalake2}",
        ForbiddenException.class,
        () -> {
          normalUserClient.alterMetalake(
              testMetalake2, MetalakeChange.setProperty("key1", "value1"));
        });
    serviceAdminClient.alterMetalake(testMetalake2, MetalakeChange.setProperty("key1", "value1"));
  }

  @Test
  @Order(5)
  public void testDropMetalake() {
    assertThrows(
        "Current user access metadata {testMetalake2}",
        ForbiddenException.class,
        () -> {
          serviceAdminButNotOwnerClient.dropMetalake(testMetalake3, true);
        });
    assertThrows(
        "Current user access metadata {testMetalake2}",
        ForbiddenException.class,
        () -> {
          normalUserClient.dropMetalake(testMetalake3, true);
        });
    serviceAdminClient.dropMetalake(testMetalake3, true);
  }

  private void assertMetalakeEquals(
      String[] expectedMetalakes, GravitinoMetalake[] actualMetalakes) {
    Assertions.assertEquals(expectedMetalakes.length, actualMetalakes.length);
    Arrays.sort(expectedMetalakes);
    Arrays.sort(actualMetalakes, Comparator.comparing(MetalakeDTO::name));
    for (int i = 0; i < expectedMetalakes.length; i++) {
      Assertions.assertEquals(expectedMetalakes[i], actualMetalakes[i].name());
    }
  }
}
