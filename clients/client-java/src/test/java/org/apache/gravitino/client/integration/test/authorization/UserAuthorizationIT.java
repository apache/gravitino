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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class UserAuthorizationIT extends BaseRestApiAuthorizationIT {

  @Test
  @Order(1)
  public void testCreateUser() {
    assertThrows(
        "Current user can not access metadata {testMetalake}",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).addUser("user1");
        });
    client.loadMetalake(METALAKE).addUser("user1");
    client.loadMetalake(METALAKE).addUser("user2");
  }

  @Test
  @Order(2)
  public void testListUser() {
    User[] users = client.loadMetalake(METALAKE).listUsers();
    assertUserEquals(new String[] {USER, NORMAL_USER, "user1", "user2"}, users);
    User[] usersLoadByUser1 = getClientByUser("user1").loadMetalake(METALAKE).listUsers();
    assertUserEquals(
        new String[] {
          "user1",
        },
        usersLoadByUser1);
    String[] usernames = client.loadMetalake(METALAKE).listUserNames();
    Assertions.assertArrayEquals(new String[] {USER, NORMAL_USER, "user1", "user2"}, usernames);
    String[] usernamesLoadByUser1 = getClientByUser("user1").loadMetalake(METALAKE).listUserNames();
    Assertions.assertArrayEquals(
        new String[] {
          "user1",
        },
        usernamesLoadByUser1);
  }

  @Test
  @Order(2)
  public void testLoadUser() {
    GravitinoAdminClient user1Client = getClientByUser("user1");
    user1Client.loadMetalake(METALAKE).getUser("user1");
    assertThrows(
        "Current user can not get user.",
        ForbiddenException.class,
        () -> {
          user1Client.loadMetalake(METALAKE).getUser("user2");
        });
    client.loadMetalake(METALAKE).getUser("user1");
    client.loadMetalake(METALAKE).getUser("user2");
  }

  @Test
  @Order(3)
  public void testRemoveUser() {
    GravitinoAdminClient user1Client = getClientByUser("user1");
    assertThrows(
        "Current user can not get user",
        ForbiddenException.class,
        () -> {
          user1Client.loadMetalake(METALAKE).removeUser("user2");
        });
    assertThrows(
        "Current user can not get user.",
        ForbiddenException.class,
        () -> {
          user1Client.loadMetalake(METALAKE).removeUser("user1");
        });
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    // owner can remove user
    gravitinoMetalake.removeUser("user2");
    // user1 can remove user after grant
    gravitinoMetalake.createRole("role", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.grantPrivilegesToRole(
        "role",
        MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.ManageUsers.allow()));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of("role"), "user1");
    gravitinoMetalake.addUser("user3");
    User[] users = gravitinoMetalake.listUsers();
    assertUserEquals(new String[] {USER, NORMAL_USER, "user1", "user3"}, users);
    user1Client.loadMetalake(METALAKE).removeUser("user3");
    users = gravitinoMetalake.listUsers();
    assertUserEquals(new String[] {USER, NORMAL_USER, "user1"}, users);
  }

  private void assertUserEquals(String[] exceptUsers, User[] actualUsers) {
    Arrays.sort(exceptUsers);
    Arrays.sort(actualUsers, Comparator.comparing(User::name));
    assertEquals(exceptUsers.length, actualUsers.length);
    for (int i = 0; i < exceptUsers.length; i++) {
      assertEquals(exceptUsers[i], actualUsers[i].name());
    }
  }

  private GravitinoAdminClient getClientByUser(String username) {
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(username).build();
    closer.register(client);
    return client;
  }
}
