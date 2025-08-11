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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RoleAuthorizationIT extends BaseRestApiAuthorizationIT {

  @Test
  @Order(1)
  public void testCreateRole() {
    client.loadMetalake(METALAKE).createRole("role1", new HashMap<>(), Collections.emptyList());
    client.loadMetalake(METALAKE).createRole("role2", new HashMap<>(), Collections.emptyList());
    client.loadMetalake(METALAKE).createRole("role3", new HashMap<>(), Collections.emptyList());
    // normal user can not create role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createRole("role4", new HashMap<>(), Collections.emptyList());
        });
    client.loadMetalake(METALAKE).grantRolesToUser(ImmutableList.of("role1"), NORMAL_USER);
    client
        .loadMetalake(METALAKE)
        .grantPrivilegesToRole(
            "role1",
            MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
            ImmutableList.of(Privileges.CreateRole.allow()));
    // normal user can create role after grant
    normalUserClient
        .loadMetalake(METALAKE)
        .createRole("role4", new HashMap<>(), Collections.emptyList());
  }

  @Test
  @Order(2)
  public void testListRole() {
    String[] roleNames = client.loadMetalake(METALAKE).listRoleNames();
    assertArrayEquals(new String[] {"role1", "role2", "role3", "role4"}, roleNames);
    roleNames = normalUserClient.loadMetalake(METALAKE).listRoleNames();
    assertArrayEquals(new String[] {"role1", "role4"}, roleNames);
  }

  @Test
  @Order(3)
  public void testGetRole() {
    client.loadMetalake(METALAKE).getRole("role1");
    client.loadMetalake(METALAKE).getRole("role2");
    client.loadMetalake(METALAKE).getRole("role3");
    client.loadMetalake(METALAKE).getRole("role4");
    normalUserClient.loadMetalake(METALAKE).getRole("role1");
    // normal user can not get role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).getRole("role2");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).getRole("role3");
        });
  }

  @Test
  @Order(4)
  public void testDeleteRole() {
    // normal user can not delete role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role1");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role2");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role3");
        });
    // owner can delete role
    client.loadMetalake(METALAKE).deleteRole("role1");
    client.loadMetalake(METALAKE).deleteRole("role2");
    client.loadMetalake(METALAKE).deleteRole("role3");
    // normal user can not create role after delete role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createRole("role2", new HashMap<>(), Collections.emptyList());
        });
  }
}
