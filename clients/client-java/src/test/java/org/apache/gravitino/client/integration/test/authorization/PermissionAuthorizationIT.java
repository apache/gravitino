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

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@Tag("gravitino-docker-test")
public class PermissionAuthorizationIT extends BaseRestApiAuthorizationIT {

  @Test
  public void testGrant() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.createRole("role1", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.createRole("role2", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.createRole("role3", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.addGroup("group1");
    gravitinoMetalake.grantRolesToUser(ImmutableList.of("role1"), NORMAL_USER);
    gravitinoMetalake.grantRolesToGroup(ImmutableList.of("role2"), "group1");
    gravitinoMetalake.grantPrivilegesToRole(
        "role1",
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.ManageGrants.allow()));
    gravitinoMetalake.revokeRolesFromUser(ImmutableList.of("role1"), NORMAL_USER);
    gravitinoMetalake.revokeRolesFromGroup(ImmutableList.of("role2"), "group1");
    gravitinoMetalake.revokeRolesFromUser(ImmutableList.of("role1"), NORMAL_USER);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.grantRolesToUser(
              ImmutableList.of("role1"), NORMAL_USER);
        });
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.grantRolesToGroup(ImmutableList.of("role2"), "group1");
        });
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.grantRolesToGroup(ImmutableList.of("role2"), "group1");
        });
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.revokePrivilegesFromRole(
              "role1",
              MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
              ImmutableSet.of(Privileges.ManageUsers.allow()));
        });
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.revokeRolesFromGroup(
              ImmutableList.of("role2"), "group1");
        });
    assertThrows(
        "Current user can not grant.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.revokeRolesFromUser(
              ImmutableList.of("role1"), NORMAL_USER);
        });

    gravitinoMetalake.grantRolesToUser(ImmutableList.of("role3"), NORMAL_USER);
    gravitinoMetalake.grantPrivilegesToRole(
        "role3",
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.ManageGrants.allow()));
    // normal user can grant after grant
    gravitinoMetalakeLoadByNormalUser.grantRolesToUser(ImmutableList.of("role1"), NORMAL_USER);
    gravitinoMetalakeLoadByNormalUser.grantRolesToGroup(ImmutableList.of("role2"), "group1");
    gravitinoMetalakeLoadByNormalUser.grantRolesToGroup(ImmutableList.of("role2"), "group1");
    gravitinoMetalakeLoadByNormalUser.revokePrivilegesFromRole(
        "role1",
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableSet.of(Privileges.ManageUsers.allow()));
    gravitinoMetalakeLoadByNormalUser.revokeRolesFromGroup(ImmutableList.of("role2"), "group1");
    gravitinoMetalakeLoadByNormalUser.revokeRolesFromUser(ImmutableList.of("role1"), NORMAL_USER);
  }
}
