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
package org.apache.gravitino.integration.test.authorization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccessControlIT extends AbstractIT {

  private static String metalakeName = RandomNameUtils.genRandomName("metalake");
  private static GravitinoMetalake metalake;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
  }

  @Test
  void testManageUsers() {
    String username = "user1#123";
    User user = metalake.addUser(username);
    Assertions.assertEquals(username, user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    // Add an existed user
    Assertions.assertThrows(UserAlreadyExistsException.class, () -> metalake.addUser(username));

    user = metalake.getUser(username);
    Assertions.assertEquals(username, user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    // Get a not-existed user
    Assertions.assertThrows(NoSuchUserException.class, () -> metalake.getUser("not-existed"));

    Assertions.assertTrue(metalake.removeUser(username));
    Assertions.assertFalse(metalake.removeUser(username));
  }

  @Test
  void testManageGroups() {
    String groupName = "group1#123";
    Group group = metalake.addGroup(groupName);
    Assertions.assertEquals(group.name(), groupName);
    Assertions.assertTrue(group.roles().isEmpty());

    // Added an existed group
    Assertions.assertThrows(GroupAlreadyExistsException.class, () -> metalake.addGroup(groupName));

    group = metalake.getGroup(groupName);
    Assertions.assertEquals(group.name(), groupName);
    Assertions.assertTrue(group.roles().isEmpty());

    // Get a not-existed group
    Assertions.assertThrows(NoSuchGroupException.class, () -> metalake.getGroup("not-existed"));

    Assertions.assertTrue(metalake.removeGroup(groupName));
    Assertions.assertFalse(metalake.removeGroup(groupName));
  }

  @Test
  void testManageRoles() {
    String roleName = "role#123";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Role role = metalake.createRole(roleName, properties, Lists.newArrayList(metalakeObject));

    Assertions.assertEquals(roleName, role.name());
    Assertions.assertEquals(properties, role.properties());

    // Verify the object
    Assertions.assertEquals(1, role.securableObjects().size());
    SecurableObject createdObject = role.securableObjects().get(0);
    Assertions.assertEquals(metalakeObject.name(), createdObject.name());
    Assertions.assertEquals(metalakeObject.type(), createdObject.type());

    // Verify the privilege
    Assertions.assertEquals(1, createdObject.privileges().size());
    Privilege createdPrivilege = createdObject.privileges().get(0);
    Assertions.assertEquals(createdPrivilege.name(), Privilege.Name.CREATE_CATALOG);
    Assertions.assertEquals(createdPrivilege.condition(), Privilege.Condition.ALLOW);

    // Test a not-existed metadata object
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            "not-existed", Lists.newArrayList(Privileges.UseCatalog.allow()));

    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> metalake.createRole("not-existed", properties, Lists.newArrayList(catalogObject)));

    // Get a role
    role = metalake.getRole(roleName);

    Assertions.assertEquals(roleName, role.name());
    Assertions.assertEquals(properties, role.properties());

    // Verify the object
    Assertions.assertEquals(1, role.securableObjects().size());
    createdObject = role.securableObjects().get(0);
    Assertions.assertEquals(metalakeObject.name(), createdObject.name());
    Assertions.assertEquals(metalakeObject.type(), createdObject.type());

    // Verify the privilege
    Assertions.assertEquals(1, createdObject.privileges().size());
    createdPrivilege = createdObject.privileges().get(0);
    Assertions.assertEquals(createdPrivilege.name(), Privilege.Name.CREATE_CATALOG);
    Assertions.assertEquals(createdPrivilege.condition(), Privilege.Condition.ALLOW);

    // Delete a role
    Assertions.assertTrue(metalake.deleteRole(roleName));
    Assertions.assertFalse(metalake.deleteRole(roleName));
  }

  @Test
  void testManageUserPermissions() {
    String username = "user1#123";
    User user = metalake.addUser(username);
    Assertions.assertTrue(user.roles().isEmpty());

    String roleName = "role#123";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    metalake.createRole(roleName, properties, Lists.newArrayList(metalakeObject));

    user = metalake.grantRolesToUser(Lists.newArrayList(roleName), username);
    Assertions.assertEquals(Lists.newArrayList(roleName), user.roles());

    user = metalake.revokeRolesFromUser(Lists.newArrayList(roleName), username);
    Assertions.assertTrue(user.roles().isEmpty());

    // Grant a not-existed role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> metalake.grantRolesToUser(Lists.newArrayList("not-existed"), username));

    // Revoke a not-existed role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> metalake.revokeRolesFromUser(Lists.newArrayList("not-existed"), username));

    // Grant to a not-existed user
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> metalake.grantRolesToUser(Lists.newArrayList(roleName), "not-existed"));

    // Revoke from a not-existed user
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> metalake.grantRolesToUser(Lists.newArrayList(roleName), "not-existed"));

    // Grant a granted role
    metalake.grantRolesToUser(Lists.newArrayList(roleName), username);
    Assertions.assertDoesNotThrow(
        () -> metalake.grantRolesToUser(Lists.newArrayList(roleName), username));

    // Revoke a revoked role
    metalake.revokeRolesFromUser(Lists.newArrayList(roleName), username);
    Assertions.assertDoesNotThrow(
        () -> metalake.revokeRolesFromUser(Lists.newArrayList(roleName), username));

    // Clean up
    metalake.removeUser(username);
    metalake.deleteRole(roleName);
  }

  @Test
  void testManageGroupPermissions() {
    String groupName = "group1#123";
    Group group = metalake.addGroup(groupName);
    Assertions.assertTrue(group.roles().isEmpty());

    String roleName = "role#123";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    metalake.createRole(roleName, properties, Lists.newArrayList(metalakeObject));

    group = metalake.grantRolesToGroup(Lists.newArrayList(roleName), groupName);
    Assertions.assertEquals(Lists.newArrayList(roleName), group.roles());

    group = metalake.revokeRolesFromGroup(Lists.newArrayList(roleName), groupName);
    Assertions.assertTrue(group.roles().isEmpty());

    // Grant a not-existed role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> metalake.grantRolesToGroup(Lists.newArrayList("not-existed"), groupName));

    // Revoke a not-existed role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> metalake.revokeRolesFromGroup(Lists.newArrayList("not-existed"), groupName));

    // Grant to a not-existed group
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> metalake.grantRolesToGroup(Lists.newArrayList(roleName), "not-existed"));

    // Revoke from a not-existed group
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> metalake.grantRolesToGroup(Lists.newArrayList(roleName), "not-existed"));

    // Grant a granted role
    metalake.grantRolesToGroup(Lists.newArrayList(roleName), groupName);
    Assertions.assertDoesNotThrow(
        () -> metalake.grantRolesToGroup(Lists.newArrayList(roleName), groupName));

    // Revoke a revoked role
    metalake.revokeRolesFromGroup(Lists.newArrayList(roleName), groupName);
    Assertions.assertDoesNotThrow(
        () -> metalake.revokeRolesFromGroup(Lists.newArrayList(roleName), groupName));

    // Clean up
    metalake.removeGroup(groupName);
    metalake.deleteRole(roleName);
  }
}
