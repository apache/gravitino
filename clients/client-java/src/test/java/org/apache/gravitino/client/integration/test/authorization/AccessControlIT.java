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
package org.apache.gravitino.client.integration.test.authorization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
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
import org.apache.gravitino.exceptions.IllegalMetadataObjectException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccessControlIT extends BaseIT {

  private static String metalakeName = RandomNameUtils.genRandomName("metalake");
  private static GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    super.startIntegrationTest();
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());

    Catalog filesetCatalog =
        metalake.createCatalog(
            "fileset_catalog", Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap());
    NameIdentifier fileIdent = NameIdentifier.of("fileset_schema", "fileset");
    filesetCatalog.asSchemas().createSchema("fileset_schema", "comment", Collections.emptyMap());
    filesetCatalog
        .asFilesetCatalog()
        .createFileset(fileIdent, "comment", Fileset.Type.EXTERNAL, "tmp", Collections.emptyMap());
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

    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));

    // Test the user with the role
    metalake.createRole("role1", properties, Lists.newArrayList(metalakeObject));
    metalake.grantRolesToUser(Lists.newArrayList("role1"), username);

    // List users
    String anotherUser = "another-user";
    metalake.addUser(anotherUser);
    String[] usernames = metalake.listUserNames();
    Arrays.sort(usernames);
    Assertions.assertEquals(
        Lists.newArrayList(AuthConstants.ANONYMOUS_USER, anotherUser, username),
        Arrays.asList(usernames));
    List<User> users =
        Arrays.stream(metalake.listUsers())
            .sorted(Comparator.comparing(User::name))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        Lists.newArrayList(AuthConstants.ANONYMOUS_USER, anotherUser, username),
        users.stream().map(User::name).collect(Collectors.toList()));
    Assertions.assertEquals(Lists.newArrayList("role1"), users.get(2).roles());

    // Get a not-existed user
    Assertions.assertThrows(NoSuchUserException.class, () -> metalake.getUser("not-existed"));

    Assertions.assertTrue(metalake.removeUser(username));

    Assertions.assertFalse(metalake.removeUser(username));

    // clean up
    metalake.removeUser(anotherUser);
    metalake.deleteRole("role1");
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

    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));

    // Test the group with the role
    metalake.createRole("role2", properties, Lists.newArrayList(metalakeObject));
    metalake.grantRolesToGroup(Lists.newArrayList("role2"), groupName);

    // List groups
    String anotherGroup = "group2#456";
    metalake.addGroup(anotherGroup);
    String[] groupNames = metalake.listGroupNames();
    Arrays.sort(groupNames);
    Assertions.assertEquals(Lists.newArrayList(groupName, anotherGroup), Arrays.asList(groupNames));

    List<Group> groups =
        Arrays.stream(metalake.listGroups())
            .sorted(Comparator.comparing(Group::name))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        Lists.newArrayList(groupName, anotherGroup),
        groups.stream().map(Group::name).collect(Collectors.toList()));
    Assertions.assertEquals(Lists.newArrayList("role2"), groups.get(0).roles());

    Assertions.assertTrue(metalake.removeGroup(groupName));
    Assertions.assertFalse(metalake.removeGroup(groupName));

    // clean up
    metalake.removeGroup(anotherGroup);
    metalake.deleteRole("role2");
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
        IllegalMetadataObjectException.class,
        () -> metalake.createRole("not-existed", properties, Lists.newArrayList(catalogObject)));

    // Create a role with duplicated securable objects
    SecurableObject duplicatedMetalakeObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.createRole(
                roleName,
                properties,
                Lists.newArrayList(metalakeObject, duplicatedMetalakeObject)));

    // Create a role with wrong privilege
    SecurableObject wrongPrivilegeObject =
        SecurableObjects.ofCatalog("wrong", Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.createRole(
                "not-existed", properties, Lists.newArrayList(wrongPrivilegeObject)));

    // Create a role with wrong privilege
    SecurableObject wrongCatalogObject =
        SecurableObjects.ofCatalog(
            "fileset_catalog", Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.createRole("not-existed", properties, Lists.newArrayList(wrongCatalogObject)));

    // Create a role with duplicated privilege
    SecurableObject duplicatedCatalogObject =
        SecurableObjects.ofCatalog(
            "fileset_catalog",
            Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.SelectTable.deny()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            metalake.createRole(
                "not-existed", properties, Lists.newArrayList(duplicatedCatalogObject)));

    // Get a role
    role = metalake.getRole(roleName);

    Assertions.assertEquals(roleName, role.name());
    Assertions.assertEquals(properties, role.properties());
    assertSecurableObjects(Lists.newArrayList(metalakeObject), role.securableObjects());

    // List roles
    String anotherRoleName = "another-role";
    metalake.createRole(anotherRoleName, properties, Lists.newArrayList(metalakeObject));
    String[] roleNames = metalake.listRoleNames();
    Arrays.sort(roleNames);

    Assertions.assertEquals(
        Lists.newArrayList(anotherRoleName, roleName), Arrays.asList(roleNames));

    // List roles by the object (metalake)
    roleNames = metalake.listBindingRoleNames();
    Arrays.sort(roleNames);
    Assertions.assertEquals(
        Lists.newArrayList(anotherRoleName, roleName), Arrays.asList(roleNames));

    String testObjectRole = "testObjectRole";
    SecurableObject anotherCatalogObject =
        SecurableObjects.ofCatalog(
            "fileset_catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            anotherCatalogObject,
            "fileset_schema",
            Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject filesetObject =
        SecurableObjects.ofFileset(
            schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));

    metalake.createRole(
        testObjectRole,
        properties,
        Lists.newArrayList(anotherCatalogObject, schemaObject, filesetObject));

    // List roles by the object (catalog)
    Catalog catalog = metalake.loadCatalog("fileset_catalog");
    roleNames = catalog.supportsRoles().listBindingRoleNames();
    Assertions.assertEquals(Lists.newArrayList(testObjectRole), Arrays.asList(roleNames));

    // List roles by the object (schema)
    Schema schema = catalog.asSchemas().loadSchema("fileset_schema");
    roleNames = schema.supportsRoles().listBindingRoleNames();
    Assertions.assertEquals(Lists.newArrayList(testObjectRole), Arrays.asList(roleNames));

    // List roles by the object (fileset)
    Fileset fileset =
        catalog.asFilesetCatalog().loadFileset(NameIdentifier.of("fileset_schema", "fileset"));
    roleNames = fileset.supportsRoles().listBindingRoleNames();
    Assertions.assertEquals(Lists.newArrayList(testObjectRole), Arrays.asList(roleNames));

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

    // create empty objects role
    Role emptyObjectsRole = metalake.createRole("empty", properties, Collections.emptyList());
    Assertions.assertEquals("empty", emptyObjectsRole.name());
    Assertions.assertEquals(properties, role.properties());

    // Delete a role
    Assertions.assertTrue(metalake.deleteRole(roleName));
    Assertions.assertFalse(metalake.deleteRole(roleName));
    metalake.deleteRole(emptyObjectsRole.name());
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
        IllegalRoleException.class,
        () -> metalake.grantRolesToUser(Lists.newArrayList("not-existed"), username));

    // Revoke a not-existed role
    Assertions.assertThrows(
        IllegalRoleException.class,
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
        IllegalRoleException.class,
        () -> metalake.grantRolesToGroup(Lists.newArrayList("not-existed"), groupName));

    // Revoke a not-existed role
    Assertions.assertThrows(
        IllegalRoleException.class,
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

  @Test
  void testManageRolePermissions() {
    String roleName = "role#123";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("k1", "v1");
    metalake.createRole(roleName, properties, Lists.newArrayList());
    MetadataObject metadataObject =
        MetadataObjects.of(null, metalakeName, MetadataObject.Type.METALAKE);

    // grant a privilege
    Role role =
        metalake.grantPrivilegesToRole(
            roleName, metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertEquals(1, role.securableObjects().size());

    // grant a wrong privilege
    MetadataObject catalog = MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () ->
            metalake.grantPrivilegesToRole(
                roleName, catalog, Lists.newArrayList(Privileges.CreateCatalog.allow())));

    // grant a wrong catalog type privilege
    MetadataObject wrongCatalog =
        MetadataObjects.of(null, "fileset_catalog", MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () ->
            metalake.grantPrivilegesToRole(
                roleName, wrongCatalog, Lists.newArrayList(Privileges.SelectTable.allow())));

    // grant a duplicated privilege
    MetadataObject duplicatedCatalog =
        MetadataObjects.of(null, "fileset_catalog", MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () ->
            metalake.grantPrivilegesToRole(
                roleName,
                duplicatedCatalog,
                Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.SelectTable.deny())));

    // repeat to grant a privilege
    role =
        metalake.grantPrivilegesToRole(
            roleName, metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertEquals(1, role.securableObjects().size());

    // grant a not-existing role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () ->
            metalake.grantPrivilegesToRole(
                "not-exist", metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow())));

    // revoke a privilege
    role =
        metalake.revokePrivilegesFromRole(
            roleName, metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertTrue(role.securableObjects().isEmpty());

    // revoke a wrong privilege
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () ->
            metalake.revokePrivilegesFromRole(
                roleName, catalog, Lists.newArrayList(Privileges.CreateCatalog.allow())));

    // revoke a wrong catalog type privilege
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () ->
            metalake.revokePrivilegesFromRole(
                roleName, wrongCatalog, Lists.newArrayList(Privileges.SelectTable.allow())));

    // repeat to revoke a privilege
    role =
        metalake.revokePrivilegesFromRole(
            roleName, metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertTrue(role.securableObjects().isEmpty());

    // revoke a not-existing role
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () ->
            metalake.revokePrivilegesFromRole(
                "not-exist", metadataObject, Lists.newArrayList(Privileges.CreateCatalog.allow())));

    // Cleanup
    metalake.deleteRole(roleName);
  }

  private static void assertSecurableObjects(
      List<SecurableObject> expect, List<SecurableObject> actual) {
    Assertions.assertEquals(expect.size(), actual.size());
    for (int index = 0; index < expect.size(); index++) {
      Assertions.assertEquals(expect.get(index).fullName(), actual.get(index).fullName());
      Assertions.assertEquals(expect.get(index).type(), actual.get(index).type());
      List<Privilege> expectPrivileges = expect.get(index).privileges();
      List<Privilege> actualPrivileges = actual.get(index).privileges();
      Assertions.assertEquals(expectPrivileges.size(), actualPrivileges.size());
      for (int priIndex = 0; priIndex < expectPrivileges.size(); priIndex++) {
        Assertions.assertEquals(
            expectPrivileges.get(priIndex).name(), actualPrivileges.get(priIndex).name());
        Assertions.assertEquals(
            actualPrivileges.get(priIndex).condition(), actualPrivileges.get(priIndex).condition());
      }
    }
  }
}
