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
package org.apache.gravitino.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestAccessControlManagerForPermissions {

  private static AccessControlManager accessControlManager;

  private static EntityStore entityStore;
  private static CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  private static Config config;

  private static String METALAKE = "metalake";
  private static String CATALOG = "catalog";

  private static String USER = "user";

  private static String GROUP = "group";

  private static List<String> ROLE = Lists.newArrayList("role");

  private static AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  private static BaseMetalake metalakeEntity =
      BaseMetalake.builder()
          .withId(1L)
          .withName(METALAKE)
          .withAuditInfo(auditInfo)
          .withVersion(SchemaVersion.V_0_1)
          .build();

  private static UserEntity userEntity =
      UserEntity.builder()
          .withNamespace(
              Namespace.of(METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME))
          .withId(1L)
          .withName(USER)
          .withAuditInfo(auditInfo)
          .build();

  private static GroupEntity groupEntity =
      GroupEntity.builder()
          .withNamespace(
              Namespace.of(METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME))
          .withId(1L)
          .withName(GROUP)
          .withAuditInfo(auditInfo)
          .build();

  private static RoleEntity roleEntity =
      RoleEntity.builder()
          .withNamespace(
              Namespace.of(METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
          .withId(1L)
          .withName("role")
          .withProperties(Maps.newHashMap())
          .withSecurableObjects(
              Lists.newArrayList(
                  SecurableObjects.ofCatalog(
                      CATALOG, Lists.newArrayList(Privileges.UseCatalog.allow()))))
          .withAuditInfo(auditInfo)
          .build();

  private static RoleEntity grantedRoleEntity =
      RoleEntity.builder()
          .withNamespace(
              Namespace.of(METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
          .withId(1L)
          .withName("grantedRole")
          .withProperties(Maps.newHashMap())
          .withSecurableObjects(
              Lists.newArrayList(
                  SecurableObjects.ofCatalog(
                      CATALOG, Lists.newArrayList(Privileges.UseCatalog.allow()))))
          .withAuditInfo(auditInfo)
          .build();

  private static RoleEntity revokedRoleEntity =
      RoleEntity.builder()
          .withNamespace(
              Namespace.of(METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
          .withId(1L)
          .withName("revokedRole")
          .withProperties(Maps.newHashMap())
          .withSecurableObjects(
              Lists.newArrayList(
                  SecurableObjects.ofCatalog(
                      CATALOG, Lists.newArrayList(Privileges.UseCatalog.allow()))))
          .withAuditInfo(auditInfo)
          .build();

  @BeforeAll
  public static void setUp() throws Exception {
    config = new Config(false) {};
    config.set(Configs.SERVICE_ADMINS, Lists.newArrayList("admin"));

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    entityStore.put(metalakeEntity, true);
    entityStore.put(userEntity, true);
    entityStore.put(groupEntity, true);
    entityStore.put(roleEntity, true);
    entityStore.put(grantedRoleEntity, true);
    entityStore.put(revokedRoleEntity, true);

    accessControlManager = new AccessControlManager(entityStore, new RandomIdGenerator(), config);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", accessControlManager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    BaseCatalog catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    Mockito.when(catalogManager.listCatalogsInfo(Mockito.any()))
        .thenReturn(new Catalog[] {catalog});
    authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testGrantRoleToUser() {
    reset(authorizationPlugin);
    String notExist = "not-exist";

    User user = accessControlManager.getUser(METALAKE, USER);
    Assertions.assertNull(user.roles());

    reset(authorizationPlugin);

    user = accessControlManager.grantRolesToUser(METALAKE, ROLE, USER);
    Assertions.assertFalse(user.roles().isEmpty());

    // Test authorization plugin
    Mockito.verify(authorizationPlugin).onGrantedRolesToUser(any(), any());

    user = accessControlManager.getUser(METALAKE, USER);
    Assertions.assertEquals(1, user.roles().size());
    Assertions.assertEquals(ROLE, user.roles());

    // Test with a role which exists
    user = accessControlManager.grantRolesToUser(METALAKE, ROLE, USER);
    Assertions.assertEquals(1, user.roles().size());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.grantRolesToUser(notExist, ROLE, USER));

    // Throw IllegalRoleException
    Assertions.assertThrows(
        IllegalRoleException.class,
        () -> accessControlManager.grantRolesToUser(METALAKE, Lists.newArrayList(notExist), USER));

    // Throw NoSuchUserException
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> accessControlManager.grantRolesToUser(METALAKE, Lists.newArrayList(ROLE), notExist));

    // Clear Resource
    user = accessControlManager.revokeRolesFromUser(METALAKE, Lists.newArrayList(ROLE), USER);
    Assertions.assertTrue(user.roles().isEmpty());
  }

  @Test
  public void testRevokeRoleFromUser() {
    String notExist = "not-exist";

    User user = accessControlManager.grantRolesToUser(METALAKE, ROLE, USER);
    Assertions.assertFalse(user.roles().isEmpty());

    reset(authorizationPlugin);
    user = accessControlManager.revokeRolesFromUser(METALAKE, ROLE, USER);
    Assertions.assertTrue(user.roles().isEmpty());

    // Test authorization plugin
    Mockito.verify(authorizationPlugin).onRevokedRolesFromUser(any(), any());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRolesFromUser(notExist, ROLE, USER));

    // Throw IllegalRoleException
    Assertions.assertThrows(
        IllegalRoleException.class,
        () ->
            accessControlManager.revokeRolesFromUser(METALAKE, Lists.newArrayList(notExist), USER));

    // Remove role which doesn't exist.
    user = accessControlManager.revokeRolesFromUser(METALAKE, ROLE, USER);
    Assertions.assertTrue(user.roles().isEmpty());

    // Throw NoSuchUserException
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> accessControlManager.revokeRolesFromUser(METALAKE, ROLE, notExist));
  }

  @Test
  public void testGrantRoleToGroup() {
    String notExist = "not-exist";

    Group group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());

    reset(authorizationPlugin);

    group = accessControlManager.grantRolesToGroup(METALAKE, ROLE, GROUP);
    Assertions.assertFalse(group.roles().isEmpty());

    // Test authorization plugin
    verify(authorizationPlugin).onGrantedRolesToGroup(any(), any());

    group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertEquals(1, group.roles().size());
    Assertions.assertEquals(ROLE, group.roles());

    // Test with a role which exists
    group = accessControlManager.grantRolesToGroup(METALAKE, ROLE, GROUP);
    Assertions.assertEquals(1, group.roles().size());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.grantRolesToGroup(notExist, ROLE, GROUP));

    // Throw IllegalRoleException
    Assertions.assertThrows(
        IllegalRoleException.class,
        () ->
            accessControlManager.grantRolesToGroup(METALAKE, Lists.newArrayList(notExist), GROUP));

    // Throw NoSuchGroupException
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> accessControlManager.grantRolesToGroup(METALAKE, ROLE, notExist));

    // Clear Resource
    group = accessControlManager.revokeRolesFromGroup(METALAKE, ROLE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());
  }

  @Test
  public void testRevokeRoleFormGroup() {
    String notExist = "not-exist";

    Group group = accessControlManager.grantRolesToGroup(METALAKE, ROLE, GROUP);
    Assertions.assertFalse(group.roles().isEmpty());

    reset(authorizationPlugin);
    group = accessControlManager.revokeRolesFromGroup(METALAKE, ROLE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());

    // Test authorization plugin
    verify(authorizationPlugin).onRevokedRolesFromGroup(any(), any());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRolesFromGroup(notExist, ROLE, GROUP));

    // Throw IllegalRoleException
    Assertions.assertThrows(
        IllegalRoleException.class,
        () ->
            accessControlManager.revokeRolesFromGroup(
                METALAKE, Lists.newArrayList(notExist), GROUP));

    // Remove not exist role
    group = accessControlManager.revokeRolesFromGroup(METALAKE, ROLE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());

    // Throw NoSuchGroupException
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> accessControlManager.revokeRolesFromGroup(METALAKE, ROLE, notExist));
  }

  @Test
  public void testGrantPrivilegeToRole() {
    reset(authorizationPlugin);
    String notExist = "not-exist";

    Role role =
        accessControlManager.grantPrivilegeToRole(
            METALAKE,
            "grantedRole",
            MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
            Lists.newArrayList(Privileges.CreateTable.allow()));

    List<SecurableObject> objects = role.securableObjects();

    // Test authorization plugin
    verify(authorizationPlugin).onRoleUpdated(any(), any());

    Assertions.assertEquals(2, objects.size());

    // Repeat to grant
    role =
        accessControlManager.grantPrivilegeToRole(
            METALAKE,
            "grantedRole",
            MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
            Lists.newArrayList(Privileges.CreateTable.allow()));
    objects = role.securableObjects();

    Assertions.assertEquals(2, objects.size());

    // Throw IllegalRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () ->
            accessControlManager.grantPrivilegeToRole(
                METALAKE,
                notExist,
                MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
                Lists.newArrayList(Privileges.CreateTable.allow())));
  }

  @Test
  public void testRevokePrivilegeFromRole() {
    reset(authorizationPlugin);
    String notExist = "not-exist";

    Role role =
        accessControlManager.revokePrivilegesFromRole(
            METALAKE,
            "revokedRole",
            MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
            Lists.newArrayList(Privileges.UseCatalog.allow()));

    // Test authorization plugin
    verify(authorizationPlugin).onRoleUpdated(any(), any());

    List<SecurableObject> objects = role.securableObjects();

    Assertions.assertTrue(objects.isEmpty());

    // repeat to revoke
    role =
        accessControlManager.revokePrivilegesFromRole(
            METALAKE,
            "revokedRole",
            MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    objects = role.securableObjects();
    Assertions.assertTrue(objects.isEmpty());

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () ->
            accessControlManager.revokePrivilegesFromRole(
                METALAKE,
                notExist,
                MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
                Lists.newArrayList(Privileges.CreateTable.allow())));
  }
}
