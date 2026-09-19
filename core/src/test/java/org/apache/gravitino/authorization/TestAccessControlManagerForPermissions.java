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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.lock.LockManager;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestAccessControlManagerForPermissions {

  private static AccessControlManager accessControlManager;

  private static EntityStore entityStore;
  private static CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  private static Config config;

  private static String METALAKE = "metalake";
  private static String CATALOG = "catalog";
  private static String SCHEMA = "schema";

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
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    Mockito.when(catalogManager.listCatalogs(Mockito.any()))
        .thenReturn(new NameIdentifier[] {NameIdentifier.of("metalake", "catalog")});
    authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);

    config.set(TREE_LOCK_MAX_NODE_IN_MEMORY, 100000L);
    config.set(TREE_LOCK_MIN_NODE_IN_MEMORY, 1000L);
    config.set(TREE_LOCK_CLEAN_INTERVAL, 36000L);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testRoleDisappearingDuringGrantIsReportedAsIllegalRole() throws IOException {
    EntityStore failingStore = Mockito.mock(EntityStore.class);
    RoleManager roleManager = Mockito.mock(RoleManager.class);
    Mockito.when(roleManager.getRole(METALAKE, roleEntity.name())).thenReturn(roleEntity);
    NoSuchRoleException missing = new NoSuchRoleException("Role was deleted during grant");
    Mockito.doThrow(missing).when(failingStore).update(any(), any(), any(), any());
    PermissionManager manager = new PermissionManager(failingStore, roleManager);

    IllegalRoleException userFailure =
        Assertions.assertThrows(
            IllegalRoleException.class,
            () -> manager.grantRolesToUser(METALAKE, List.of(roleEntity.name()), USER));
    Assertions.assertSame(missing, userFailure.getCause());
    IllegalRoleException groupFailure =
        Assertions.assertThrows(
            IllegalRoleException.class,
            () -> manager.grantRolesToGroup(METALAKE, List.of(roleEntity.name()), GROUP));
    Assertions.assertSame(missing, groupFailure.getCause());
  }

  @ParameterizedTest
  @CsvSource({
    "false, false, false", "false, true, false",
    "true, false, false", "true, true, false",
    "false, false, true", "false, true, true",
    "true, false, true", "true, true, true"
  })
  void testMembershipUpdateRejectsChangedExistingRole(boolean group, boolean grant, boolean deleted)
      throws IOException {
    try (EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore()) {
      store.initialize(config);
      RoleEntity retained = membershipRole(10L, "retained");
      RoleEntity target = membershipRole(20L, "target");
      RoleEntity another = membershipRole(30L, "another");
      putMembershipPrincipal(store, group, List.of(retained, target));
      Entity observed = membershipPrincipal(store, group);
      RoleManager roles = Mockito.mock(RoleManager.class);
      Mockito.when(roles.getRole(METALAKE, target.name())).thenReturn(target);
      Mockito.when(roles.getRole(METALAKE, another.name())).thenReturn(another);
      // The updater sees the old membership, but the name lookup sees a completed delete/recreate.
      if (deleted) {
        Mockito.when(roles.getRole(METALAKE, retained.name()))
            .thenThrow(new NoSuchRoleException("Role was deleted"));
      } else {
        Mockito.when(roles.getRole(METALAKE, retained.name()))
            .thenReturn(membershipRole(11L, retained.name()));
      }
      PermissionManager manager = new PermissionManager(store, roles);
      reset(authorizationPlugin);
      Assertions.assertThrows(
          IllegalRoleException.class,
          () -> changeMembership(manager, group, grant, List.of(target.name(), another.name())));
      // The whole batch must fail before writing a new principal or notifying the plugin.
      Assertions.assertSame(observed, membershipPrincipal(store, group));
      Mockito.verifyNoInteractions(authorizationPlugin);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMembershipUpdateRequiresPairedRoleIds(boolean missingIds) throws IOException {
    try (EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore()) {
      store.initialize(config);
      // Role IDs are optional entity fields, but a name alone cannot prove membership identity.
      UserEntity observed =
          UserEntity.builder()
              .withId(100L)
              .withName(USER)
              .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
              .withRoleNames(List.of("retained"))
              .withRoleIds(missingIds ? null : List.of(10L, 11L))
              .withAuditInfo(auditInfo)
              .build();
      store.put(observed, false);
      RoleManager roles = Mockito.mock(RoleManager.class);
      RoleEntity target = membershipRole(20L, "target");
      Mockito.when(roles.getRole(METALAKE, target.name())).thenReturn(target);
      PermissionManager manager = new PermissionManager(store, roles);
      reset(authorizationPlugin);
      Assertions.assertThrows(
          IllegalRoleException.class,
          () -> manager.grantRolesToUser(METALAKE, List.of(target.name()), USER));
      Assertions.assertSame(observed, membershipPrincipal(store, false));
      Mockito.verifyNoInteractions(authorizationPlugin);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRevokeOldRoleDoesNotRemoveReplacementName(boolean group) throws IOException {
    try (EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore()) {
      store.initialize(config);
      RoleEntity old = membershipRole(10L, "recreated");
      RoleEntity replacement = membershipRole(11L, old.name());
      RoleEntity removed = membershipRole(20L, "removed");
      putMembershipPrincipal(store, group, List.of(replacement, removed));
      RoleManager roles = Mockito.mock(RoleManager.class);
      // The request resolves the old ID, then a concurrent operation grants the replacement
      // before the principal snapshot is read. Revoking the old ID must preserve the new pair.
      Mockito.when(roles.getRole(METALAKE, old.name())).thenReturn(old, replacement);
      Mockito.when(roles.getRole(METALAKE, removed.name())).thenReturn(removed);
      PermissionManager manager = new PermissionManager(store, roles);
      changeMembership(manager, group, false, List.of(old.name(), removed.name(), removed.name()));
      Entity updated = membershipPrincipal(store, group);
      Assertions.assertEquals(
          List.of(replacement.name()),
          group ? ((GroupEntity) updated).roleNames() : ((UserEntity) updated).roleNames());
      Assertions.assertEquals(
          List.of(replacement.id()),
          group ? ((GroupEntity) updated).roleIds() : ((UserEntity) updated).roleIds());
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
            Sets.newHashSet(Privileges.CreateTable.allow()));

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
            Sets.newHashSet(Privileges.CreateTable.allow()));
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
                Sets.newHashSet(Privileges.CreateTable.allow())));
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
            Sets.newHashSet(Privileges.UseCatalog.allow()));

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
            Sets.newHashSet(Privileges.UseCatalog.allow()));
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
                Sets.newHashSet(Privileges.CreateTable.allow())));
  }

  @Test
  public void testOverridePrivileges() throws Exception {
    String testRole = "role";
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            CATALOG,
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateTable.allow()));

    SecurableObject schema =
        SecurableObjects.ofSchema(
            catalog, SCHEMA, Lists.newArrayList(Privileges.CreateTable.allow()));

    // Add two securable objects
    Role role =
        accessControlManager.overridePrivilegesInRole(
            METALAKE, testRole, Lists.newArrayList(catalog, schema));

    List<SecurableObject> objects = role.securableObjects();

    Assertions.assertEquals(2, objects.size());

    // Remove one securable object
    role =
        accessControlManager.overridePrivilegesInRole(
            METALAKE, testRole, Lists.newArrayList(catalog));
    objects = role.securableObjects();
    Assertions.assertEquals(1, objects.size());
    Assertions.assertEquals(catalog, objects.get(0));

    // Update one securable object
    SecurableObject catalogAnother =
        SecurableObjects.ofCatalog(CATALOG, Lists.newArrayList(Privileges.UseCatalog.allow()));
    role =
        accessControlManager.overridePrivilegesInRole(
            METALAKE, testRole, Lists.newArrayList(catalogAnother));

    objects = role.securableObjects();
    Assertions.assertEquals(1, objects.size());
    Assertions.assertEquals(catalogAnother, objects.get(0));

    // Throw IllegalRoleException
    String notExist = "not-exist";
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () ->
            accessControlManager.overridePrivilegesInRole(
                METALAKE, notExist, Lists.newArrayList()));
  }

  private RoleEntity membershipRole(long id, String name) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
        .withProperties(Maps.newHashMap())
        .withSecurableObjects(roleEntity.securableObjects())
        .withAuditInfo(auditInfo)
        .build();
  }

  private void putMembershipPrincipal(EntityStore store, boolean group, List<RoleEntity> roles)
      throws IOException {
    List<String> names = roles.stream().map(RoleEntity::name).toList();
    List<Long> ids = roles.stream().map(RoleEntity::id).toList();
    if (group) {
      store.put(
          GroupEntity.builder()
              .withId(100L)
              .withName(GROUP)
              .withNamespace(AuthorizationUtils.ofGroupNamespace(METALAKE))
              .withRoleNames(names)
              .withRoleIds(ids)
              .withAuditInfo(auditInfo)
              .build(),
          false);
    } else {
      store.put(
          UserEntity.builder()
              .withId(100L)
              .withName(USER)
              .withNamespace(AuthorizationUtils.ofUserNamespace(METALAKE))
              .withRoleNames(names)
              .withRoleIds(ids)
              .withAuditInfo(auditInfo)
              .build(),
          false);
    }
  }

  private Entity membershipPrincipal(EntityStore store, boolean group) throws IOException {
    return group
        ? store.get(
            AuthorizationUtils.ofGroup(METALAKE, GROUP), Entity.EntityType.GROUP, GroupEntity.class)
        : store.get(
            AuthorizationUtils.ofUser(METALAKE, USER), Entity.EntityType.USER, UserEntity.class);
  }

  private void changeMembership(
      PermissionManager manager, boolean group, boolean grant, List<String> roles) {
    if (group) {
      if (grant) {
        manager.grantRolesToGroup(METALAKE, roles, GROUP);
      } else {
        manager.revokeRolesFromGroup(METALAKE, roles, GROUP);
      }
    } else if (grant) {
      manager.grantRolesToUser(METALAKE, roles, USER);
    } else {
      manager.revokeRolesFromUser(METALAKE, roles, USER);
    }
  }
}
