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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
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
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
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

  private static final List<Privilege> USE_CATALOG =
      Lists.newArrayList(Privileges.UseCatalog.allow());

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
    Mockito.when(catalog.name()).thenReturn(CATALOG);
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
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void testMembershipUpdateCarriesObservedRolesForward(boolean group, boolean grant)
      throws IOException {
    try (EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore()) {
      store.initialize(config);
      RoleEntity retained = membershipRole(10L, "retained");
      RoleEntity target = membershipRole(20L, "target");
      putMembershipPrincipal(store, group, List.of(retained, target));
      RoleManager roles = Mockito.mock(RoleManager.class);
      Mockito.when(roles.getRole(METALAKE, target.name())).thenReturn(target);
      // "retained" has been deleted and recreated under the same name. The updater must never
      // resolve an existing membership by name, so the replacement cannot inherit the grant and
      // the update costs no lookup per existing role.
      Mockito.when(roles.getRole(METALAKE, retained.name()))
          .thenReturn(membershipRole(11L, retained.name()));
      PermissionManager manager = new PermissionManager(store, roles);
      reset(authorizationPlugin);

      changeMembership(manager, group, grant, List.of(target.name()));

      Entity updated = membershipPrincipal(store, group);
      List<String> names =
          group ? ((GroupEntity) updated).roleNames() : ((UserEntity) updated).roleNames();
      List<Long> ids = group ? ((GroupEntity) updated).roleIds() : ((UserEntity) updated).roleIds();
      // Granting an already-held role is a no-op, revoking it drops only that pair.
      Assertions.assertEquals(
          grant ? List.of(retained.name(), target.name()) : List.of(retained.name()), names);
      Assertions.assertEquals(
          grant ? List.of(retained.id(), target.id()) : List.of(retained.id()), ids);
      Mockito.verify(roles, Mockito.never()).getRole(METALAKE, retained.name());
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
      Mockito.when(roles.getRole(METALAKE, old.name())).thenReturn(old);
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

  @ParameterizedTest
  @ValueSource(strings = {"METALAKE", "CATALOG", "SCHEMA"})
  void testSemanticModelPrivilegeUpdatesStayLocal(String scope) throws IOException {
    MetadataObject.Type type = MetadataObject.Type.valueOf(scope);
    String fullName =
        type == MetadataObject.Type.METALAKE
            ? METALAKE
            : type == MetadataObject.Type.CATALOG ? CATALOG : CATALOG + "." + SCHEMA;
    MetadataObject object = MetadataObjects.parse(fullName, type);
    String name = "semantic_updates_" + scope;
    entityStore.put(
        RoleEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
            .withAuditInfo(auditInfo)
            .withSecurableObjects(List.of())
            .build(),
        false);
    PermissionManager manager = new PermissionManager(entityStore, Mockito.mock(RoleManager.class));
    Privilege semantic = Privileges.UseSemanticModel.allow();
    Privilege table = Privileges.SelectTable.allow();
    SecurableObject tableObject = SecurableObjects.parse(fullName, type, List.of(table));
    SecurableObject semanticObject = SecurableObjects.parse(fullName, type, List.of(semantic));
    SecurableObject mixed = SecurableObjects.parse(fullName, type, List.of(semantic, table));
    reset(authorizationPlugin);
    Role semanticRole =
        manager.grantPrivilegesToRole(
            METALAKE, name, object, Set.of(semantic, Privileges.CreateSemanticModel.allow()));
    Assertions.assertEquals(
        Set.of(semantic, Privileges.CreateSemanticModel.allow()),
        Set.copyOf(semanticRole.securableObjects().get(0).privileges()));
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.grantPrivilegesToRole(METALAKE, name, object, Set.of(table));
    assertConnectorChange(RoleChange.addSecurableObject(name, tableObject));
    manager.grantPrivilegesToRole(
        METALAKE, name, object, Set.of(Privileges.ModifySemanticModel.allow()));
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.revokePrivilegesFromRole(METALAKE, name, object, Set.of(table));
    assertConnectorChange(RoleChange.removeSecurableObject(name, tableObject));
    manager.overridePrivilegesInRole(METALAKE, name, List.of(mixed));
    assertConnectorChange(RoleChange.addSecurableObject(name, tableObject));
    SecurableObject modifiedTable =
        SecurableObjects.parse(fullName, type, List.of(Privileges.ModifyTable.allow()));
    manager.overridePrivilegesInRole(
        METALAKE,
        name,
        List.of(
            SecurableObjects.parse(
                fullName, type, List.of(semantic, Privileges.ModifyTable.allow()))));
    assertConnectorChange(RoleChange.updateSecurableObject(name, tableObject, modifiedTable));
    manager.revokePrivilegesFromRole(METALAKE, name, object, Set.of(semantic));
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.overridePrivilegesInRole(METALAKE, name, List.of(semanticObject));
    assertConnectorChange(RoleChange.removeSecurableObject(name, modifiedTable));
    manager.overridePrivilegesInRole(METALAKE, name, List.of());
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.overridePrivilegesInRole(METALAKE, name, List.of(semanticObject));
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.revokePrivilegesFromRole(METALAKE, name, object, Set.of(semantic));
    Mockito.verifyNoInteractions(authorizationPlugin);
    manager.grantPrivilegesToRole(METALAKE, name, object, Set.of(semantic, table));
    assertConnectorChange(RoleChange.addSecurableObject(name, tableObject));
    manager.revokePrivilegesFromRole(METALAKE, name, object, Set.of(semantic, table));
    assertConnectorChange(RoleChange.removeSecurableObject(name, tableObject));
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

  private void assertConnectorChange(RoleChange expected) {
    ArgumentCaptor<Role> role = ArgumentCaptor.forClass(Role.class);
    ArgumentCaptor<RoleChange> change = ArgumentCaptor.forClass(RoleChange.class);
    verify(authorizationPlugin).onRoleUpdated(role.capture(), change.capture());
    Assertions.assertEquals(expected, change.getValue());
    role.getValue()
        .securableObjects()
        .forEach(
            object ->
                object
                    .privileges()
                    .forEach(
                        privilege ->
                            Assertions.assertFalse(
                                privilege.name().name().endsWith("SEMANTIC_MODEL"))));
    reset(authorizationPlugin);
  }

  @Test
  public void testRemoveUserRevokesRolesInAuthorizationPlugin() {
    String user = "user_to_remove";
    accessControlManager.addUser(METALAKE, user);
    accessControlManager.grantRolesToUser(METALAKE, ROLE, user);

    reset(authorizationPlugin);
    Assertions.assertTrue(accessControlManager.removeUser(METALAKE, user));

    ArgumentCaptor<List<Role>> roles = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<User> removedUser = ArgumentCaptor.forClass(User.class);
    verify(authorizationPlugin).onRevokedRolesFromUser(roles.capture(), removedUser.capture());
    Assertions.assertEquals(ROLE, roles.getValue().stream().map(Role::name).toList());
    Assertions.assertEquals(user, removedUser.getValue().name());
    // The plugin gets the user as it was before the delete, still listing the revoked roles.
    Assertions.assertEquals(ROLE, removedUser.getValue().roles());
    // The user in the underlying system was not created by Gravitino, so it is not dropped.
    verify(authorizationPlugin, Mockito.never()).onUserRemoved(any());
    Assertions.assertThrows(
        NoSuchUserException.class, () -> accessControlManager.getUser(METALAKE, user));
  }

  @Test
  public void testRemoveUserWithoutRolesOrMissingDoesNotCallAuthorizationPlugin() {
    String user = "user_without_roles";
    accessControlManager.addUser(METALAKE, user);

    reset(authorizationPlugin);
    Assertions.assertTrue(accessControlManager.removeUser(METALAKE, user));
    Assertions.assertFalse(accessControlManager.removeUser(METALAKE, user));
    Mockito.verifyNoInteractions(authorizationPlugin);
  }

  @Test
  public void testRemoveGroupRevokesRolesInAuthorizationPlugin() {
    String group = "group_to_remove";
    accessControlManager.addGroup(METALAKE, group);
    accessControlManager.grantRolesToGroup(METALAKE, ROLE, group);

    reset(authorizationPlugin);
    Assertions.assertTrue(accessControlManager.removeGroup(METALAKE, group));

    ArgumentCaptor<List<Role>> roles = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Group> removedGroup = ArgumentCaptor.forClass(Group.class);
    verify(authorizationPlugin).onRevokedRolesFromGroup(roles.capture(), removedGroup.capture());
    Assertions.assertEquals(ROLE, roles.getValue().stream().map(Role::name).toList());
    Assertions.assertEquals(group, removedGroup.getValue().name());
    verify(authorizationPlugin, Mockito.never()).onGroupRemoved(any());

    // A group without roles, or one that no longer exists, does not reach the plugin.
    String emptyGroup = "group_without_roles";
    accessControlManager.addGroup(METALAKE, emptyGroup);
    reset(authorizationPlugin);
    Assertions.assertTrue(accessControlManager.removeGroup(METALAKE, emptyGroup));
    Assertions.assertFalse(accessControlManager.removeGroup(METALAKE, emptyGroup));
    Mockito.verifyNoInteractions(authorizationPlugin);
  }

  @Test
  public void testBulkRemoveRevokesRolesInAuthorizationPlugin() {
    List<String> users = List.of("bulk_user_1", "bulk_user_2");
    List<String> groups = List.of("bulk_group_1", "bulk_group_2");
    for (int i = 0; i < 2; i++) {
      accessControlManager.addUser(METALAKE, users.get(i));
      accessControlManager.grantRolesToUser(METALAKE, ROLE, users.get(i));
      accessControlManager.addGroup(METALAKE, groups.get(i));
      accessControlManager.grantRolesToGroup(METALAKE, ROLE, groups.get(i));
    }

    reset(authorizationPlugin);
    List<BulkItemResult<String>> userResults =
        accessControlManager.removeUsers(METALAKE, users, Optional.empty());
    List<BulkItemResult<String>> groupResults =
        accessControlManager.removeGroups(METALAKE, groups, Optional.empty());

    Assertions.assertEquals(2, userResults.size());
    Assertions.assertEquals(2, groupResults.size());
    userResults.forEach(result -> Assertions.assertTrue(result.succeeded()));
    groupResults.forEach(result -> Assertions.assertTrue(result.succeeded()));
    ArgumentCaptor<List<Role>> userRoles = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<User> removedUsers = ArgumentCaptor.forClass(User.class);
    verify(authorizationPlugin, Mockito.times(2))
        .onRevokedRolesFromUser(userRoles.capture(), removedUsers.capture());
    Assertions.assertEquals(
        users, removedUsers.getAllValues().stream().map(User::name).sorted().toList());
    userRoles
        .getAllValues()
        .forEach(r -> Assertions.assertEquals(ROLE, r.stream().map(Role::name).toList()));
    ArgumentCaptor<List<Role>> groupRoles = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Group> removedGroups = ArgumentCaptor.forClass(Group.class);
    verify(authorizationPlugin, Mockito.times(2))
        .onRevokedRolesFromGroup(groupRoles.capture(), removedGroups.capture());
    Assertions.assertEquals(
        groups, removedGroups.getAllValues().stream().map(Group::name).sorted().toList());
    groupRoles
        .getAllValues()
        .forEach(r -> Assertions.assertEquals(ROLE, r.stream().map(Role::name).toList()));
  }

  @Test
  public void testBulkRemoveUsersReportsPluginFailurePerItem() {
    List<String> users = List.of("bulk_failure_user_1", "bulk_failure_user_2");
    for (String user : users) {
      accessControlManager.addUser(METALAKE, user);
      accessControlManager.grantRolesToUser(METALAKE, ROLE, user);
    }

    reset(authorizationPlugin);
    RuntimeException failure = new RuntimeException("plugin failed once");
    List<BulkItemResult<String>> results;
    try {
      Mockito.doThrow(failure)
          .doReturn(Boolean.TRUE)
          .when(authorizationPlugin)
          .onRevokedRolesFromUser(any(), any());
      results = accessControlManager.removeUsers(METALAKE, users, Optional.empty());
      verify(authorizationPlugin, Mockito.times(2)).onRevokedRolesFromUser(any(), any());
    } finally {
      reset(authorizationPlugin);
    }

    Assertions.assertEquals(2, results.size());
    // The first user is removed, but its plugin cleanup failed, so its item is a failure.
    Assertions.assertFalse(results.get(0).succeeded());
    Assertions.assertTrue(results.get(1).succeeded());
    for (String user : users) {
      Assertions.assertThrows(
          NoSuchUserException.class, () -> accessControlManager.getUser(METALAKE, user));
    }
  }

  @Test
  public void testRemoveUserReportsAuthorizationPluginFailureAfterDelete() {
    String user = "user_plugin_failure";
    accessControlManager.addUser(METALAKE, user);
    accessControlManager.grantRolesToUser(METALAKE, ROLE, user);

    reset(authorizationPlugin);
    RuntimeException failure = new RuntimeException("plugin failed");
    Mockito.doThrow(failure).when(authorizationPlugin).onRevokedRolesFromUser(any(), any());
    try {
      AuthorizationPluginException thrown =
          Assertions.assertThrows(
              AuthorizationPluginException.class,
              () -> accessControlManager.removeUser(METALAKE, user));
      Assertions.assertSame(failure, thrown.getCause());
    } finally {
      reset(authorizationPlugin);
    }
    // The plugin is called after the delete, so the user is removed from Gravitino.
    Assertions.assertThrows(
        NoSuchUserException.class, () -> accessControlManager.getUser(METALAKE, user));
    // Known limitation: a retry finds no user and does not reach the plugin again.
    Assertions.assertFalse(accessControlManager.removeUser(METALAKE, user));
    Mockito.verifyNoInteractions(authorizationPlugin);
  }

  @Test
  public void testRemoveGroupReportsAuthorizationPluginFailureAfterDelete() {
    String group = "group_plugin_failure";
    accessControlManager.addGroup(METALAKE, group);
    accessControlManager.grantRolesToGroup(METALAKE, ROLE, group);

    reset(authorizationPlugin);
    RuntimeException failure = new RuntimeException("plugin failed");
    Mockito.doThrow(failure).when(authorizationPlugin).onRevokedRolesFromGroup(any(), any());
    try {
      AuthorizationPluginException thrown =
          Assertions.assertThrows(
              AuthorizationPluginException.class,
              () -> accessControlManager.removeGroup(METALAKE, group));
      Assertions.assertSame(failure, thrown.getCause());
    } finally {
      reset(authorizationPlugin);
    }
    Assertions.assertThrows(
        NoSuchGroupException.class, () -> accessControlManager.getGroup(METALAKE, group));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFailedRemoveDoesNotCallAuthorizationPlugin(boolean group) throws IOException {
    EntityStore store = Mockito.mock(EntityStore.class);
    Mockito.when(store.get(Mockito.eq(NameIdentifier.of(METALAKE)), any(), any()))
        .thenReturn(metalakeEntity);
    String name = group ? "group_delete_fails" : "user_delete_fails";
    NameIdentifier ident =
        group
            ? AuthorizationUtils.ofGroup(METALAKE, name)
            : AuthorizationUtils.ofUser(METALAKE, name);
    Entity.EntityType type = group ? Entity.EntityType.GROUP : Entity.EntityType.USER;
    if (group) {
      GroupEntity groupWithRole =
          GroupEntity.builder()
              .withNamespace(groupEntity.namespace())
              .withId(100L)
              .withName(name)
              .withRoleNames(ROLE)
              .withRoleIds(List.of(roleEntity.id()))
              .withAuditInfo(auditInfo)
              .build();
      Mockito.when(store.get(ident, type, GroupEntity.class)).thenReturn(groupWithRole);
    } else {
      UserEntity userWithRole =
          UserEntity.builder()
              .withNamespace(userEntity.namespace())
              .withId(100L)
              .withName(name)
              .withRoleNames(ROLE)
              .withRoleIds(List.of(roleEntity.id()))
              .withAuditInfo(auditInfo)
              .build();
      Mockito.when(store.get(ident, type, UserEntity.class)).thenReturn(userWithRole);
    }
    NameIdentifier roleIdent = AuthorizationUtils.ofRole(METALAKE, roleEntity.name());
    Mockito.when(store.get(roleIdent, Entity.EntityType.ROLE, RoleEntity.class))
        .thenReturn(roleEntity);
    // Another request removed the principal between the read and the delete.
    Mockito.when(store.delete(ident, type)).thenReturn(false);
    UserGroupManager manager = new UserGroupManager(store, new RandomIdGenerator());

    reset(authorizationPlugin);
    Assertions.assertFalse(
        group ? manager.removeGroup(METALAKE, name) : manager.removeUser(METALAKE, name));
    // The remove read the principal and its role, so it reached the delete with a non-empty role
    // list, and only the failed delete kept it from calling the plugin.
    verify(store).get(roleIdent, Entity.EntityType.ROLE, RoleEntity.class);
    verify(store).delete(ident, type);
    Mockito.verifyNoInteractions(authorizationPlugin);
  }

  @Test
  public void testRemoveUserStoreFailuresDoNotCallAuthorizationPlugin() throws IOException {
    String name = "user_store_fails";
    NameIdentifier ident = AuthorizationUtils.ofUser(METALAKE, name);
    UserEntity userWithRole =
        UserEntity.builder()
            .withNamespace(userEntity.namespace())
            .withId(101L)
            .withName(name)
            .withRoleNames(ROLE)
            .withRoleIds(List.of(roleEntity.id()))
            .withAuditInfo(auditInfo)
            .build();
    IOException failure = new IOException("store unavailable");

    // The read fails: nothing is deleted and the plugin is not called.
    EntityStore readFails = Mockito.mock(EntityStore.class);
    Mockito.when(readFails.get(Mockito.eq(NameIdentifier.of(METALAKE)), any(), any()))
        .thenReturn(metalakeEntity);
    Mockito.when(readFails.get(ident, Entity.EntityType.USER, UserEntity.class)).thenThrow(failure);
    reset(authorizationPlugin);
    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                new UserGroupManager(readFails, new RandomIdGenerator())
                    .removeUser(METALAKE, name));
    Assertions.assertSame(failure, thrown.getCause());
    verify(readFails, Mockito.never()).delete(any(), any());

    // The delete fails: the plugin is not called.
    EntityStore deleteFails = Mockito.mock(EntityStore.class);
    Mockito.when(deleteFails.get(Mockito.eq(NameIdentifier.of(METALAKE)), any(), any()))
        .thenReturn(metalakeEntity);
    Mockito.when(deleteFails.get(ident, Entity.EntityType.USER, UserEntity.class))
        .thenReturn(userWithRole);
    Mockito.when(
            deleteFails.get(
                AuthorizationUtils.ofRole(METALAKE, roleEntity.name()),
                Entity.EntityType.ROLE,
                RoleEntity.class))
        .thenReturn(roleEntity);
    Mockito.when(deleteFails.delete(ident, Entity.EntityType.USER)).thenThrow(failure);
    thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                new UserGroupManager(deleteFails, new RandomIdGenerator())
                    .removeUser(METALAKE, name));
    Assertions.assertSame(failure, thrown.getCause());
    Mockito.verifyNoInteractions(authorizationPlugin);
  }

  @Test
  public void testRemoveUserRevokesRolesFilteredPerCatalog() throws Exception {
    AuthorizationPlugin pluginA = Mockito.mock(AuthorizationPlugin.class);
    AuthorizationPlugin pluginB = Mockito.mock(AuthorizationPlugin.class);
    RoleEntity roleA = putRole("role_on_a", SecurableObjects.ofCatalog("catalog_a", USE_CATALOG));
    RoleEntity roleB = putRole("role_on_b", SecurableObjects.ofCatalog("catalog_b", USE_CATALOG));
    String user = putUserWithRoles("user_two_catalogs", roleA, roleB);

    withCatalogs(
        Map.of("catalog_a", pluginA, "catalog_b", pluginB),
        () -> Assertions.assertTrue(accessControlManager.removeUser(METALAKE, user)));

    // Each plugin is called once, with each role holding only its own catalog's objects.
    for (Map.Entry<String, AuthorizationPlugin> entry :
        Map.of("catalog_a", pluginA, "catalog_b", pluginB).entrySet()) {
      ArgumentCaptor<List<Role>> roles = ArgumentCaptor.forClass(List.class);
      verify(entry.getValue()).onRevokedRolesFromUser(roles.capture(), any());
      Assertions.assertEquals(2, roles.getValue().size());
      for (Role role : roles.getValue()) {
        role.securableObjects()
            .forEach(object -> Assertions.assertEquals(entry.getKey(), object.fullName()));
      }
      Assertions.assertEquals(
          1, roles.getValue().stream().mapToInt(r -> r.securableObjects().size()).sum());
    }
  }

  @Test
  public void testRemoveUserPluginFailureDoesNotStopOtherCatalogs() throws Exception {
    AuthorizationPlugin pluginA = Mockito.mock(AuthorizationPlugin.class);
    AuthorizationPlugin pluginB = Mockito.mock(AuthorizationPlugin.class);
    AuthorizationPlugin pluginC = Mockito.mock(AuthorizationPlugin.class);
    RuntimeException failureA = new RuntimeException("catalog_a plugin failed");
    RuntimeException failureC = new RuntimeException("catalog_c plugin failed");
    Mockito.doThrow(failureA).when(pluginA).onRevokedRolesFromUser(any(), any());
    Mockito.doThrow(failureC).when(pluginC).onRevokedRolesFromUser(any(), any());
    RoleEntity role =
        putRole(
            "role_on_three_catalogs",
            SecurableObjects.ofCatalog("catalog_a", USE_CATALOG),
            SecurableObjects.ofCatalog("catalog_b", USE_CATALOG),
            SecurableObjects.ofCatalog("catalog_c", USE_CATALOG));
    String user = putUserWithRoles("user_partial_failure", role);

    withCatalogs(
        Map.of("catalog_a", pluginA, "catalog_b", pluginB, "catalog_c", pluginC),
        () -> {
          AuthorizationPluginException thrown =
              Assertions.assertThrows(
                  AuthorizationPluginException.class,
                  () -> accessControlManager.removeUser(METALAKE, user));
          Assertions.assertSame(failureA, thrown.getCause());
          Assertions.assertEquals(List.of(failureC), List.of(thrown.getSuppressed()));
          Assertions.assertTrue(thrown.getMessage().contains("catalog_a"));
          Assertions.assertTrue(thrown.getMessage().contains("catalog_c"));
          Assertions.assertFalse(thrown.getMessage().contains("catalog_b"));
        });

    // The failing catalogs do not stop the others, and the user is removed.
    verify(pluginA).onRevokedRolesFromUser(any(), any());
    verify(pluginB).onRevokedRolesFromUser(any(), any());
    verify(pluginC).onRevokedRolesFromUser(any(), any());
    Assertions.assertFalse(
        entityStore.exists(AuthorizationUtils.ofUser(METALAKE, user), Entity.EntityType.USER));
  }

  @Test
  public void testRemoveUserWhenRoleCatalogCannotLoadStillRevokesOtherCatalogs() throws Exception {
    AuthorizationPlugin pluginB = Mockito.mock(AuthorizationPlugin.class);
    RoleEntity role =
        putRole(
            "role_on_broken_catalog",
            SecurableObjects.ofCatalog("broken_catalog", USE_CATALOG),
            SecurableObjects.ofCatalog("catalog_b", USE_CATALOG));
    String user = putUserWithRoles("user_broken_catalog", role);

    // broken_catalog is not among the catalogs, so loading it throws NoSuchCatalogException.
    withCatalogs(
        Map.of("catalog_b", pluginB),
        () -> {
          AuthorizationPluginException thrown =
              Assertions.assertThrows(
                  AuthorizationPluginException.class,
                  () -> accessControlManager.removeUser(METALAKE, user));
          Assertions.assertInstanceOf(NoSuchCatalogException.class, thrown.getCause());
        });

    verify(pluginB).onRevokedRolesFromUser(any(), any());
    Assertions.assertFalse(
        entityStore.exists(AuthorizationUtils.ofUser(METALAKE, user), Entity.EntityType.USER));
  }

  @Test
  public void testRemoveGroupWithMetalakeScopedRoleRevokesInAllCatalogs() throws Exception {
    AuthorizationPlugin pluginA = Mockito.mock(AuthorizationPlugin.class);
    AuthorizationPlugin pluginB = Mockito.mock(AuthorizationPlugin.class);
    RoleEntity role =
        putRole(
            "role_on_metalake",
            SecurableObjects.ofMetalake(
                METALAKE, Lists.newArrayList(Privileges.SelectTable.allow())));
    String group = "group_metalake_role";
    entityStore.put(
        GroupEntity.builder()
            .withNamespace(groupEntity.namespace())
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(group)
            .withRoleNames(List.of(role.name()))
            .withRoleIds(List.of(role.id()))
            .withAuditInfo(auditInfo)
            .build(),
        false);

    withCatalogs(
        Map.of("catalog_a", pluginA, "catalog_b", pluginB),
        () -> Assertions.assertTrue(accessControlManager.removeGroup(METALAKE, group)));

    verify(pluginA).onRevokedRolesFromGroup(any(), any());
    verify(pluginB).onRevokedRolesFromGroup(any(), any());
  }

  @Test
  public void testRemoveUserSkipsRolesWithoutConnectorPrivilegesOrDeleted() throws Exception {
    AuthorizationPlugin pluginA = Mockito.mock(AuthorizationPlugin.class);
    RoleEntity manageUsers =
        putRole(
            "role_manage_users",
            SecurableObjects.ofMetalake(
                METALAKE, Lists.newArrayList(Privileges.ManageUsers.allow())));
    String onlyManageUsers = putUserWithRoles("user_manage_users_only", manageUsers);

    withCatalogs(
        Map.of("catalog_a", pluginA),
        () -> Assertions.assertTrue(accessControlManager.removeUser(METALAKE, onlyManageUsers)));
    Mockito.verifyNoInteractions(pluginA);

    // A role deleted before the remove is skipped, and the existing role is still revoked.
    RoleEntity roleA =
        putRole("role_kept_on_a", SecurableObjects.ofCatalog("catalog_a", USE_CATALOG));
    String user = "user_with_deleted_role";
    entityStore.put(
        UserEntity.builder()
            .withNamespace(userEntity.namespace())
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(user)
            .withRoleNames(List.of("role_already_deleted", roleA.name()))
            .withRoleIds(List.of(RandomIdGenerator.INSTANCE.nextId(), roleA.id()))
            .withAuditInfo(auditInfo)
            .build(),
        false);

    withCatalogs(
        Map.of("catalog_a", pluginA),
        () -> Assertions.assertTrue(accessControlManager.removeUser(METALAKE, user)));
    ArgumentCaptor<List<Role>> roles = ArgumentCaptor.forClass(List.class);
    verify(pluginA).onRevokedRolesFromUser(roles.capture(), any());
    Assertions.assertEquals(
        List.of(roleA.name()), roles.getValue().stream().map(Role::name).toList());
  }

  @Test
  public void testRemoveUserWithoutAuthorizationPluginSucceeds() throws Exception {
    RoleEntity roleA =
        putRole("role_on_plain_catalog", SecurableObjects.ofCatalog("catalog_a", USE_CATALOG));
    String user = putUserWithRoles("user_plain_catalog", roleA);
    Map<String, AuthorizationPlugin> noPlugin = new HashMap<>();
    noPlugin.put("catalog_a", null);

    withCatalogs(
        noPlugin, () -> Assertions.assertTrue(accessControlManager.removeUser(METALAKE, user)));
    Assertions.assertFalse(
        entityStore.exists(AuthorizationUtils.ofUser(METALAKE, user), Entity.EntityType.USER));
  }

  private static RoleEntity putRole(String name, SecurableObject... objects) throws IOException {
    RoleEntity role =
        RoleEntity.builder()
            .withNamespace(roleEntity.namespace())
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withProperties(Maps.newHashMap())
            .withSecurableObjects(Lists.newArrayList(objects))
            .withAuditInfo(auditInfo)
            .build();
    entityStore.put(role, false);
    return role;
  }

  private static String putUserWithRoles(String name, RoleEntity... roles) throws IOException {
    entityStore.put(
        UserEntity.builder()
            .withNamespace(userEntity.namespace())
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withRoleNames(Arrays.stream(roles).map(RoleEntity::name).toList())
            .withRoleIds(Arrays.stream(roles).map(RoleEntity::id).toList())
            .withAuditInfo(auditInfo)
            .build(),
        false);
    return name;
  }

  /**
   * Runs {@code action} with a catalog manager that has the given catalogs, each with its own
   * authorization plugin (or none for a null value). Loading any other catalog throws {@link
   * NoSuchCatalogException}. The shared catalog manager is restored afterwards.
   */
  private static void withCatalogs(Map<String, AuthorizationPlugin> plugins, Executable action)
      throws Exception {
    CatalogManager localCatalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(localCatalogManager.listCatalogs(any()))
        .thenReturn(
            plugins.keySet().stream()
                .sorted()
                .map(name -> NameIdentifier.of(METALAKE, name))
                .toArray(NameIdentifier[]::new));
    Mockito.doAnswer(
            invocation -> {
              NameIdentifier ident = invocation.getArgument(0);
              if (!plugins.containsKey(ident.name())) {
                throw new NoSuchCatalogException("Catalog %s does not exist", ident);
              }
              BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
              Mockito.when(catalog.name()).thenReturn(ident.name());
              Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(plugins.get(ident.name()));
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(localCatalogManager)
        .doWithCatalog(any(), any());

    GravitinoEnv env = GravitinoEnv.getInstance();
    Object original = FieldUtils.readField(env, "catalogManager", true);
    FieldUtils.writeField(env, "catalogManager", localCatalogManager, true);
    try {
      action.execute();
    } catch (Throwable t) {
      Throwables.throwIfUnchecked(t);
      Throwables.throwIfInstanceOf(t, Exception.class);
      throw new AssertionError(t);
    } finally {
      FieldUtils.writeField(env, "catalogManager", original, true);
    }
  }
}
