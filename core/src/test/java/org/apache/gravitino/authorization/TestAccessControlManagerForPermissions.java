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
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
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

<<<<<<< HEAD
=======
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

  @ParameterizedTest
  @CsvSource({
    "CREATE_MODEL,REGISTER_MODEL,ALLOW",
    "REGISTER_MODEL,CREATE_MODEL,ALLOW",
    "CREATE_MODEL_VERSION,LINK_MODEL_VERSION,ALLOW",
    "LINK_MODEL_VERSION,CREATE_MODEL_VERSION,ALLOW",
    "CREATE_MODEL,REGISTER_MODEL,DENY",
    "REGISTER_MODEL,CREATE_MODEL,DENY",
    "CREATE_MODEL_VERSION,LINK_MODEL_VERSION,DENY",
    "LINK_MODEL_VERSION,CREATE_MODEL_VERSION,DENY"
  })
  void testGrantEquivalentPrivilegeIsIdempotent(
      Privilege.Name heldName, Privilege.Name grantedName, Privilege.Condition condition)
      throws IOException {
    String name = "alias_" + heldName + "_" + condition;
    Privilege held =
        condition == Privilege.Condition.ALLOW
            ? Privileges.allow(heldName)
            : Privileges.deny(heldName);
    Privilege granted =
        condition == Privilege.Condition.ALLOW
            ? Privileges.allow(grantedName)
            : Privileges.deny(grantedName);
    MetadataObject object = MetadataObjects.parse(METALAKE, MetadataObject.Type.METALAKE);
    entityStore.put(
        RoleEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                List.of(SecurableObjects.parse(METALAKE, object.type(), List.of(held))))
            .build(),
        false);
    PermissionManager manager = new PermissionManager(entityStore, Mockito.mock(RoleManager.class));
    reset(authorizationPlugin);
    Role role = manager.grantPrivilegesToRole(METALAKE, name, object, Set.of(granted));
    Assertions.assertEquals(List.of(held), role.securableObjects().get(0).privileges());
    Mockito.verifyNoInteractions(authorizationPlugin);

    role =
        manager.grantPrivilegesToRole(
            METALAKE, name, object, Set.of(granted, Privileges.CreateTable.allow()));
    Set<Privilege> expected = Set.of(held, Privileges.CreateTable.allow());
    Assertions.assertEquals(expected, Set.copyOf(role.securableObjects().get(0).privileges()));
    Privilege conflicting =
        condition == Privilege.Condition.ALLOW
            ? Privileges.deny(grantedName)
            : Privileges.allow(grantedName);
    Assertions.assertThrows(
        IllegalPrivilegeException.class,
        () -> manager.grantPrivilegesToRole(METALAKE, name, object, Set.of(conflicting)));
    Assertions.assertEquals(
        expected,
        Set.copyOf(
            accessControlManager.getRole(METALAKE, name).securableObjects().get(0).privileges()));
    role = manager.revokePrivilegesFromRole(METALAKE, name, object, Set.of(granted));
    Assertions.assertEquals(
        List.of(Privileges.CreateTable.allow()), role.securableObjects().get(0).privileges());
  }

  @ParameterizedTest
  @CsvSource({
    "CREATE_MODEL,REGISTER_MODEL",
    "REGISTER_MODEL,CREATE_MODEL",
    "CREATE_MODEL_VERSION,LINK_MODEL_VERSION",
    "LINK_MODEL_VERSION,CREATE_MODEL_VERSION"
  })
  void testEquivalentGrantPreservesStoredDuplicatePrivileges(
      Privilege.Name heldName, Privilege.Name grantedName)
      throws IOException, IllegalAccessException {
    PermissionManager manager = new PermissionManager(entityStore, Mockito.mock(RoleManager.class));
    MetadataObject object = MetadataObjects.parse(METALAKE, MetadataObject.Type.METALAKE);
    for (Privilege.Condition condition : Privilege.Condition.values()) {
      Privilege held =
          condition == Privilege.Condition.ALLOW
              ? Privileges.allow(heldName)
              : Privileges.deny(heldName);
      Privilege granted =
          condition == Privilege.Condition.ALLOW
              ? Privileges.allow(grantedName)
              : Privileges.deny(grantedName);
      for (boolean aliasDuplicate : new boolean[] {false, true}) {
        String name = "duplicates_" + heldName + "_" + condition + "_" + aliasDuplicate;
        List<Privilege> stored = List.of(held, aliasDuplicate ? granted : held);
        SecurableObject storedObject = SecurableObjects.parse(METALAKE, object.type(), stored);
        // Simulate stored duplicates without the constructor's automatic deduplication.
        FieldUtils.writeField(storedObject, "privileges", stored, true);
        entityStore.put(
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName(name)
                .withNamespace(AuthorizationUtils.ofRoleNamespace(METALAKE))
                .withAuditInfo(auditInfo)
                .withSecurableObjects(List.of(storedObject))
                .build(),
            false);
        reset(authorizationPlugin);
        Role role = manager.grantPrivilegesToRole(METALAKE, name, object, Set.of(granted));
        Assertions.assertEquals(stored, role.securableObjects().get(0).privileges());
        Assertions.assertEquals(
            stored,
            accessControlManager.getRole(METALAKE, name).securableObjects().get(0).privileges());
        Mockito.verifyNoInteractions(authorizationPlugin);
      }
    }
  }

>>>>>>> cd678ef91 ([#14723] fix(authz): Correct role grants, assignment audits and metadata validation (#14728))
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
}
