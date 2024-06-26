/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static org.mockito.ArgumentMatchers.any;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestAuthorizationUtils {

  String metalake = "metalake";

  @Test
  void testCreateNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertEquals(AuthorizationUtils.ofUserNamespace(metalake), user.namespace());
    Assertions.assertEquals("user", user.name());
    Assertions.assertEquals(AuthorizationUtils.ofGroupNamespace(metalake), group.namespace());
    Assertions.assertEquals("group", group.name());
    Assertions.assertEquals(AuthorizationUtils.ofRoleNamespace(metalake), role.namespace());
    Assertions.assertEquals("role", role.name());
  }

  @Test
  void testCreateNameIdentifierWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, ""));
  }

  @Test
  void testCreateNamespace() {
    Namespace namespace = AuthorizationUtils.ofUserNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("user", namespace.level(2));

    namespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("group", namespace.level(2));

    namespace = AuthorizationUtils.ofRoleNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("role", namespace.level(2));
  }

  @Test
  void testCreateNamespaceWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(""));
  }

  @Test
  void testCheckNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUser(user));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroup(group));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRole(role));

    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkUser(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkGroup(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkRole(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkUser(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkGroup(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkRole(NameIdentifier.of("")));
  }

  @Test
  void testCheckNamespace() {
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    Namespace groupNamespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Namespace roleNamespace = AuthorizationUtils.ofRoleNamespace(metalake);

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUserNamespace(userNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroupNamespace(groupNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRoleNamespace(roleNamespace));

    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkUserNamespace(Namespace.of("a", "b")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkGroupNamespace(Namespace.of("a")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkRoleNamespace(Namespace.of("a", "b", "c", "d")));
  }

  @Test
  void testSatisfyAdminPrivileges() throws IllegalAccessException {
    AccessControlManager manager = Mockito.mock(AccessControlManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlManager", manager, true);
    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList());

    // case 1: User has empty roles
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.allow()))));

    // case 2: Allow to add metalake admin
    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(roleEntity.namespace())
        .thenReturn(Namespace.of(Entity.SYSTEM_METALAKE_RESERVED_NAME));
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.allow()))));
    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.allow()))));

    // case 3: Deny to add metalake admin
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.allow()))));

    // case 4: Allow to remove metalake admin
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(
                    Lists.newArrayList(Privileges.RemoveUser.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.RemoveUser.allow()))));

    // case 5: Deny to remove metalake admin
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.AddUser.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.RemoveUser.allow()))));
  }

  @Test
  void testEntityComplexCases() throws IllegalAccessException {
    AccessControlManager manager = Mockito.mock(AccessControlManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlManager", manager, true);

    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(roleEntity.namespace()).thenReturn(Namespace.of("metalake"));
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseMetalake.allow()))));
    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));

    // case 1: Allow one entity to execute the operation
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.UseMetalake.allow()))));

    // case 2: Deny one entity to execute the operation
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseMetalake.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.UseMetalake.allow()))));

    // case 3: Allow one entity to execute if they don't need any privilege
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake", SecurableObjects.ofMetalake("metalake", Lists.newArrayList())));

    // case 4: Parent has the privilege, so the child can use this privilege
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // case 5: Parent denies the privilege, but the child allows the privilege
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseCatalog.deny()))));
    RoleEntity anotherRoleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(anotherRoleEntity.namespace()).thenReturn(Namespace.of("metalake"));
    Mockito.when(anotherRoleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Mockito.when(manager.listRolesByUser(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity, anotherRoleEntity));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseCatalog.deny())),
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // case 6: Parent allows the privilege, but the child denies the privilege
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Mockito.when(anotherRoleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.deny()))));
    Mockito.when(manager.listRolesByUser(any(), any()))
        .thenReturn(Lists.newArrayList(roleEntity, anotherRoleEntity));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.UseCatalog.allow())),
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // case 7: Deny one entity if there are no privileges
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog1", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // case 8: Allow the operation if the entity can satisfy any of multiple privileges
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog",
                    Lists.newArrayList(
                        Privileges.UseCatalog.deny(), Privileges.CreateSchema.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog1",
                Lists.newArrayList(
                    Privileges.UseCatalog.allow(), Privileges.CreateSchema.allow()))));
  }

  @Test
  void testSatisfyEntities() throws IllegalAccessException {
    AccessControlManager manager = Mockito.mock(AccessControlManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlManager", manager, true);

    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(roleEntity.namespace()).thenReturn(Namespace.of("metalake"));
    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));

    // case 1: Test cases related to metalake
    // Allow to operate metalake
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(
                    Lists.newArrayList(Privileges.CreateMetalake.allow())),
                SecurableObjects.ofMetalake(
                    "metalake",
                    Lists.newArrayList(
                        Privileges.UseMetalake.allow(), Privileges.ManageMetalake.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(
                Lists.newArrayList(Privileges.CreateMetalake.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.UseMetalake.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.ManageMetalake.allow()))));

    // Deny to operate metalake
    Mockito.when(roleEntity.securableObjects())
        .thenReturn(
            Lists.newArrayList(
                SecurableObjects.ofAllMetalakes(
                    Lists.newArrayList(Privileges.CreateMetalake.deny())),
                SecurableObjects.ofMetalake(
                    "metalake",
                    Lists.newArrayList(
                        Privileges.UseMetalake.deny(), Privileges.ManageMetalake.deny()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            Entity.SYSTEM_METALAKE_RESERVED_NAME,
            SecurableObjects.ofAllMetalakes(
                Lists.newArrayList(Privileges.CreateMetalake.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.UseMetalake.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.ManageMetalake.allow()))));

    // case 2: Test cases related to catalog
    // Allow to operate catalog
    SecurableObject metalake =
        SecurableObjects.ofMetalake(
            "metalake", Lists.newArrayList(Privileges.CreateCatalog.allow()));
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(
                Privileges.UseCatalog.allow(),
                Privileges.AlterCatalog.allow(),
                Privileges.DropCatalog.allow()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(metalake, catalog));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.CreateCatalog.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.AlterCatalog.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.DropCatalog.allow()))));

    // Deny to operate catalog
    metalake =
        SecurableObjects.ofMetalake(
            "metalake", Lists.newArrayList(Privileges.CreateCatalog.deny()));
    catalog =
        SecurableObjects.ofCatalog(
            "catalog",
            Lists.newArrayList(
                Privileges.UseCatalog.deny(),
                Privileges.AlterCatalog.deny(),
                Privileges.DropCatalog.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(metalake, catalog));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofMetalake(
                "metalake", Lists.newArrayList(Privileges.CreateMetalake.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.AlterCatalog.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.DropCatalog.allow()))));

    // case 3: Test cases related to schema
    // Allow to operate schema
    catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.CreateSchema.allow()));
    SecurableObject schema =
        SecurableObjects.ofSchema(
            catalog,
            "schema",
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.AlterSchema.allow(),
                Privileges.DropSchema.allow()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(catalog, schema));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.CreateSchema.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.UseSchema.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.AlterSchema.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.DropSchema.allow()))));

    // Deny to operate schema
    catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.CreateSchema.deny()));
    schema =
        SecurableObjects.ofSchema(
            catalog,
            "schema",
            Lists.newArrayList(
                Privileges.UseSchema.deny(),
                Privileges.AlterSchema.deny(),
                Privileges.DropSchema.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(catalog, schema));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.CreateSchema.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.UseSchema.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.AlterSchema.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.DropSchema.allow()))));

    // case 4: Test cases related to topic
    // Allow to operate topic
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateTopic.allow()));
    SecurableObject topic =
        SecurableObjects.ofTopic(
            schema,
            "topic",
            Lists.newArrayList(
                Privileges.ReadTopic.allow(),
                Privileges.WriteTopic.allow(),
                Privileges.DropTopic.allow()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, topic));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateTopic.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.ReadTopic.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.WriteTopic.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.DropTopic.allow()))));
    // Deny to operate topic
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateTopic.deny()));
    topic =
        SecurableObjects.ofTopic(
            schema,
            "topic",
            Lists.newArrayList(
                Privileges.ReadTopic.deny(),
                Privileges.WriteTopic.deny(),
                Privileges.DropTopic.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, topic));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateTopic.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.ReadTopic.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.WriteTopic.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTopic(
                schema, "topic", Lists.newArrayList(Privileges.DropTopic.allow()))));

    // case 5: Test cases related to table
    // Allow to operate table
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateTable.allow()));
    SecurableObject table =
        SecurableObjects.ofTable(
            schema,
            "table",
            Lists.newArrayList(
                Privileges.ReadTable.allow(),
                Privileges.WriteTable.allow(),
                Privileges.DropTable.allow()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, table));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateTable.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.ReadTable.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.WriteTable.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.DropTable.allow()))));

    // Deny to operate table
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateTable.deny()));
    table =
        SecurableObjects.ofTable(
            schema,
            "table",
            Lists.newArrayList(
                Privileges.ReadTable.deny(),
                Privileges.WriteTable.deny(),
                Privileges.DropTable.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, table));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateTable.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.ReadTable.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.WriteTable.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofTable(
                schema, "table", Lists.newArrayList(Privileges.DropTable.allow()))));

    // case 6: Test cases related to fileset
    // Allow to operate fileset
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateFileset.allow()));
    SecurableObject fileset =
        SecurableObjects.ofFileset(
            schema,
            "fileset",
            Lists.newArrayList(
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow(),
                Privileges.DropFileset.allow()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, fileset));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateFileset.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()))));
    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.WriteFileset.allow()))));

    Assertions.assertTrue(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.DropFileset.allow()))));

    // Deny to operate fileset
    schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.CreateFileset.deny()));
    fileset =
        SecurableObjects.ofFileset(
            schema,
            "fileset",
            Lists.newArrayList(
                Privileges.ReadFileset.deny(),
                Privileges.WriteFileset.deny(),
                Privileges.DropFileset.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(schema, fileset));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofSchema(
                catalog, "schema", Lists.newArrayList(Privileges.CreateFileset.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()))));
    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.WriteFileset.allow()))));

    Assertions.assertFalse(
        AuthorizationUtils.satisfyPrivileges(
            "metalake",
            SecurableObjects.ofFileset(
                schema, "fileset", Lists.newArrayList(Privileges.DropFileset.allow()))));
  }

  @Test
  void testSatisfyAccessControlPrivileges() throws IllegalAccessException {
    AccessControlManager manager = Mockito.mock(AccessControlManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlManager", manager, true);
    SecurableObject metalake =
            SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.AddUser.allow(), Privileges.RemoveUser.allow(), Privileges.GetUser.allow(), Privileges.AddGroup.allow(), Privileges.RemoveGroup.allow(), Privileges.GetGroup.allow(), Privileges.GrantRole.allow(), Privileges.RevokeRole.allow(), Privileges.CreateRole.allow(), Privileges.DeleteRole.allow(), Privileges.GetRole.allow()));
    RoleEntity roleEntity = Mockito.mock(RoleEntity.class);
    Mockito.when(roleEntity.namespace()).thenReturn(Namespace.of("metalake"));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(metalake));
    Mockito.when(manager.listRolesByUser(any(), any())).thenReturn(Lists.newArrayList(roleEntity));

    // Allow to operate user
    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.AddUser.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RemoveUser.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetUser.allow()))));

    // Allow to operate group
    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.AddGroup.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RemoveGroup.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetGroup.allow()))));
    // Allow to operate role
    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.CreateRole.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.DeleteRole.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetRole.allow()))));
    // Allow to operate permission
    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GrantRole.allow()))));

    Assertions.assertTrue(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RevokeRole.allow()))));

    metalake =
            SecurableObjects.ofMetalake(
                    "metalake", Lists.newArrayList(Privileges.AddUser.deny(), Privileges.RemoveUser.deny(), Privileges.GetUser.deny(), Privileges.AddGroup.deny(), Privileges.RemoveGroup.deny(), Privileges.GetGroup.deny(), Privileges.GrantRole.deny(), Privileges.RevokeRole.deny(), Privileges.CreateRole.deny(), Privileges.DeleteRole.deny(), Privileges.GetRole.deny()));
    Mockito.when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(metalake));

    // Deny to operate user
    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.AddUser.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RemoveUser.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetUser.allow()))));

    // Deny to operate group
    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.AddGroup.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RemoveGroup.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetGroup.allow()))));
    // Deny to operate role
    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.CreateRole.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.DeleteRole.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GetRole.allow()))));
    // Deny to operate permission
    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.GrantRole.allow()))));

    Assertions.assertFalse(
            AuthorizationUtils.satisfyPrivileges(
                    "metalake",
                    SecurableObjects.ofMetalake(
                            "metalake", Lists.newArrayList(Privileges.RevokeRole.allow()))));
  }
}
