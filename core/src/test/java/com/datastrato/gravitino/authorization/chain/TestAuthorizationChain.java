/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationManager;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.authorization.chain.authorization1.TestAuthorizationOperations1;
import com.datastrato.gravitino.authorization.chain.authorization2.TestAuthorizationOperations2;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAuthorizationChain {
  private static AuthorizationManager authorizationManager;
  private static Config config;
  private static CatalogEntity catalogTest1;
  private static CatalogEntity catalogTest2;
  private static RoleEntity roleEntity;
  private static SecurableObject dropTable;
  private static AuditInfo auditInfo;

  @BeforeAll
  public static void setUp() throws Exception {
    auditInfo = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    catalogTest1 =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog-test1")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test1"))
            .withProvider("hive")
            .withAuditInfo(auditInfo)
            .build();

    catalogTest2 =
        CatalogEntity.builder()
            .withId(2L)
            .withName("catalog-test2")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test2"))
            .withProvider("hive")
            .withAuditInfo(auditInfo)
            .build();

    roleEntity =
        RoleEntity.builder()
            .withId(1L)
            .withName("role1")
            .withProperties(Maps.newHashMap())
            .withSecurableObjects(
                Lists.newArrayList(
                    SecurableObjects.ofNamespace(
                        MetadataObject.Type.TABLE,
                        Namespace.of("catalog", "schema", "table1"),
                        Lists.newArrayList(Privileges.ReadTable.allow()))))
            .withAuditInfo(auditInfo)
            .build();

    dropTable =
        SecurableObjects.ofNamespace(
            MetadataObject.Type.TABLE,
            Namespace.of("catalog", "schema", "table1"),
            Lists.newArrayList(Privileges.DropTable.allow()));

    config = new Config(false) {};
    config.set(Configs.AUTHORIZATION_LOAD_ISOLATED, false);

    authorizationManager = new AuthorizationManager(config);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (authorizationManager != null) {
      authorizationManager.close();
    }
  }

  @Test
  public void testAuthorizationCatalog1() {
    AuthorizationOperations authOps1 =
        authorizationManager.loadAuthorizationAndWrap(catalogTest1).getOps();
    Assertions.assertInstanceOf(TestAuthorizationOperations1.class, authOps1);
    TestAuthorizationOperations1 operations1 = (TestAuthorizationOperations1) authOps1;
    Assertions.assertFalse(operations1.grantRolesToUser1);
    Assertions.assertFalse(operations1.grantRolesToGroup1);
    Assertions.assertFalse(operations1.revokeRolesFromUser1);
    Assertions.assertFalse(operations1.revokeRolesFromGroup1);
    Assertions.assertFalse(operations1.updateRole1);
    Assertions.assertFalse(operations1.deleteRoles1);

    authorizationManager.runAuthorizationChain(
        catalogTest1,
        ops -> ops.grantRolesToUser(Arrays.asList(roleEntity), "user1"),
        ops -> ops.grantRolesToGroup(Arrays.asList(roleEntity), "group1"),
        ops -> ops.revokeRolesFromUser(Arrays.asList(roleEntity), "user1"),
        ops -> ops.revokeRolesFromGroup(Arrays.asList(roleEntity), "group1"),
        ops -> ops.updateRole(roleEntity, RoleChange.addSecurableObject(dropTable)),
        ops -> ops.deleteRoles(Arrays.asList(roleEntity)));

    Assertions.assertTrue(operations1.grantRolesToUser1);
    Assertions.assertTrue(operations1.grantRolesToGroup1);
    Assertions.assertTrue(operations1.revokeRolesFromUser1);
    Assertions.assertTrue(operations1.revokeRolesFromGroup1);
    Assertions.assertTrue(operations1.updateRole1);
    Assertions.assertTrue(operations1.deleteRoles1);
  }

  @Test
  public void testAuthorizationCatalog2() {
    AuthorizationOperations authOps2 =
        authorizationManager.loadAuthorizationAndWrap(catalogTest2).getOps();
    Assertions.assertInstanceOf(TestAuthorizationOperations2.class, authOps2);
    TestAuthorizationOperations2 operations2 = (TestAuthorizationOperations2) authOps2;
    Assertions.assertFalse(operations2.grantRolesToUser2);
    Assertions.assertFalse(operations2.grantRolesToGroup2);
    Assertions.assertFalse(operations2.revokeRolesFromUser2);
    Assertions.assertFalse(operations2.revokeRolesFromGroup2);
    Assertions.assertFalse(operations2.updateRole2);
    Assertions.assertFalse(operations2.deleteRoles2);

    authorizationManager.runAuthorizationChain(
        catalogTest2,
        ops -> ops.grantRolesToUser(Arrays.asList(roleEntity), "user2"),
        ops -> ops.grantRolesToGroup(Arrays.asList(roleEntity), "group2"),
        ops -> ops.revokeRolesFromUser(Arrays.asList(roleEntity), "user2"),
        ops -> ops.revokeRolesFromGroup(Arrays.asList(roleEntity), "group2"),
        ops -> ops.updateRole(roleEntity, RoleChange.addSecurableObject(dropTable)),
        ops -> ops.deleteRoles(Arrays.asList(roleEntity)));

    Assertions.assertTrue(operations2.grantRolesToUser2);
    Assertions.assertTrue(operations2.grantRolesToGroup2);
    Assertions.assertTrue(operations2.revokeRolesFromUser2);
    Assertions.assertTrue(operations2.revokeRolesFromGroup2);
    Assertions.assertTrue(operations2.updateRole2);
    Assertions.assertTrue(operations2.deleteRoles2);
  }
}
