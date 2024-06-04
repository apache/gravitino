/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.AuthorizationManager;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.chain.authorization1.TestAuthorizationOperations1;
import com.datastrato.gravitino.authorization.chain.authorization2.TestAuthorizationOperations2;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAuthorizationChain {
  private static AuthorizationManager authorizationManager;
  private static Config config;

  private static CatalogEntity catalogTest1;
  private static CatalogEntity catalogTest2;

  @BeforeAll
  public static void setUp() throws Exception {
    AuditInfo auditInfo1 =
        AuditInfo.builder()
            .withCreator("TestAuthorizationChain")
            .withCreateTime(Instant.now())
            .build();

    catalogTest1 =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog-test1")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test1"))
            .withProvider("hive")
            .withAuditInfo(auditInfo1)
            .build();

    AuditInfo auditInfo2 =
        AuditInfo.builder()
            .withCreator("TestAuthorizationChain")
            .withCreateTime(Instant.now())
            .build();
    catalogTest2 =
        CatalogEntity.builder()
            .withId(2L)
            .withName("catalog-test2")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(AuthorizationManager.AUTHORIZATION_PROVIDER, "test2"))
            .withProvider("hive")
            .withAuditInfo(auditInfo2)
            .build();

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
    authorizationManager.runAuthorizationChain(
        catalogTest1,
        ops -> ops.createRole("role1"),
        ops -> ops.toUser("user1"),
        ops -> ops.toGroup("group1"),
        ops -> ops.updateRole("role1", null));

    AuthorizationOperations authOps1 =
        authorizationManager.loadAuthorizationAndWrap(catalogTest1).getOps();
    Assertions.assertTrue(authOps1 instanceof TestAuthorizationOperations1);
    TestAuthorizationOperations1 operations1 = (TestAuthorizationOperations1) authOps1;
    Assertions.assertEquals(operations1.roleName1, "role1");
    Assertions.assertEquals(operations1.user1, "user1");
    Assertions.assertEquals(operations1.group1, "group1");
    Assertions.assertTrue(operations1.updateRole1);
  }

  @Test
  public void testAuthorizationCatalog2() {
    authorizationManager.runAuthorizationChain(
        catalogTest2,
        ops -> ops.createRole("role2"),
        ops -> ops.toUser("user2"),
        ops -> ops.toGroup("group2"),
        ops -> ops.updateRole("role2", null));

    AuthorizationOperations authOps2 =
        authorizationManager.loadAuthorizationAndWrap(catalogTest2).getOps();
    Assertions.assertTrue(authOps2 instanceof TestAuthorizationOperations2);
    TestAuthorizationOperations2 operations2 = (TestAuthorizationOperations2) authOps2;
    Assertions.assertEquals(operations2.roleName2, "role2");
    Assertions.assertEquals(operations2.user2, "user2");
    Assertions.assertEquals(operations2.group2, "group2");
    Assertions.assertTrue(operations2.updateRole2);
  }
}
