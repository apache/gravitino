/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAccessControlManagerForPermissions {

  private static AccessControlManager accessControlManager;

  private static EntityStore entityStore;

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
          .withSecurableObject(SecurableObjects.ofCatalog(CATALOG))
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

    accessControlManager = new AccessControlManager(entityStore, new RandomIdGenerator(), config);

    GravitinoEnv.getInstance().setEntityStore(entityStore);
    GravitinoEnv.getInstance().setAccessControlManager(accessControlManager);
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
    String notExist = "not-exist";

    User user = accessControlManager.getUser(METALAKE, USER);
    Assertions.assertTrue(user.roles().isEmpty());

    user = accessControlManager.grantRolesToUser(METALAKE, ROLE, USER);
    Assertions.assertFalse(user.roles().isEmpty());

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

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
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

    user = accessControlManager.revokeRolesFromUser(METALAKE, ROLE, USER);
    Assertions.assertTrue(user.roles().isEmpty());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRolesFromUser(notExist, ROLE, USER));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
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

    group = accessControlManager.grantRolesToGroup(METALAKE, ROLE, GROUP);
    Assertions.assertFalse(group.roles().isEmpty());

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

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
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

    group = accessControlManager.revokeRolesFromGroup(METALAKE, ROLE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRolesFromGroup(notExist, ROLE, GROUP));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
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
  public void testDropRole() throws IOException {
    String anotherRole = "anotherRole";

    RoleEntity roleEntity =
        RoleEntity.builder()
            .withNamespace(
                Namespace.of(
                    METALAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
            .withId(1L)
            .withName(anotherRole)
            .withProperties(Maps.newHashMap())
            .withSecurableObject(SecurableObjects.ofCatalog(CATALOG))
            .withAuditInfo(auditInfo)
            .build();

    entityStore.put(roleEntity, true);

    User user =
        accessControlManager.grantRolesToUser(METALAKE, Lists.newArrayList(anotherRole), USER);
    Assertions.assertFalse(user.roles().isEmpty());

    Group group =
        accessControlManager.grantRolesToGroup(METALAKE, Lists.newArrayList(anotherRole), GROUP);
    Assertions.assertFalse(group.roles().isEmpty());

    Assertions.assertTrue(accessControlManager.deleteRole(METALAKE, anotherRole));
    group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());
  }
}
