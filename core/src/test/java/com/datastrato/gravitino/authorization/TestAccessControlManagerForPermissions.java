/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
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

  private static String ROLE = "role";

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
          .withName(ROLE)
          .withProperties(Maps.newHashMap())
          .withPrivileges(Lists.newArrayList(Privileges.LoadCatalog.get()))
          .withSecurableObject(SecurableObjects.of(CATALOG))
          .withAuditInfo(auditInfo)
          .build();

  @BeforeAll
  public static void setUp() throws Exception {
    config = new Config(false) {};

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
  public void testAddRoleToUser() {
    String notExist = "not-exist";

    User user = accessControlManager.getUser(METALAKE, USER);
    Assertions.assertTrue(user.roles().isEmpty());

    Assertions.assertTrue(accessControlManager.grantRoleToUser(METALAKE, ROLE, USER));
    user = accessControlManager.getUser(METALAKE, USER);
    Assertions.assertEquals(1, user.roles().size());
    Assertions.assertEquals(ROLE, user.roles().get(0));

    // Throw RoleAlreadyExistsException
    Assertions.assertThrows(
        RoleAlreadyExistsException.class,
        () -> accessControlManager.grantRoleToUser(METALAKE, ROLE, USER));

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.grantRoleToUser(notExist, ROLE, USER));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> accessControlManager.grantRoleToUser(METALAKE, notExist, USER));

    // Throw NoSuchUserException
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> accessControlManager.grantRoleToUser(METALAKE, ROLE, notExist));

    // Clear Resource
    Assertions.assertTrue(accessControlManager.revokeRoleFromUser(METALAKE, ROLE, USER));
  }

  @Test
  public void testRemoveRoleFromUser() {
    String notExist = "not-exist";

    Assertions.assertTrue(accessControlManager.grantRoleToUser(METALAKE, ROLE, USER));
    Assertions.assertTrue(accessControlManager.revokeRoleFromUser(METALAKE, ROLE, USER));

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRoleFromUser(notExist, ROLE, USER));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> accessControlManager.revokeRoleFromUser(METALAKE, notExist, USER));

    // Remove role which doesn't exist.
    Assertions.assertFalse(accessControlManager.revokeRoleFromUser(METALAKE, ROLE, USER));

    // Throw NoSuchUserException
    Assertions.assertThrows(
        NoSuchUserException.class,
        () -> accessControlManager.revokeRoleFromUser(METALAKE, ROLE, notExist));
  }

  @Test
  public void testAddRoleToGroup() {
    String notExist = "not-exist";

    Group group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());

    Assertions.assertTrue(accessControlManager.grantRoleToGroup(METALAKE, ROLE, GROUP));

    group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertEquals(1, group.roles().size());
    Assertions.assertEquals(ROLE, group.roles().get(0));

    // Throw RoleAlreadyExistsException
    Assertions.assertThrows(
        RoleAlreadyExistsException.class,
        () -> accessControlManager.grantRoleToGroup(METALAKE, ROLE, GROUP));

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.grantRoleToGroup(notExist, ROLE, GROUP));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> accessControlManager.grantRoleToGroup(METALAKE, notExist, GROUP));

    // Throw NoSuchGroupException
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> accessControlManager.grantRoleToGroup(METALAKE, ROLE, notExist));

    // Clear Resource
    Assertions.assertTrue(accessControlManager.revokeRoleFromGroup(METALAKE, ROLE, GROUP));
  }

  @Test
  public void testRemoveRoleFormGroup() {
    String notExist = "not-exist";

    Assertions.assertTrue(accessControlManager.grantRoleToGroup(METALAKE, ROLE, GROUP));
    Assertions.assertTrue(accessControlManager.revokeRoleFromGroup(METALAKE, ROLE, GROUP));

    // Throw NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.revokeRoleFromGroup(notExist, ROLE, GROUP));

    // Throw NoSuchRoleException
    Assertions.assertThrows(
        NoSuchRoleException.class,
        () -> accessControlManager.revokeRoleFromGroup(METALAKE, notExist, USER));

    // Remove not exist role
    Assertions.assertFalse(accessControlManager.revokeRoleFromUser(METALAKE, ROLE, USER));

    // Throw NoSuchGroupException
    Assertions.assertThrows(
        NoSuchGroupException.class,
        () -> accessControlManager.revokeRoleFromGroup(METALAKE, ROLE, notExist));
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
            .withPrivileges(Lists.newArrayList(Privileges.LoadCatalog.get()))
            .withSecurableObject(SecurableObjects.ofCatalog(CATALOG))
            .withAuditInfo(auditInfo)
            .build();

    entityStore.put(roleEntity, true);
    Assertions.assertTrue(accessControlManager.grantRoleToUser(METALAKE, anotherRole, USER));
    Assertions.assertTrue(accessControlManager.grantRoleToGroup(METALAKE, anotherRole, GROUP));
    accessControlManager.dropRole(METALAKE, anotherRole);
    Group group = accessControlManager.getGroup(METALAKE, GROUP);
    Assertions.assertTrue(group.roles().isEmpty());
  }
}
