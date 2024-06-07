/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.Configs.SERVICE_ADMINS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdminManager {

  @Test
  public void testUpdateSystemUsers() throws Exception {
    Config config = new Config(false) {};
    config.set(SERVICE_ADMINS, Lists.newArrayList("admin1", "admin2"));
    EntityStore entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    RoleManager roleManager = mock(RoleManager.class);
    RoleEntity createRoleEntity = mock(RoleEntity.class);
    RoleEntity manageRoleEntity = mock(RoleEntity.class);
    when(roleManager.getRole(Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.METALAKE_CREATE_ROLE))
        .thenReturn(createRoleEntity);
    when(roleManager.getRole(
            Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE))
        .thenReturn(manageRoleEntity);
    when(createRoleEntity.id()).thenReturn(1L);
    when(manageRoleEntity.id()).thenReturn(2L);

    // case 1: init the store without any metalake admin
    new AdminManager(entityStore, new RandomIdGenerator(), config, roleManager);
    Assertions.assertTrue(
        entityStore.exists(
            AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, "admin1"),
            Entity.EntityType.USER));
    Assertions.assertTrue(
        entityStore.exists(
            AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, "admin2"),
            Entity.EntityType.USER));

    // case 2: init the store with  the user is metalake admin
    UserEntity admin2 =
        UserEntity.builder()
            .withId(1L)
            .withName("admin2")
            .withNamespace(AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
            .withRoleNames(
                Lists.newArrayList(
                    Entity.METALAKE_CREATE_ROLE, Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE))
            .withRoleIds(Lists.newArrayList(1L, 2L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    UserEntity admin3 =
        UserEntity.builder()
            .withId(2L)
            .withName("admin3")
            .withNamespace(AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
            .withRoleNames(Lists.newArrayList(Entity.METALAKE_CREATE_ROLE))
            .withRoleIds(Lists.newArrayList(1L, 2L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(admin2, true);
    entityStore.put(admin3, true);
    config.set(SERVICE_ADMINS, Lists.newArrayList("admin2", "admin3"));
    new AdminManager(entityStore, new RandomIdGenerator(), config, roleManager);
    Assertions.assertFalse(
        entityStore.exists(
            AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, "admin1"),
            Entity.EntityType.USER));
    UserEntity admin2New =
        entityStore.get(
            AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, "admin2"),
            Entity.EntityType.USER,
            UserEntity.class);
    Assertions.assertEquals(admin2.name(), admin2New.name());
    Assertions.assertEquals(admin2.roleNames(), admin2New.roleNames());
    UserEntity admin3New =
        entityStore.get(
            AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, "admin3"),
            Entity.EntityType.USER,
            UserEntity.class);
    Assertions.assertEquals(admin3.name(), admin3New.name());
    Assertions.assertNotEquals(admin3.roleNames(), admin3New.roleNames());
    Assertions.assertEquals(2, admin2New.roles().size());
    Assertions.assertTrue(admin2New.roles().contains(Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE));
    Assertions.assertTrue(admin2New.roles().contains(Entity.METALAKE_CREATE_ROLE));

    entityStore.close();
  }
}
