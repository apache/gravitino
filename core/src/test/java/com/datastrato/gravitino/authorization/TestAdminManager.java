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
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import com.google.common.collect.Lists;
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

    // case 2:

    entityStore.close();
  }
}
