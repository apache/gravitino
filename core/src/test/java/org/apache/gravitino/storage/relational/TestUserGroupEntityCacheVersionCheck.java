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
package org.apache.gravitino.storage.relational;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.cache.CacheFactory;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.service.GroupMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Verifies that name-keyed USER/GROUP {@code store.get} reloads when another writer advances {@code
 * *_meta.updated_at} without invalidating this process's cache (the HA peer case).
 */
public class TestUserGroupEntityCacheVersionCheck extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_user_group_cache_version_test";

  @TestTemplate
  void testGetUserReloadsWhenUpdatedAtAdvances() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "cached_user",
            AUDIT_INFO);
    UserMetaService.getInstance().insertUser(user, false);

    RelationalEntityStore store = newStore();
    UserEntity first = store.get(user.nameIdentifier(), Entity.EntityType.USER, UserEntity.class);
    Assertions.assertTrue(first.enabled());
    Assertions.assertTrue(store.getCache().contains(user.nameIdentifier(), Entity.EntityType.USER));

    UserMetaService.getInstance()
        .updateUserById(
            METALAKE_NAME,
            user.id(),
            existing -> {
              UserEntity current = (UserEntity) existing;
              return UserEntity.builder()
                  .withId(current.id())
                  .withName(current.name())
                  .withNamespace(current.namespace())
                  .withExternalId(current.externalId())
                  .withEnabled(false)
                  .withRoleNames(current.roleNames())
                  .withRoleIds(current.roleIds())
                  .withAuditInfo(current.auditInfo())
                  .withUpdatedAt(current.updatedAt())
                  .build();
            });

    UserEntity second = store.get(user.nameIdentifier(), Entity.EntityType.USER, UserEntity.class);
    Assertions.assertFalse(second.enabled());
  }

  @TestTemplate
  void testGetGroupReloadsWhenUpdatedAtAdvances() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    GroupEntity group =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(METALAKE_NAME),
            "cached_group",
            AUDIT_INFO,
            null,
            null);
    GroupMetaService.getInstance().insertGroup(group, false);

    RelationalEntityStore store = newStore();
    GroupEntity first =
        store.get(group.nameIdentifier(), Entity.EntityType.GROUP, GroupEntity.class);
    Assertions.assertNull(first.externalId());

    GroupMetaService.getInstance()
        .updateGroupById(
            METALAKE_NAME,
            group.id(),
            existing -> {
              GroupEntity current = (GroupEntity) existing;
              return GroupEntity.builder()
                  .withId(current.id())
                  .withName(current.name())
                  .withNamespace(current.namespace())
                  .withExternalId("ext-new")
                  .withRoleNames(current.roleNames())
                  .withRoleIds(current.roleIds())
                  .withAuditInfo(current.auditInfo())
                  .withUpdatedAt(current.updatedAt())
                  .build();
            });

    GroupEntity second =
        store.get(group.nameIdentifier(), Entity.EntityType.GROUP, GroupEntity.class);
    Assertions.assertEquals("ext-new", second.externalId());
  }

  @TestTemplate
  void testGetUserReloadsAfterDelete() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    UserEntity user =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(METALAKE_NAME),
            "deleted_cached_user",
            AUDIT_INFO);
    UserMetaService.getInstance().insertUser(user, false);

    RelationalEntityStore store = newStore();
    store.get(user.nameIdentifier(), Entity.EntityType.USER, UserEntity.class);
    Assertions.assertTrue(UserMetaService.getInstance().deleteUser(user.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(user.nameIdentifier(), Entity.EntityType.USER, UserEntity.class));
  }

  private RelationalEntityStore newStore() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", CacheFactory.getEntityCache(new Config(false) {}), true);
    return store;
  }
}
