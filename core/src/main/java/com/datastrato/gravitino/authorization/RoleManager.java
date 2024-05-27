/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RoleManager is responsible for managing the roles. Role contains the privileges of one privilege
 * entity. If one Role is created and the privilege entity is an external system, the role will be
 * created in the underlying entity, too.
 */
class RoleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RoleManager.class);
  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final Cache<NameIdentifier, RoleEntity> cache;

  RoleManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;

    long cacheEvictionIntervalInMs = config.get(Configs.ROLE_CACHE_EVICTION_INTERVAL_MS);
    // One role entity is about 40 bytes using jol estimate, there are usually about 100w+
    // roles in the production environment, this won't bring too much memory cost, but it
    // can improve the performance significantly.
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheEvictionIntervalInMs, TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  LOG.info("Remove role {} from the cache.", k);
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("role-cleaner-%d")
                            .build())))
            .build();

    initSystemRoles();
  }

  RoleEntity createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException {
    if (role.startsWith(Entity.SYSTEM_RESERVED_ROLE_NAME_PREFIX)) {
      throw new IllegalArgumentException(
          "Can't create a role with with reserved prefix `system_role`");
    }

    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(idGenerator.nextId())
            .withName(role)
            .withProperties(properties)
            .withSecurableObjects(securableObjects)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(roleEntity, false /* overwritten */);
      cache.put(roleEntity.nameIdentifier(), roleEntity);
      return roleEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Role {} in the metalake {} already exists", role, metalake, e);
      throw new RoleAlreadyExistsException(
          "Role %s in the metalake %s already exists", role, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating role {} failed in the metalake {} due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  RoleEntity getRole(String metalake, String role) throws NoSuchRoleException {
    try {
      return getRoleEntity(AuthorizationUtils.ofRole(metalake, role));
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  boolean deleteRole(String metalake, String role) {
    try {
      NameIdentifier ident = AuthorizationUtils.ofRole(metalake, role);
      cache.invalidate(ident);

      return store.delete(ident, Entity.EntityType.ROLE);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting role {} in the metalake {} failed due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  RoleEntity getRoleEntity(NameIdentifier identifier) {
    return cache.get(
        identifier,
        id -> {
          try {
            return store.get(identifier, Entity.EntityType.ROLE, RoleEntity.class);
          } catch (IOException ioe) {
            LOG.error("Failed to get roles {} due to storage issues", identifier, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @VisibleForTesting
  Cache<NameIdentifier, RoleEntity> getCache() {
    return cache;
  }

  List<RoleEntity> getValidRoles(String metalake, List<String> roleNames, List<Long> roleIds) {
    List<RoleEntity> roleEntities = Lists.newArrayList();
    if (roleNames == null || roleNames.isEmpty()) {
      return roleEntities;
    }

    int index = 0;
    for (String role : roleNames) {
      try {

        RoleEntity roleEntity = getRoleEntity(AuthorizationUtils.ofRole(metalake, role));

        if (roleEntity.id().equals(roleIds.get(index))) {
          roleEntities.add(roleEntity);
        }
        index++;

      } catch (NoSuchEntityException nse) {
        // ignore this entity
      }
    }
    return roleEntities;
  }

  void initSystemRoles() {
    try {
      if (!store.exists(AuthorizationUtils.ofSystemMetalakeAddUserRole(), Entity.EntityType.ROLE)) {
        RoleEntity roleEntity =
            RoleEntity.builder()
                .withId(idGenerator.nextId())
                .withName(Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE)
                .withNamespace(
                    AuthorizationUtils.ofRoleNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
                .withSecurableObjects(
                    Lists.newArrayList(
                        SecurableObjects.ofMetalake(
                            Entity.SYSTEM_METALAKE_RESERVED_NAME,
                            Lists.newArrayList(
                                Privileges.AddUser.allow(), Privileges.RemoveUser.allow()))))
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(System.getProperty("user.name"))
                        .withCreateTime(Instant.now())
                        .build())
                .build();

        store.put(roleEntity, false /* overwritten */);
        cache.put(roleEntity.nameIdentifier(), roleEntity);
      }

      if (!store.exists(AuthorizationUtils.ofMetalakeCreateRole(), Entity.EntityType.ROLE)) {
        RoleEntity roleEntity =
            RoleEntity.builder()
                .withId(idGenerator.nextId())
                .withNamespace(
                    AuthorizationUtils.ofRoleNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
                .withName(Entity.METALAKE_CREATE_ROLE)
                .withSecurableObjects(
                    Lists.newArrayList(
                        SecurableObjects.ofAllMetalakes(
                            Lists.newArrayList(Privileges.CreateMetalake.allow()))))
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(System.getProperty("user.name"))
                        .withCreateTime(Instant.now())
                        .build())
                .build();

        store.put(roleEntity, false /* overwritten */);
        cache.put(roleEntity.nameIdentifier(), roleEntity);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to init system roles due to storage issues", ioe);
      throw new RuntimeException(ioe);
    }
  }
}
