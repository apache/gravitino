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
import com.datastrato.gravitino.Namespace;
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
  }

  RoleEntity createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      SecurableObject securableObject,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException {
    AuthorizationUtils.checkMetalakeExists(metalake);
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(idGenerator.nextId())
            .withName(role)
            .withProperties(properties)
            .withSecurableObject(securableObject)
            .withPrivileges(privileges)
            .withNamespace(
                Namespace.of(
                    metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
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

  RoleEntity loadRole(String metalake, String role) throws NoSuchRoleException {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      return getRoleEntity(AuthorizationUtils.ofRole(metalake, role));
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  boolean dropRole(String metalake, String role) {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      NameIdentifier ident = AuthorizationUtils.ofRole(metalake, role);
      cache.invalidate(ident);

      return store.delete(ident, Entity.EntityType.ROLE);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting role {} in the metalake {} failed due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private RoleEntity getRoleEntity(NameIdentifier identifier) {
    return cache.get(
        identifier,
        id -> {
          try {
            return store.get(identifier, Entity.EntityType.ROLE, RoleEntity.class);
          } catch (IOException ioe) {
            LOG.error("getting roles {} failed  due to storage issues", identifier, ioe);
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
}
