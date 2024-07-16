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

package org.apache.gravitino.authorization;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
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
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException {
    AuthorizationUtils.checkMetalakeExists(metalake);
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
      AuthorizationUtils.checkMetalakeExists(metalake);
      return getRoleEntity(AuthorizationUtils.ofRole(metalake, role));
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  boolean deleteRole(String metalake, String role) {
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
            LOG.error("Failed to get roles {} due to storage issues", identifier, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @VisibleForTesting
  Cache<NameIdentifier, RoleEntity> getCache() {
    return cache;
  }
}
