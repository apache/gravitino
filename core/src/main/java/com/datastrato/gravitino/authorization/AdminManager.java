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
import com.datastrato.gravitino.exceptions.PrivilegesAlreadyGrantedException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There are two kinds of admin roles in the system: service admin and metalake admin. The service
 * admin is configured instead of managing by APIs. It is responsible for creating metalake admin.
 * If Gravitino enables authorization, service admin is required. Metalake admin can create a
 * metalake or drops its metalake. The metalake admin will be responsible for managing the access
 * control. AdminManager operates underlying store using the lock because kv storage needs the lock.
 */
class AdminManager {

  private static final Logger LOG = LoggerFactory.getLogger(AdminManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final List<String> serviceAdmins;

  AdminManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.serviceAdmins = config.get(Configs.SERVICE_ADMINS);
  }

  User addMetalakeAdmin(String user) {
    try {
      UserEntity userEntity =
          UserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(user)
              .withNamespace(
                  AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
              .withRoleNames(Lists.newArrayList())
              .withRoleIds(Lists.newArrayList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("The metalake admin {} has been added before", user, e);
      throw new PrivilegesAlreadyGrantedException(
          "The metalake admin %s has been added before", user);
    } catch (IOException ioe) {
      LOG.error("Adding user {} failed to the metalake admin due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeMetalakeAdmin(String user) {
    try {
      return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} from the metalake admin {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean isServiceAdmin(String user) {
    return serviceAdmins.contains(user);
  }

  private NameIdentifier ofMetalakeAdmin(String user) {
    return AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, user);
  }
}
