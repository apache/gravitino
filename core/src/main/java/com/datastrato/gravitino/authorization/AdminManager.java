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
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
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

  public AdminManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.serviceAdmins = config.get(Configs.SERVICE_ADMINS);
  }

  /**
   * Adds a new metalake admin.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addMetalakeAdmin(String user) {

    UserEntity userEntity =
        UserEntity.builder()
            .withId(idGenerator.nextId())
            .withName(user)
            .withNamespace(
                Namespace.of(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Entity.AUTHORIZATION_CATALOG_NAME,
                    Entity.ADMIN_SCHEMA_NAME))
            .withRoleNames(Lists.newArrayList())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake admin already exists", user, e);
      throw new UserAlreadyExistsException("User %s in the metalake admin already exists", user);
    } catch (IOException ioe) {
      LOG.error("Adding user {} failed to the metalake admin due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a metalake admin.
   *
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeMetalakeAdmin(String user) {
    try {
      return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} from the metalake admin {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Judges whether the user is the service admin.
   *
   * @param user the name of the user
   * @return true, if the user is service admin, otherwise false.
   */
  public boolean isServiceAdmin(String user) {
    return serviceAdmins.contains(user);
  }

  /**
   * Judges whether the user is the metalake admin.
   *
   * @param user the name of the user
   * @return true, if the user is metalake admin, otherwise false.
   */
  public boolean isMetalakeAdmin(String user) {
    try {
      return store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Fail to check whether {} is the metalake admin {} due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofMetalakeAdmin(String user) {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME,
        Entity.AUTHORIZATION_CATALOG_NAME,
        Entity.ADMIN_SCHEMA_NAME,
        user);
  }
}
