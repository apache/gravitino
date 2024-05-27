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
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
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
  private final RoleManager roleManager;

  AdminManager(EntityStore store, IdGenerator idGenerator, Config config, RoleManager roleManager) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    this.roleManager = roleManager;

    updateSystemMetalakeUsers();
  }

  User addMetalakeAdmin(String user) {
    try {
      if (isServiceAdmin(user)) {
        if (hasUserMetalakeCreateRole(user)) {
          throw new EntityAlreadyExistsException(
              "Metalake admin has added the metalake create role");
        }
        return updateUserEntity(
            user,
            Lists.newArrayList(
                Entity.METALAKE_CREATE_ROLE, Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE));
      } else {
        long roleId =
            roleManager
                .getRole(Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.METALAKE_CREATE_ROLE)
                .id();
        UserEntity userEntity =
            UserEntity.builder()
                .withId(idGenerator.nextId())
                .withName(user)
                .withNamespace(
                    AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
                .withRoleNames(Lists.newArrayList(Entity.METALAKE_CREATE_ROLE))
                .withRoleIds(Lists.newArrayList(roleId))
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                        .withCreateTime(Instant.now())
                        .build())
                .build();

        store.put(userEntity, false /* overwritten */);
        return userEntity;
      }
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake admin already exists", user, e);
      throw new UserAlreadyExistsException("User %s in the metalake admin already exists", user);
    } catch (IOException ioe) {
      LOG.error("Adding user {} failed to the metalake admin due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeMetalakeAdmin(String user) {
    try {
      if (!isServiceAdmin(user)) {
        return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
      }

      if (hasUserMetalakeCreateRole(user)) {
        updateUserEntity(user, Lists.newArrayList(Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE));
        return true;
      } else {
        return false;
      }
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} from the metalake admin {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean isServiceAdmin(String user) {
    return serviceAdmins.contains(user);
  }

  boolean isMetalakeAdmin(String user) {
    try {
      return store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Fail to check whether {} is the metalake admin {} due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofMetalakeAdmin(String user) {
    return AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, user);
  }

  private boolean hasUserMetalakeCreateRole(String user) throws IOException {
    UserEntity userEntity =
        store.get(ofMetalakeAdmin(user), Entity.EntityType.USER, UserEntity.class);
    return userEntity.roleNames().contains(Entity.METALAKE_CREATE_ROLE);
  }

  private UserEntity updateUserEntity(String user, List<String> roleNames) throws IOException {
    List<Long> roleIds =
        roleNames.stream()
            .map(
                roleName ->
                    roleManager.getRole(Entity.SYSTEM_METALAKE_RESERVED_NAME, roleName).id())
            .collect(Collectors.toList());

    return store.update(
        ofMetalakeAdmin(user),
        UserEntity.class,
        Entity.EntityType.USER,
        userEntity -> {
          return UserEntity.builder()
              .withId(userEntity.id())
              .withName(userEntity.name())
              .withNamespace(
                  AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(userEntity.auditInfo().creator())
                      .withCreateTime(userEntity.auditInfo().createTime())
                      .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                      .withLastModifiedTime(Instant.now())
                      .build())
              .build();
        });
  }

  private void updateSystemMetalakeUsers() {
    try {
      List<UserEntity> userEntities =
          store.list(
              AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME),
              UserEntity.class,
              Entity.EntityType.USER);

      for (UserEntity userEntity : userEntities) {
        if (serviceAdmins.contains(userEntity.name())) {
          updateUserEntity(
              userEntity.name(),
              Lists.newArrayList(
                  Entity.METALAKE_CREATE_ROLE, Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE));
        } else if (userEntity.roleNames().contains(Entity.METALAKE_CREATE_ROLE)) {
          updateUserEntity(userEntity.name(), Lists.newArrayList(Entity.METALAKE_CREATE_ROLE));
        } else {
          store.delete(userEntity.nameIdentifier(), Entity.EntityType.USER);
        }
      }

      for (String user : serviceAdmins) {
        if (!store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER)) {
          long roleId =
              roleManager
                  .getRole(
                      Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE)
                  .id();
          UserEntity userEntity =
              UserEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(user)
                  .withNamespace(
                      AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
                  .withRoleNames(Lists.newArrayList(Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE))
                  .withRoleIds(Lists.newArrayList(roleId))
                  .withAuditInfo(
                      AuditInfo.builder()
                          .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                          .withCreateTime(Instant.now())
                          .build())
                  .build();

          store.put(userEntity, false /* overwritten */);
        }
      }
    } catch (IOException ioe) {
      LOG.error("Failed to update system metalake users due to storage issues", ioe);
      throw new RuntimeException(ioe);
    }
  }
}
