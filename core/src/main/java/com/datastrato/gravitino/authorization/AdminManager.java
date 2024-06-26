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
        if (hasMetalakeCreateRole(user)) {
          throw new PrivilegesAlreadyGrantedException(
              "Metalake admin has added the metalake create role");
        }
        return updateUserEntity(
            user,
            Lists.newArrayList(Entity.METALAKE_CREATE_ROLE, Entity.MANAGE_METALAKE_ADMIN_ROLE));
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
      if (!isServiceAdmin(user)) {
        return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
      }

      if (hasMetalakeCreateRole(user)) {
        updateUserEntity(user, Lists.newArrayList(Entity.MANAGE_METALAKE_ADMIN_ROLE));
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

  private NameIdentifier ofMetalakeAdmin(String user) {
    return AuthorizationUtils.ofUser(Entity.SYSTEM_METALAKE_RESERVED_NAME, user);
  }

  private boolean hasMetalakeCreateRole(String user) throws IOException {
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
        // Case 1: If the user is the service admin, we should add the system metalake manage user
        // role
        // for it.
        if (serviceAdmins.contains(userEntity.name())) {
          if (!userEntity.roleNames().contains(Entity.MANAGE_METALAKE_ADMIN_ROLE)) {
            List<String> newRoleNames = Lists.newArrayList(userEntity.roleNames());
            newRoleNames.add(Entity.MANAGE_METALAKE_ADMIN_ROLE);
            updateUserEntity(userEntity.name(), newRoleNames);
          }
        } else if (userEntity.roleNames().contains(Entity.METALAKE_CREATE_ROLE)) {
          // Case 2: If the user isn't the service admin and has metalake create role,
          // we should remove system metalake manage user role.
          if (userEntity.roleNames().contains(Entity.MANAGE_METALAKE_ADMIN_ROLE)) {
            updateUserEntity(userEntity.name(), Lists.newArrayList(Entity.METALAKE_CREATE_ROLE));
          }
        } else {
          // Case 3: If the user isn't the service admin and has no other role,
          // we should delete it.
          store.delete(userEntity.nameIdentifier(), Entity.EntityType.USER);
        }
      }

      // Case 4: If the user is service admin and isn't stored before, we store them.
      for (String user : serviceAdmins) {
        if (!store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER)) {
          long roleId =
              roleManager
                  .getRole(Entity.SYSTEM_METALAKE_RESERVED_NAME, Entity.MANAGE_METALAKE_ADMIN_ROLE)
                  .id();
          UserEntity userEntity =
              UserEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(user)
                  .withNamespace(
                      AuthorizationUtils.ofUserNamespace(Entity.SYSTEM_METALAKE_RESERVED_NAME))
                  .withRoleNames(Lists.newArrayList(Entity.MANAGE_METALAKE_ADMIN_ROLE))
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
