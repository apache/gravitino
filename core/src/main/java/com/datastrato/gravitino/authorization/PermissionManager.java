/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.AuthorizationUtils.GROUP_DOES_NOT_EXIST_MSG;
import static com.datastrato.gravitino.authorization.AuthorizationUtils.USER_DOES_NOT_EXIST_MSG;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionManager is used for managing the logic the granting and revoking roles. Role is used
 * for manging permissions. GrantManager will filter the invalid roles, too.
 */
class PermissionManager {
  private static final Logger LOG = LoggerFactory.getLogger(PermissionManager.class);

  private final EntityStore store;
  private final RoleManager roleManager;

  PermissionManager(EntityStore store, RoleManager roleManager) {
    this.store = store;
    this.roleManager = roleManager;
  }

  boolean grantRoleToUser(String metalake, String role, String user) {
    try {
      RoleEntity roleEntity = roleManager.getRole(metalake, role);

      AtomicBoolean granted = new AtomicBoolean(true);
      store.update(
          AuthorizationUtils.ofUser(metalake, user),
          UserEntity.class,
          Entity.EntityType.USER,
          userEntity -> {
            List<RoleEntity> roleEntities =
                roleManager.getValidRoles(metalake, userEntity.roleNames(), userEntity.roleIds());

            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

            if (roleNames.contains(roleEntity.name())) {
              granted.set(false);
              LOG.warn(
                  "Failed to grant, role {} already exists in the user {} of metalake {}",
                  role,
                  user,
                  metalake);
            } else {
              roleNames.add(roleEntity.name());
              roleIds.add(roleEntity.id());
            }

            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(userEntity.auditInfo().creator())
                    .withCreateTime(userEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build();

            return UserEntity.builder()
                .withNamespace(userEntity.namespace())
                .withId(userEntity.id())
                .withName(userEntity.name())
                .withRoleNames(roleNames)
                .withRoleIds(roleIds)
                .withAuditInfo(auditInfo)
                .build();
          });
      return granted.get();
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to grant role {} to user {} in the metalake {} due to storage issues",
          role,
          user,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean grantRoleToGroup(String metalake, String role, String group) {
    try {
      RoleEntity roleEntity = roleManager.getRole(metalake, role);

      AtomicBoolean granted = new AtomicBoolean(true);
      store.update(
          AuthorizationUtils.ofGroup(metalake, group),
          GroupEntity.class,
          Entity.EntityType.GROUP,
          groupEntity -> {
            List<RoleEntity> roleEntities =
                roleManager.getValidRoles(metalake, groupEntity.roleNames(), groupEntity.roleIds());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

            if (roleNames.contains(roleEntity.name())) {
              granted.set(false);
              LOG.warn(
                  "Failed to grant, role {} already exists in the group {} of metalake {}",
                  role,
                  group,
                  metalake);
            } else {
              roleNames.add(roleEntity.name());
              roleIds.add(roleEntity.id());
            }

            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(groupEntity.auditInfo().creator())
                    .withCreateTime(groupEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build();

            return GroupEntity.builder()
                .withId(groupEntity.id())
                .withNamespace(groupEntity.namespace())
                .withName(groupEntity.name())
                .withRoleNames(roleNames)
                .withRoleIds(roleIds)
                .withAuditInfo(auditInfo)
                .build();
          });
      return granted.get();
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to grant role {} to group {} in the metalake {} due to storage issues",
          role,
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean revokeRoleFromGroup(String metalake, String role, String group) {
    try {
      RoleEntity roleEntity = roleManager.getRole(metalake, role);

      AtomicBoolean removed = new AtomicBoolean(true);

      store.update(
          AuthorizationUtils.ofGroup(metalake, group),
          GroupEntity.class,
          Entity.EntityType.GROUP,
          groupEntity -> {
            List<RoleEntity> roleEntities =
                roleManager.getValidRoles(metalake, groupEntity.roleNames(), groupEntity.roleIds());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));
            roleNames.remove(roleEntity.name());
            removed.set(roleIds.remove(roleEntity.id()));

            if (!removed.get()) {
              LOG.warn(
                  "Failed to revoke, role {} does not exist in the group {} of metalake {}",
                  role,
                  group,
                  metalake);
            }

            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(groupEntity.auditInfo().creator())
                    .withCreateTime(groupEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build();

            return GroupEntity.builder()
                .withNamespace(groupEntity.namespace())
                .withId(groupEntity.id())
                .withName(groupEntity.name())
                .withRoleNames(roleNames)
                .withRoleIds(roleIds)
                .withAuditInfo(auditInfo)
                .build();
          });

      return removed.get();
    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to revoke, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to revoke role {} from  group {} in the metalake {} due to storage issues",
          role,
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean revokeRoleFromUser(String metalake, String role, String user) {
    try {
      RoleEntity roleEntity = roleManager.getRole(metalake, role);
      AtomicBoolean removed = new AtomicBoolean(true);

      store.update(
          AuthorizationUtils.ofUser(metalake, user),
          UserEntity.class,
          Entity.EntityType.USER,
          userEntity -> {
            List<RoleEntity> roleEntities =
                roleManager.getValidRoles(metalake, userEntity.roleNames(), userEntity.roleIds());

            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

            roleNames.remove(roleEntity.name());
            removed.set(roleIds.remove(roleEntity.id()));
            if (!removed.get()) {
              LOG.warn(
                  "Failed to revoke, role {} doesn't exist in the user {} of metalake {}",
                  role,
                  user,
                  metalake);
            }

            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(userEntity.auditInfo().creator())
                    .withCreateTime(userEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build();
            return UserEntity.builder()
                .withId(userEntity.id())
                .withNamespace(userEntity.namespace())
                .withName(userEntity.name())
                .withRoleNames(roleNames)
                .withRoleIds(roleIds)
                .withAuditInfo(auditInfo)
                .build();
          });
      return removed.get();
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to revoke, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to revoke role {} from  user {} in the metalake {} due to storage issues",
          role,
          user,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private List<String> toRoleNames(List<RoleEntity> roleEntities) {
    return roleEntities.stream().map(RoleEntity::name).collect(Collectors.toList());
  }

  private List<Long> toRoleIds(List<RoleEntity> roleEntities) {
    return roleEntities.stream().map(RoleEntity::id).collect(Collectors.toList());
  }
}
