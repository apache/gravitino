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
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
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

class GrantManager {
  private static final Logger LOG = LoggerFactory.getLogger(GrantManager.class);

  private final EntityStore store;

  GrantManager(EntityStore store) {
    this.store = store;
  }

  public boolean addRoleToUser(String metalake, String role, String user) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);

      checkRoleExists(metalake, role);

      RoleEntity roleEntity =
          store.get(
              NameIdentifierUtils.ofRole(metalake, role), Entity.EntityType.ROLE, RoleEntity.class);

      store.update(
          NameIdentifierUtils.ofUser(metalake, user),
          UserEntity.class,
          Entity.EntityType.USER,
          userEntity -> {
            List<RoleEntity> roleEntities = removeInvalidRoles(metalake, userEntity.roles());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

            if (roleNames.contains(roleEntity.name())) {
              throw new RoleAlreadyExistsException(
                  "Role %s already exists in the user %s of the metalake %s", role, user, metalake);
            }

            roleNames.add(roleEntity.name());
            roleIds.add(roleEntity.id());
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
      return true;
    } catch (NoSuchEntityException nse) {
      LOG.warn("User {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding role {} from  user {} failed in the metalake {} due to storage issues",
          role,
          user,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean addRoleToGroup(String metalake, String role, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);

      checkRoleExists(metalake, role);

      RoleEntity roleEntity =
          store.get(
              NameIdentifierUtils.ofRole(metalake, role), Entity.EntityType.ROLE, RoleEntity.class);

      store.update(
          NameIdentifierUtils.ofGroup(metalake, group),
          GroupEntity.class,
          Entity.EntityType.GROUP,
          groupEntity -> {
            List<RoleEntity> roleEntities = removeInvalidRoles(metalake, groupEntity.roles());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

            if (roleNames.contains(roleEntity.name())) {
              throw new RoleAlreadyExistsException(
                  "Role %s already exists in the group %s of the metalake %s",
                  role, group, metalake);
            }

            AuditInfo auditInfo =
                AuditInfo.builder()
                    .withCreator(groupEntity.auditInfo().creator())
                    .withCreateTime(groupEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build();

            roleNames.add(roleEntity.name());
            roleIds.add(roleEntity.id());
            return GroupEntity.builder()
                .withId(groupEntity.id())
                .withNamespace(groupEntity.namespace())
                .withName(groupEntity.name())
                .withRoleNames(roleNames)
                .withRoleIds(roleIds)
                .withAuditInfo(auditInfo)
                .build();
          });
      return true;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding role {} from  group {} failed in the metalake {} due to storage issues",
          role,
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean removeRoleFromGroup(String metalake, String role, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);

      checkRoleExists(metalake, role);

      RoleEntity roleEntity =
          store.get(
              NameIdentifierUtils.ofRole(metalake, role), Entity.EntityType.ROLE, RoleEntity.class);

      AtomicBoolean removed = new AtomicBoolean(true);

      store.update(
          NameIdentifierUtils.ofGroup(metalake, group),
          GroupEntity.class,
          Entity.EntityType.GROUP,
          groupEntity -> {
            List<RoleEntity> roleEntities = removeInvalidRoles(metalake, groupEntity.roles());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));
            roleNames.remove(roleEntity.name());
            removed.set(roleIds.remove(roleEntity.id()));

            if (!removed.get()) {
              LOG.warn(
                  "Role {} does not exist in the group {} of metalake {}", role, group, metalake);
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
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Removing role {} from  group {} failed in the metalake {} due to storage issues",
          role,
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean removeRoleFromUser(String metalake, String role, String user) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);

      checkRoleExists(metalake, role);

      RoleEntity roleEntity =
          store.get(
              NameIdentifierUtils.ofRole(metalake, role), Entity.EntityType.ROLE, RoleEntity.class);

      AtomicBoolean removed = new AtomicBoolean(true);

      store.update(
          NameIdentifierUtils.ofUser(metalake, user),
          UserEntity.class,
          Entity.EntityType.USER,
          userEntity -> {
            List<RoleEntity> roleEntities = removeInvalidRoles(metalake, userEntity.roles());
            List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
            List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));
            roleNames.remove(roleEntity.name());
            removed.set(roleIds.remove(roleEntity.id()));
            if (!removed.get()) {
              LOG.warn("Role {} doesn't exist in the user {} of metalake {}", role, user, metalake);
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
      LOG.warn("User {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Removing role {} from  user {} failed in the metalake {} due to storage issues",
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

  private void checkRoleExists(String metalake, String role) throws IOException {
    if (!store.exists(NameIdentifierUtils.ofRole(metalake, role), Entity.EntityType.ROLE)) {
      throw new NoSuchRoleException(AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  private List<RoleEntity> removeInvalidRoles(String metalake, List<String> roles) {
    if (roles == null || roles.isEmpty()) {
      return Lists.newArrayList();
    }

    List<RoleEntity> roleEntities = Lists.newArrayList();
    for (String role : roles) {
      try {
        roleEntities.add(
            store.get(
                NameIdentifierUtils.ofRole(metalake, role),
                Entity.EntityType.ROLE,
                RoleEntity.class));
      } catch (IOException ioe) {
        LOG.error(
            "Checking roles {} failed in the metalake {} due to storage issues",
            role,
            metalake,
            ioe);
        throw new RuntimeException(ioe);
      } catch (NoSuchEntityException nse) {
        // ignore this entity
      }
    }
    return roleEntities;
  }
}
