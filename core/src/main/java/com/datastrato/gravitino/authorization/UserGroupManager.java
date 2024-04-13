/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserGroupManager is used for add, remove and get users or roles from one metalake.
 * UserGroupManager doesn't manage users or groups, just sets up the relationship between the
 * metalake and the user or group. Metalake is like a concept of the organization. `AddUser` or
 * `AddGroup` means that a role or user enters an organization.
 */
class UserGroupManager implements SupportsUserOperation, SupportsGroupOperation {

  private static final Logger LOG = LoggerFactory.getLogger(UserGroupManager.class);
  private static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";

  private static final String GROUP_DOES_NOT_EXIST_MSG =
      "Group %s does not exist in th metalake %s";

  private final EntityStore store;
  private final IdGenerator idGenerator;

  public UserGroupManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  public User addUser(String metalake, String user) throws UserAlreadyExistsException {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      UserEntity userEntity =
          UserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(user)
              .withNamespace(
                  Namespace.of(
                      metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME))
              .withRoles(Lists.newArrayList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", user, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", user, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding user {} failed in the metalake {} due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean removeUser(String metalake, String user) {

    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      return store.delete(ofUser(metalake, user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} in the metalake {} failed due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public User getUser(String metalake, String user) throws NoSuchUserException {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      return store.get(ofUser(metalake, user), Entity.EntityType.USER, UserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("User {} does not exist in the metalake {}", user, metalake, e);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      GroupEntity groupEntity =
          GroupEntity.builder()
              .withId(idGenerator.nextId())
              .withName(group)
              .withNamespace(
                  Namespace.of(
                      metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME))
              .withRoles(Collections.emptyList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(groupEntity, false /* overwritten */);
      return groupEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Group {} in the metalake {} already exists", group, metalake, e);
      throw new GroupAlreadyExistsException(
          "Group %s in the metalake %s already exists", group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding group {} failed in the metalake {} due to storage issues", group, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  public boolean removeGroup(String metalake, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      return store.delete(ofGroup(metalake, group), Entity.EntityType.GROUP);
    } catch (IOException ioe) {
      LOG.error(
          "Removing group {} in the metalake {} failed due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public Group getGroup(String metalake, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(store, metalake);
      return store.get(ofGroup(metalake, group), Entity.EntityType.GROUP, GroupEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, e);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofUser(String metalake, String user) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME, user);
  }

  private NameIdentifier ofGroup(String metalake, String group) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME, group);
  }
}
