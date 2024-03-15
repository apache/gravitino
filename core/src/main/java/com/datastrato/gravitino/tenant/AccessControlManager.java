/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.Group;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.User;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.MetalakeGroup;
import com.datastrato.gravitino.meta.MetalakeUser;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* AccessControlManager is used for manage users, roles, grant information, this class is
 * an important class for tenant management. */
public class AccessControlManager implements SupportsUserManagement, SupportsGroupManagement {

  private static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";
  private static final String GROUP_DOES_NOT_EXIST_MSG =
      "Group %s does not exist in th metalake %s";

  private static final Logger LOG = LoggerFactory.getLogger(AccessControlManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;

  /**
   * Constructs a AccessControlManager instance.
   *
   * @param store The EntityStore to use for managing access control.
   * @param idGenerator The IdGenerator to use for generating identifiers.
   */
  public AccessControlManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  /**
   * Creates a new User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @param properties Additional properties for the Metalake.
   * @return The created User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If creating the User encounters storage issues.
   */
  @Override
  public User createUser(String metalake, String userName, Map<String, String> properties)
      throws UserAlreadyExistsException {
    MetalakeUser metalakeUser =
        MetalakeUser.builder()
            .withId(idGenerator.nextId())
            .withName(userName)
            .withMetalake(metalake)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(metalakeUser, false /* overwritten */);
      return metalakeUser;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", userName, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", userName, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating user {} failed in the metalake {} due to storage issues",
          userName,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Deletes a User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @return `true` if the User was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the User encounters storage issues.
   */
  @Override
  public boolean dropUser(String metalake, String userName) {

    try {
      return store.delete(NameIdentifier.of(metalake, userName), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting user {} in the metalake {} failed due to storage issues",
          userName,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads a User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @return The loaded User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If loading the User encounters storage issues.
   */
  @Override
  public User loadUser(String metalake, String userName) throws NoSuchUserException {
    try {
      return store.get(
          NameIdentifier.of(metalake, userName), Entity.EntityType.USER, MetalakeUser.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("user {} does not exist in the metalake {}", userName, metalake, e);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, userName, metalake);
    } catch (IOException ioe) {
      LOG.error("Loading user {} failed due to storage issues", userName, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Creates a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @param users The users of the Group.
   * @param properties Additional properties for the Metalake.
   * @return The created Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If creating the Group encounters storage issues.
   */
  @Override
  public Group createGroup(
      String metalake, String group, List<String> users, Map<String, String> properties)
      throws GroupAlreadyExistsException {
    MetalakeGroup metalakeGroup =
        MetalakeGroup.builder()
            .withId(idGenerator.nextId())
            .withName(group)
            .withMetalake(metalake)
            .withProperties(properties)
            .withUsers(users)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(metalakeGroup, false /* overwritten */);
      return metalakeGroup;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Group {} in the metalake {} already exists", group, metalake, e);
      throw new GroupAlreadyExistsException(
          "Group %s in the metalake %s already exists", group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating group {} failed in the metalake {} due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Deletes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the Group encounters storage issues.
   */
  @Override
  public boolean dropGroup(String metalake, String group) {
    try {
      return store.delete(NameIdentifier.of(metalake, group), Entity.EntityType.GROUP);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting group {} in the metalake {} failed due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The loaded Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If loading the Group encounters storage issues.
   */
  @Override
  public Group loadGroup(String metalake, String group) {
    try {
      return store.get(
          NameIdentifier.of(metalake, group), Entity.EntityType.GROUP, MetalakeGroup.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, e);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error("Loading group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
