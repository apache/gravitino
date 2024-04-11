/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;

/**
 * AccessControlManager is used for manage users, roles, admin, grant information, this class is an
 * entrance class for tenant management. The operations should be protected by the method
 * `AuthorizationUtils.doWithLock`.
 */
public class AccessControlManager {

  private final UserGroupManager userGroupManager;
  private final AdminManager adminManager;

  public AccessControlManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.userGroupManager = new UserGroupManager(store, idGenerator);
    this.adminManager = new AdminManager(store, idGenerator, config);
  }

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param name The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String metalake, String name) throws UserAlreadyExistsException {
    return userGroupManager.addUser(metalake, name);
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) {
    return userGroupManager.removeUser(metalake, user);
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String metalake, String user) throws NoSuchUserException {
    return userGroupManager.getUser(metalake, user);
  }

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    return userGroupManager.addGroup(metalake, group);
  }

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String metalake, String group) {
    return userGroupManager.removeGroup(metalake, group);
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  public Group getGroup(String metalake, String group) throws NoSuchGroupException {
    return userGroupManager.getGroup(metalake, group);
  }

  /**
   * Adds a new metalake admin. Only service admins can manage metalake admins.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addMetalakeAdmin(String user) {
    String currentUser = PrincipalUtils.getCurrentPrincipal().getName();
    if (!isServiceAdmin(currentUser)) {
      throw new ForbiddenException(
          "%s is not service admin, only service admins can manage metalake admins", currentUser);
    }
    return adminManager.addMetalakeAdmin(user);
  }

  /**
   * Removes a metalake admin. Only service admins can manage metalake admins.
   *
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeMetalakeAdmin(String user) {
    String currentUser = PrincipalUtils.getCurrentPrincipal().getName();
    if (!isServiceAdmin(currentUser)) {
      throw new ForbiddenException(
          "%s is not service admin, only service admins can manage metalake admins", currentUser);
    }
    return adminManager.removeMetalakeAdmin(user);
  }

  /**
   * Judges whether the user is the service admin.
   *
   * @param user the name of the user
   * @return true, if the user is service admin, otherwise false.
   */
  boolean isServiceAdmin(String user) {
    return adminManager.isServiceAdmin(user);
  }

  /**
   * Judges whether the user is the metalake admin.
   *
   * @param user the name of the user
   * @return true, if the user is metalake admin, otherwise false.
   */
  public boolean isMetalakeAdmin(String user) {
    return adminManager.isMetalakeAdmin(user);
  }

  /**
   * Judges whether the user is in the metalake.
   *
   * @param user The name of the User
   * @param metalake The name of the Metalake
   * @return true, if the user is in the metalake, otherwise false.
   */
  public boolean isUserInMetalake(String user, String metalake) {
    return userGroupManager.isUserInMetalake(user, metalake);
  }
}
