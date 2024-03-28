/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.storage.IdGenerator;

/**
 * AccessControlManager is used for manage users, roles, grant information, this class is an
 * entrance class for tenant management.
 */
public class AccessControlManager {

  private final UserManager userManager;

  public AccessControlManager(EntityStore store, IdGenerator idGenerator) {
    this.userManager = new UserManager(store, idGenerator);
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
    return userManager.addUser(metalake, name);
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user THe name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) {
    return userManager.removeUser(metalake, user);
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
    return userManager.getUser(metalake, user);
  }
}
