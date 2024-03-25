/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;

/** The interface for the user management. */
public interface SupportsUserManagement {

  /**
   * Creates a new User.
   *
   * @param metalake The Metalake of the User.
   * @param name The name of the User.
   * @return The created User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If creating the User encounters storage issues.
   */
  User addUser(String metalake, String name);

  /**
   * Deletes a User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @return `true` if the User was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the User encounters storage issues.
   */
  boolean removeUser(String metalake, String userName);

  /**
   * Loads a User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @return The loaded User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If loading the User encounters storage issues.
   */
  User loadUser(String metalake, String userName);
}
