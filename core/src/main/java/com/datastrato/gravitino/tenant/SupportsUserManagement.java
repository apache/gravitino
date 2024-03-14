package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.User;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import java.util.Map;

/** The interface for the user management. */
public interface SupportsUserManagement {

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
  User createUser(String metalake, String userName, Map<String, String> properties);

  /**
   * Deletes a User.
   *
   * @param metalake The Metalake of the User.
   * @param userName THe name of the User.
   * @return `true` if the User was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the User encounters storage issues.
   */
  boolean dropUser(String metalake, String userName);

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
