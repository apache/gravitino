package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;

public interface SupportsGroupOperation {

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  Group addGroup(String metalake, String group) throws GroupAlreadyExistsException;

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  boolean removeGroup(String metalake, String group);

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  Group getGroup(String metalake, String group) throws NoSuchGroupException;
}
